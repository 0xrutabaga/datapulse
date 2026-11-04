from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from datapulse.cli import main
from datapulse.generator import CORRUPTIONS, generate

# sha256 over (name, NUL, bytes) of every file for seed 42 with default sizes. CI checks the
# same value on Linux, so this pins that generation is platform-independent.
GOLDEN_SEED_42 = "4325694e79d3b998076530006373d5c78fb1f7378b8a71d3ed6f6b6b81121b0b"


def tree_digest(files: list[Path]) -> str:
    h = hashlib.sha256()
    for p in files:
        h.update(p.name.encode() + b"\0" + p.read_bytes())
    return h.hexdigest()


def test_same_seed_same_bytes(tmp_path: Path) -> None:
    a = generate(tmp_path / "a", seed=7, n_orders=50, n_days=6)
    b = generate(tmp_path / "b", seed=7, n_orders=50, n_days=6)
    assert tree_digest(a.files) == tree_digest(b.files)
    c = generate(tmp_path / "c", seed=8, n_orders=50, n_days=6)
    assert tree_digest(a.files) != tree_digest(c.files)


def test_golden_digest_for_default_demo_data(tmp_path: Path) -> None:
    assert tree_digest(generate(tmp_path, seed=42).files) == GOLDEN_SEED_42


def test_files_use_lf_and_include_every_injected_case(tmp_path: Path) -> None:
    gen = generate(tmp_path, seed=42)
    assert all(b"\r\n" not in p.read_bytes() for p in gen.files)
    assert gen.expected_quarantined_lines >= len(CORRUPTIONS)
    assert gen.rejected_files == ["orders_2026-01-05_partner.csv"]
    assert gen.duplicate_files == ["orders_2026-01-03_resend.csv"]
    resend, original = (
        tmp_path / n for n in ("orders_2026-01-03_resend.csv", "orders_2026-01-03.csv")
    )
    assert resend.read_bytes() == original.read_bytes()
    assert b'"channel"' in (tmp_path / "orders_2026-01-06.jsonl").read_bytes()


def test_too_few_days_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        generate(tmp_path, n_days=3)


def test_demo_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workdir = tmp_path / "demo"
    assert main(["demo", "--workdir", str(workdir)]) == 0
    out = capsys.readouterr().out
    assert "child exit code: 86" in out
    assert "DIFFERENT" not in out
    assert sum(line.endswith("  identical") for line in out.splitlines()) == 5
    assert "outputs unchanged by the second run: True" in out
    assert "demo succeeded" in out
    # Running again resets its own directory.
    assert main(["demo", "--workdir", str(workdir)]) == 0


def test_demo_refuses_to_wipe_a_foreign_directory(tmp_path: Path) -> None:
    (tmp_path / "precious.txt").write_text("keep me")
    assert main(["demo", "--workdir", str(tmp_path)]) == 3
    assert (tmp_path / "precious.txt").exists()
