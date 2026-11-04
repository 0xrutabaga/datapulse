"""Kill the pipeline process mid-load, rerun, and require byte-identical outputs."""

from __future__ import annotations

from pathlib import Path

import pytest

from datapulse import pipeline, store
from datapulse.crash import CRASH_EXIT_CODE, POINTS, parse_spec
from datapulse.generator import Generation

from .conftest import AS_OF, digest, outputs, run_cli


def cli_run(landing: Path, db: Path, crash: str | None = None) -> int:
    proc = run_cli("run", "--landing", str(landing), "--db", str(db), "--as-of", AS_OF, crash=crash)
    return proc.returncode


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("victim", ["orders_2026-01-01.csv", "orders_2026-01-04.jsonl"])
def test_crash_then_rerun_is_byte_identical_to_clean_run(
    generated: Generation, tmp_path: Path, point: str, victim: str
) -> None:
    landing = generated.files[0].parent
    clean = tmp_path / "clean.duckdb"
    pipeline.run(landing, clean)
    expected = outputs(clean, tmp_path / "clean-out")

    crashed = tmp_path / "crashed.duckdb"
    assert cli_run(landing, crashed, crash=f"{point}:{victim}") == CRASH_EXIT_CODE

    with store.connect(crashed, read_only=True) as con:
        names = [r[0] for r in con.execute("SELECT file_name FROM manifest").fetchall()]
    # The victim is in the manifest only if the crash came after its COMMIT.
    assert (victim in names) == (point == "after_commit")

    # Exit code 1 is allowed: the default thresholds flag the schema-drifted file.
    assert cli_run(landing, crashed) in (0, 1)
    actual = outputs(crashed, tmp_path / "rerun-out")
    assert digest(actual) == digest(expected)
    assert actual == expected


def test_repeated_crashes_on_different_files_still_converge(
    generated: Generation, tmp_path: Path
) -> None:
    landing = generated.files[0].parent
    clean = tmp_path / "clean.duckdb"
    pipeline.run(landing, clean)

    db = tmp_path / "w.duckdb"
    assert cli_run(landing, db, "after_upsert:orders_2026-01-02.jsonl") == CRASH_EXIT_CODE
    assert cli_run(landing, db, "before_commit:orders_2026-01-05_partner.csv") == CRASH_EXIT_CODE
    assert cli_run(landing, db, "after_quarantine:orders_2026-01-06.jsonl") == CRASH_EXIT_CODE
    assert cli_run(landing, db) in (0, 1)
    assert outputs(db, tmp_path / "a") == outputs(clean, tmp_path / "b")


def test_crash_spec_validation() -> None:
    assert parse_spec("before_commit") == ("before_commit", None)
    assert parse_spec("after_commit:x.csv") == ("after_commit", "x.csv")
    with pytest.raises(ValueError, match="unknown crash point"):
        parse_spec("halfway")
