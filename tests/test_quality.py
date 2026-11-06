"""Data-quality checks, configuration and CLI exit codes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from datapulse import pipeline, store
from datapulse.config import Thresholds, load_config
from datapulse.generator import Generation
from datapulse.quality import run_checks

from .conftest import AS_OF, OPTS, run_cli

AS_OF_DT = datetime(2026, 1, 9, tzinfo=UTC)


@pytest.fixture
def loaded_db(generated: Generation, tmp_path: Path) -> Path:
    db = tmp_path / "w.duckdb"
    pipeline.run(generated.files[0].parent, db, OPTS)
    return db


def by_name(db: Path, limits: Thresholds, as_of: datetime = AS_OF_DT) -> dict[str, bool]:
    with store.connect(db, read_only=True) as con:
        report = run_checks(con, limits, as_of)
    return {c.name: c.passed for c in report.checks}


def test_lenient_thresholds_pass(loaded_db: Path) -> None:
    limits = Thresholds(max_rejected_files=1, max_null_rate={"discount_code": 0.95})
    assert all(by_name(loaded_db, limits).values())


@pytest.mark.parametrize(
    ("limits", "failing"),
    [
        (Thresholds(max_rejected_files=0), "rejected_files"),
        (Thresholds(max_rejected_files=1, min_orders=10_000), "row_count"),
        (Thresholds(max_rejected_files=1, max_quarantine_rate=0.001), "quarantine_rate"),
        (
            Thresholds(max_rejected_files=1, max_null_rate={"discount_code": 0.1}),
            "null_rate.discount_code",
        ),
        (Thresholds(max_rejected_files=1, amount_max=Decimal("10")), "amount_range"),
        (Thresholds(max_rejected_files=1, max_freshness_lag_hours=0.01), "freshness_lag_hours"),
    ],
)
def test_each_threshold_can_fail(loaded_db: Path, limits: Thresholds, failing: str) -> None:
    results = by_name(loaded_db, limits)
    assert [name for name, ok in results.items() if not ok] == [failing]


def test_freshness_uses_as_of_not_the_clock(loaded_db: Path) -> None:
    limits = Thresholds(max_rejected_files=1)
    assert by_name(loaded_db, limits)["freshness_lag_hours"]
    late = datetime(2026, 3, 1, tzinfo=UTC)
    assert not by_name(loaded_db, limits, late)["freshness_lag_hours"]


def test_empty_warehouse_fails_row_count_range_and_freshness(tmp_path: Path) -> None:
    db = tmp_path / "empty.duckdb"
    with store.connect(db):
        pass
    results = by_name(db, Thresholds())
    assert {n for n, ok in results.items() if not ok} == {
        "row_count",
        "amount_range",
        "freshness_lag_hours",
    }


def test_naive_as_of_rejected(loaded_db: Path) -> None:
    with store.connect(loaded_db, read_only=True) as con, pytest.raises(ValueError):
        run_checks(con, Thresholds(), datetime(2026, 1, 9))


def test_config_rejects_unknown_keys_and_columns(tmp_path: Path) -> None:
    bad = tmp_path / "bad.toml"
    bad.write_text("[checks]\nmax_quarantine_rat = 0.1\n", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_config(bad)
    with pytest.raises(ValidationError, match="unknown nullable column"):
        Thresholds(max_null_rate={"order_id; DROP TABLE orders": 0.1})
    with pytest.raises(ValidationError):
        Thresholds(max_quarantine_rate=1.5)


def test_repo_config_file_is_valid() -> None:
    config = load_config(Path(__file__).parent.parent / "datapulse.toml")
    assert config.checks.max_rejected_files == 1


def test_cli_run_writes_report_and_exits_1_on_failed_checks(
    generated: Generation, tmp_path: Path
) -> None:
    db = tmp_path / "w.duckdb"
    report = tmp_path / "reports" / "dq.json"
    landing = str(generated.files[0].parent)
    proc = run_cli("run", "--landing", landing, "--db", str(db), "--report", str(report),
                   "--as-of", AS_OF)  # fmt: skip
    assert proc.returncode == 1, proc.stderr
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["passed"] is False
    assert [c["name"] for c in data["checks"] if not c["passed"]] == ["rejected_files"]
    assert data["files"]["rejected"][0]["file"] == "orders_2026-01-05_partner.csv"
    assert "FAIL  rejected_files" in proc.stdout

    config = tmp_path / "ok.toml"
    config.write_text("[checks]\nmax_rejected_files = 1\n", encoding="utf-8")
    proc = run_cli("check", "--db", str(db), "--config", str(config), "--as-of", AS_OF,
                   "--report", str(report))  # fmt: skip
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert json.loads(report.read_text(encoding="utf-8"))["passed"] is True


def test_cli_errors_exit_3(tmp_path: Path) -> None:
    proc = run_cli("run", "--landing", str(tmp_path / "missing"), "--db", str(tmp_path / "w.db"))
    assert proc.returncode == 3
    assert "landing directory not found" in proc.stderr

    proc = run_cli("check", "--db", str(tmp_path / "missing.duckdb"))
    assert proc.returncode == 3

    bad = tmp_path / "bad.toml"
    bad.write_text("[checks\n", encoding="utf-8")
    proc = run_cli("run", "--landing", str(tmp_path), "--db", str(tmp_path / "w.db"),
                   "--config", str(bad))  # fmt: skip
    assert proc.returncode == 3


def test_cli_rejects_naive_as_of(tmp_path: Path) -> None:
    proc = run_cli("check", "--db", str(tmp_path / "w.db"), "--as-of", "2026-01-09T00:00:00")
    assert proc.returncode == 2
    assert "UTC offset" in proc.stderr


def test_second_writer_is_refused_while_the_database_is_open(
    generated: Generation, tmp_path: Path
) -> None:
    """DuckDB takes a file lock: a concurrent run fails cleanly instead of corrupting state."""
    db = tmp_path / "w.duckdb"
    with store.connect(db):
        proc = run_cli("run", "--landing", str(generated.files[0].parent), "--db", str(db))
    assert proc.returncode == 3
    assert "datapulse: error:" in proc.stderr


def test_unknown_crash_point_fails_without_partial_load(
    generated: Generation, tmp_path: Path
) -> None:
    db = tmp_path / "w.duckdb"
    proc = run_cli("run", "--landing", str(generated.files[0].parent), "--db", str(db),
                   crash="halfway")  # fmt: skip
    assert proc.returncode == 3
    with store.connect(db, read_only=True) as con:
        assert con.execute("SELECT count(*) FROM manifest").fetchone() == (0,)
