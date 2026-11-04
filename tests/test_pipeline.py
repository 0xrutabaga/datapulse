"""Integration: full runs over generated landing data against a real DuckDB file."""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from datapulse import crash, pipeline, store
from datapulse.generator import Generation

from .conftest import outputs


def orders(db: Path) -> dict[str, tuple[object, ...]]:
    with store.connect(db, read_only=True) as con:
        rows = con.execute(
            "SELECT order_id, customer_id, status, quantity, amount, currency, event_time,"
            " discount_code FROM orders ORDER BY order_id"
        ).fetchall()
    return {str(r[0]): tuple(r[1:]) for r in rows}


def test_full_run_matches_generator_ground_truth(generated: Generation, tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    summary = pipeline.run(generated.files[0].parent, db)

    assert summary.count("rejected") == 1
    assert summary.count("duplicate_drop") == 1
    assert summary.count("loaded") == len(generated.files) - 2

    expected = {
        oid: (
            e["customer_id"],
            e["status"],
            e["quantity"],
            e["amount"],
            e["currency"],
            e["event_time"].replace(tzinfo=None),
            e["discount_code"],
        )
        for oid, e in generated.expected_orders.items()
    }
    assert orders(db) == expected

    with store.connect(db, read_only=True) as con:
        quarantined = con.execute("SELECT count(DISTINCT (file_seq, line)) FROM quarantine")
        assert quarantined.fetchone() == (generated.expected_quarantined_lines,)
        reasons = {r[0] for r in con.execute("SELECT field FROM quarantine").fetchall()}
        rejected = con.execute("SELECT file_name FROM manifest WHERE status = 'rejected'")
        assert [r[0] for r in rejected.fetchall()] == generated.rejected_files
        drops = con.execute("SELECT file_name FROM duplicate_drops").fetchall()
        assert [r[0] for r in drops] == generated.duplicate_files
    # Every kind of injected corruption lands in quarantine under the right field.
    assert {"_row", "amount", "status", "event_time", "order_id", "quantity", "currency"} <= reasons


def test_second_run_is_a_no_op(generated: Generation, tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    landing = generated.files[0].parent
    pipeline.run(landing, db)
    first = outputs(db, tmp_path / "a")
    again = pipeline.run(landing, db)
    assert {o.action for o in again.outcomes} == {"already_loaded", "duplicate_drop"}
    assert outputs(db, tmp_path / "b") == first


def test_redropped_file_under_new_name_is_skipped(generated: Generation, tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    landing = generated.files[0].parent
    pipeline.run(landing, db)
    before = outputs(db, tmp_path / "a")

    shutil.copyfile(landing / "orders_2026-01-02.jsonl", landing / "zz_retry_upload.jsonl")
    summary = pipeline.run(landing, db)
    redrop = [o for o in summary.outcomes if o.file_name == "zz_retry_upload.jsonl"]
    assert [(o.action, o.detail) for o in redrop] == [
        ("duplicate_drop", "same content as orders_2026-01-02.jsonl")
    ]
    after = outputs(db, tmp_path / "b")
    # Only the duplicate_drops audit table changes; warehouse, quarantine and manifest do not.
    for name in ("orders.csv", "quarantine.csv", "manifest.csv"):
        assert after[name] == before[name]
    assert b"zz_retry_upload.jsonl" in after["duplicate_drops.csv"]


def test_late_event_does_not_overwrite_newer_state(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    header = "order_id,customer_id,status,quantity,amount,currency,event_time,discount_code\n"
    (landing / "a.csv").write_text(
        header + "ORD-000001,CUS-00001,shipped,1,5.00,USD,2026-01-02T00:00:00Z,\n",
        encoding="utf-8",
        newline="\n",
    )
    (landing / "b.csv").write_text(
        header
        + "ORD-000001,CUS-00001,placed,1,5.00,USD,2026-01-01T00:00:00Z,\n"  # late, older
        + "ORD-000002,CUS-00002,placed,1,7.00,USD,2026-01-01T00:00:00Z,\n",
        encoding="utf-8",
        newline="\n",
    )
    summary = pipeline.run(landing, tmp_path / "w.duckdb")
    assert summary.outcomes[1].detail == "2 valid, 0 quarantined, 1 stale"
    state = orders(tmp_path / "w.duckdb")
    assert state["ORD-000001"][1] == "shipped"
    assert state["ORD-000002"][3] == Decimal("7.00")


def test_restatement_with_equal_event_time_takes_the_later_file(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    header = "order_id,customer_id,status,quantity,amount,currency,event_time,discount_code\n"
    row = "ORD-000001,CUS-00001,paid,1,{},USD,2026-01-02T00:00:00Z,\n"
    (landing / "a.csv").write_text(header + row.format("5.00"), encoding="utf-8")
    (landing / "b.csv").write_text(header + row.format("6.00"), encoding="utf-8")
    pipeline.run(landing, tmp_path / "w.duckdb")
    assert orders(tmp_path / "w.duckdb")["ORD-000001"][3] == Decimal("6.00")


def test_modified_file_with_same_name_is_a_new_drop(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    header = "order_id,customer_id,status,quantity,amount,currency,event_time,discount_code\n"
    path = landing / "a.csv"
    path.write_text(header + "ORD-000001,CUS-00001,paid,1,5.00,USD,2026-01-02T00:00:00Z,\n")
    db = tmp_path / "w.duckdb"
    pipeline.run(landing, db)
    path.write_text(header + "ORD-000001,CUS-00001,shipped,1,5.00,USD,2026-01-03T00:00:00Z,\n")
    assert pipeline.run(landing, db).outcomes[0].action == "loaded"
    assert orders(db)["ORD-000001"][1] == "shipped"


def test_unsupported_files_are_ignored(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "upload.csv.part").write_text("partial")
    (landing / "notes.txt").write_text("x")
    assert pipeline.run(landing, tmp_path / "w.duckdb").outcomes == []


def test_missing_landing_directory(tmp_path: Path) -> None:
    with pytest.raises(NotADirectoryError):
        pipeline.run(tmp_path / "nope", tmp_path / "w.duckdb")


def test_error_inside_a_file_rolls_back_everything_for_that_file(
    generated: Generation, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ordinary exception (not a crash) mid-load leaves no partial rows behind."""
    db = tmp_path / "w.duckdb"
    landing = generated.files[0].parent
    target = "orders_2026-01-02.jsonl"
    real = crash.maybe_crash

    def boom(point: str, file_name: str) -> None:
        if point == "after_upsert" and file_name == target:
            raise RuntimeError("disk full")
        real(point, file_name)

    monkeypatch.setattr("datapulse.store.maybe_crash", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        pipeline.run(landing, db)
    with store.connect(db, read_only=True) as con:
        names = [r[0] for r in con.execute("SELECT file_name FROM manifest").fetchall()]
        assert names == ["orders_2026-01-01.csv"]
        seqs = con.execute("SELECT DISTINCT src_file_seq FROM orders").fetchall()
        assert seqs == [(1,)]
        q = con.execute("SELECT DISTINCT file_seq FROM quarantine").fetchall()
        assert q in ([], [(1,)])

    monkeypatch.setattr("datapulse.store.maybe_crash", real)
    pipeline.run(landing, db)
    clean = tmp_path / "clean.duckdb"
    pipeline.run(landing, clean)
    assert outputs(db, tmp_path / "a") == outputs(clean, tmp_path / "b")
