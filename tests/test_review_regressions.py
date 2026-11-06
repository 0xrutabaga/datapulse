"""Regression tests for defects found in independent review (see docs/ENGINEERING_DECISIONS.md)."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from datapulse import pipeline, store
from datapulse.config import IngestSettings
from datapulse.ingest import RawRecord, parse_csv, parse_jsonl, validate_record
from datapulse.pipeline import RunOptions
from datapulse.schema import ValidRow

from .conftest import AS_OF, OPTS, run_cli

HEADER = "order_id,customer_id,status,quantity,amount,currency,event_time,discount_code\n"
OLD = datetime(2026, 1, 1, tzinfo=UTC).timestamp()


def row(order: int, status: str, when: str, amount: str = "5.00") -> str:
    return f"ORD-{order:06d},CUS-00001,{status},1,{amount},USD,{when},\n"


def landed(path: Path, text: str, *, settled: bool = True) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    if settled:
        os.utime(path, (OLD, OLD))
    return path


def table(db: Path, sql: str) -> list[tuple[object, ...]]:
    with store.connect(db, read_only=True) as con:
        return con.execute(sql).fetchall()


# 1. The manifest hash must describe the bytes that were parsed.


def test_file_changing_between_hash_and_parse_cannot_split_hash_from_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "landing"
    v1 = HEADER + row(1, "placed", "2026-01-01T00:00:00Z")
    v2 = HEADER + row(1, "paid", "2026-01-02T00:00:00Z")
    path = landed(landing / "a.csv", v1)
    real_next_seq: Callable[..., int] = store.next_file_seq

    def overwrite_then_continue(con: object) -> int:
        # Runs after the skip check and before parsing: the window the old code left open.
        path.write_text(v2, encoding="utf-8", newline="\n")
        return real_next_seq(con)

    monkeypatch.setattr("datapulse.store.next_file_seq", overwrite_then_continue)
    db = tmp_path / "w.duckdb"
    pipeline.run(landing, db, OPTS)
    monkeypatch.setattr("datapulse.store.next_file_seq", real_next_seq)

    sha_v1 = hashlib.sha256(v1.encode()).hexdigest()
    assert table(db, "SELECT sha256 FROM manifest") == [(sha_v1,)]
    assert table(db, "SELECT status FROM orders") == [("placed",)]  # V1's rows, not V2's

    # V2 is a genuinely new drop and loads exactly once.
    assert [o.action for o in pipeline.run(landing, db, OPTS).outcomes] == ["loaded"]
    assert table(db, "SELECT status FROM orders") == [("paid",)]
    assert [o.action for o in pipeline.run(landing, db, OPTS).outcomes] == ["already_loaded"]


# 2. Producer contract: files still being written are not read.


def test_half_written_file_is_deferred_then_loaded_once(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    db = tmp_path / "w.duckdb"
    complete = (
        HEADER + row(1, "placed", "2026-01-01T00:00:00Z") + row(2, "placed", "2026-01-01T00:00:00Z")
    )
    partial = complete[: len(complete) - 20]  # producer is mid-write: last line truncated
    path = landed(landing / "a.csv", partial, settled=False)
    # A long settle interval keeps the test independent of how slowly the child process starts.
    args = ("run", "--landing", str(landing), "--db", str(db), "--as-of", AS_OF,
            "--settle-seconds", "3600")  # fmt: skip

    proc = run_cli(*args)
    assert "not_ready       a.csv" in proc.stdout, proc.stdout + proc.stderr
    assert table(db, "SELECT count(*) FROM manifest") == [(0,)]
    assert table(db, "SELECT count(*) FROM quarantine") == [(0,)]

    landed(path, complete)  # finished and settled
    proc = run_cli(*args)
    assert "loaded          a.csv  (2 valid, 0 quarantined, 0 stale)" in proc.stdout
    assert table(db, "SELECT rows_read, rows_quarantined FROM manifest") == [(2, 0)]
    assert table(db, "SELECT count(*) FROM quarantine") == [(0,)]


def test_file_that_changes_while_being_read_is_deferred(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = landed(tmp_path / "landing" / "a.csv", HEADER + row(1, "placed", "2026-01-01T00:00:00Z"))
    real_read = Path.read_bytes

    def read_then_append(self: Path) -> bytes:
        data = real_read(self)
        with self.open("ab") as fh:
            fh.write(row(2, "placed", "2026-01-01T00:00:00Z").encode())
        return data

    monkeypatch.setattr(Path, "read_bytes", read_then_append)
    summary = pipeline.run(path.parent, tmp_path / "w.duckdb", OPTS)
    assert [(o.action, o.detail) for o in summary.outcomes] == [
        ("not_ready", "changed while being read")
    ]


def test_files_behind_a_not_ready_file_wait_to_keep_name_order(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landed(landing / "a.csv", HEADER + row(1, "placed", "2026-01-01T00:00:00Z"))
    landed(landing / "b.csv", HEADER + row(1, "paid", "2026-01-01T00:00:00Z"), settled=False)
    landed(landing / "c.csv", HEADER + row(2, "paid", "2026-01-01T00:00:00Z"))
    landed(landing / ".d.csv", "hidden temp upload")
    options = RunOptions(as_of=OPTS.as_of, ingest=IngestSettings(settle_seconds=3600))
    summary = pipeline.run(landing, tmp_path / "w.duckdb", options)
    assert [(o.file_name, o.action) for o in summary.outcomes] == [
        ("a.csv", "loaded"),
        ("b.csv", "not_ready"),
        ("c.csv", "waiting"),
    ]


def test_settle_check_uses_mtime_age(tmp_path: Path) -> None:
    path = landed(tmp_path / "a.csv", HEADER)
    now = time.time()
    os.utime(path, (now - 10, now - 10))
    assert pipeline.snapshot(path, settle_seconds=5, now=now) == HEADER.encode()
    with pytest.raises(pipeline.NotReady):
        pipeline.snapshot(path, settle_seconds=60, now=now)


# 3. Strict parsing: no lax coercion of timestamps and numbers.


GOOD = {
    "order_id": "ORD-100001",
    "customer_id": "CUS-12345",
    "status": "paid",
    "quantity": "2",
    "amount": "19.98",
    "currency": "EUR",
    "event_time": "2026-01-02T10:00:00Z",
}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_time", "20260101"),  # lax mode read this as Unix time 1970-08-23
        ("event_time", "1767225600"),
        ("event_time", 1767225600),
        ("event_time", "2026-01-02"),
        ("event_time", "2026-01-02 10:00:00Z"),
        ("event_time", "2026-01-02T10:00Z"),
        ("event_time", "2026-01-02T10:00:00.1234567Z"),  # would be truncated to microseconds
        ("quantity", "1_0"),
        ("quantity", "5.0"),
        ("quantity", " 5"),
        ("quantity", "+3"),
        ("amount", "1e2"),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("amount", "1_000.00"),
        ("amount", " 5"),
    ],
)
def test_non_canonical_values_are_quarantined(field: str, value: object) -> None:
    outcome = validate_record(RawRecord(2, "raw", values={**GOOD, field: value}), file_seq=1)
    assert isinstance(outcome, list), f"{value!r} was accepted"
    assert [q.field for q in outcome] == [field]


def test_canonical_values_still_accepted() -> None:
    values = {**GOOD, "event_time": "2026-01-02T17:00:00.123456+07:00", "quantity": 3}
    outcome = validate_record(RawRecord(2, "raw", values=values), file_seq=1)
    assert isinstance(outcome, ValidRow)
    assert outcome.event_time == datetime(2026, 1, 2, 10, 0, 0, 123456)


def test_jsonl_rejects_nan_and_duplicate_keys() -> None:
    base = json.dumps({**GOOD, "amount": 1})
    nan = base.replace('"amount": 1', '"amount": NaN')
    dup = base.replace(
        '"order_id": "ORD-100001"', '"order_id": "ORD-100001", "order_id": "ORD-100002"'
    )
    records = parse_jsonl((nan + "\n" + dup + "\n").encode()).records
    assert [r.error for r in records] == [
        "invalid JSON: NaN is not a number",
        "invalid JSON: duplicate key 'order_id'",
    ]


def test_empty_string_means_no_value_in_both_formats() -> None:
    csv_values = parse_csv((HEADER + row(1, "paid", "2026-01-01T00:00:00Z")).encode()).records[0]
    jsonl = json.dumps({**GOOD, "discount_code": "", "quantity": 1})
    jsonl_values = parse_jsonl(jsonl.encode()).records[0]
    assert csv_values.values is not None and "discount_code" not in csv_values.values
    assert jsonl_values.values is not None and "discount_code" not in jsonl_values.values
    assert isinstance(validate_record(jsonl_values, 1), ValidRow)


# 4. Events from the future are quarantined; freshness fails beyond the allowed skew.


def test_future_event_is_quarantined_and_cannot_pin_an_order(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landed(landing / "a.csv", HEADER + row(1, "placed", "2099-01-01T00:00:00Z"))
    landed(landing / "b.csv", HEADER + row(1, "delivered", "2026-01-05T00:00:00Z"))
    db = tmp_path / "w.duckdb"
    proc = run_cli("run", "--landing", str(landing), "--db", str(db), "--as-of", AS_OF)
    assert proc.returncode in (0, 1), proc.stderr
    assert table(db, "SELECT status FROM orders") == [("delivered",)]
    assert table(db, "SELECT file_name, field FROM quarantine") == [("a.csv", "event_time")]


def test_future_skew_is_configurable(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landed(landing / "a.csv", HEADER + row(1, "placed", "2026-01-09T12:00:00Z"))  # as_of + 12h
    config = tmp_path / "c.toml"
    config.write_text("[ingest]\nmax_future_skew_hours = 24\n", encoding="utf-8")
    db = tmp_path / "w.duckdb"
    run_cli("run", "--landing", str(landing), "--db", str(db), "--as-of", AS_OF,
            "--config", str(config))  # fmt: skip
    assert table(db, "SELECT count(*) FROM orders") == [(1,)]
    assert table(db, "SELECT count(*) FROM quarantine") == [(0,)]


def test_freshness_fails_when_newest_event_is_beyond_skew(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landed(landing / "a.csv", HEADER + row(1, "placed", "2026-02-01T00:00:00Z"))
    db = tmp_path / "w.duckdb"
    # Loaded with a later as_of, then checked as of an earlier time: lag is about -552 h.
    run_cli("run", "--landing", str(landing), "--db", str(db), "--as-of", "2026-03-01T00:00:00Z")
    proc = run_cli("check", "--db", str(db), "--as-of", AS_OF)
    assert proc.returncode == 1
    assert "FAIL  freshness_lag_hours" in proc.stdout


# 5/6. The duplicate check can fail, and duplicate_drops keeps one row per (name, content).


def test_duplicate_rate_check_counts_in_file_resends(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    same = row(1, "placed", "2026-01-08T00:00:00Z")
    landed(landing / "a.csv", HEADER + same + same + row(2, "placed", "2026-01-08T00:00:00Z"))
    db = tmp_path / "w.duckdb"
    proc = run_cli("run", "--landing", str(landing), "--db", str(db), "--as-of", AS_OF)
    assert "FAIL  duplicate_rate           observed=0.333333" in proc.stdout
    assert "duplicate_keys" not in proc.stdout
    assert table(db, "SELECT rows_duplicate FROM manifest") == [(1,)]


def test_duplicate_drop_history_is_kept_per_content(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    db = tmp_path / "w.duckdb"
    a = HEADER + row(1, "placed", "2026-01-01T00:00:00Z")
    b = HEADER + row(2, "placed", "2026-01-01T00:00:00Z")
    landed(landing / "a.csv", a)
    landed(landing / "b.csv", b)
    landed(landing / "retry.csv", a)
    pipeline.run(landing, db, OPTS)
    landed(landing / "retry.csv", b)
    pipeline.run(landing, db, OPTS)
    assert table(db, "SELECT d.file_name, m.file_name FROM duplicate_drops d JOIN manifest m"
                     " ON m.file_seq = d.original_file_seq ORDER BY 2") == [
        ("retry.csv", "a.csv"),
        ("retry.csv", "b.csv"),
    ]  # fmt: skip
