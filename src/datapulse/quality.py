"""Data-quality checks over the warehouse state and the JSON report.

The report is derived only from database contents and `as_of`, never from wall-clock time or
per-run counters, so the same warehouse state always produces the same bytes. That property is
what lets the crash/rerun test compare reports byte for byte.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from datapulse.config import NULLABLE_COLUMNS, Thresholds


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    observed: Any
    threshold: Any


@dataclass(frozen=True, slots=True)
class Report:
    as_of: str
    passed: bool
    checks: list[CheckResult]
    files: dict[str, Any]
    rows: dict[str, int]
    quarantine_by_field: dict[str, int]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True, default=_json_default) + "\n"


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    row = con.execute(sql).fetchone()
    return None if row is None else row[0]


def _rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else round(numerator / denominator, 6)


def run_checks(
    con: duckdb.DuckDBPyConnection,
    limits: Thresholds,
    as_of: datetime,
    max_future_skew_hours: float = 1.0,
) -> Report:
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    as_of_utc = as_of.astimezone(UTC).replace(tzinfo=None)
    checks: list[CheckResult] = []

    orders = int(_scalar(con, "SELECT count(*) FROM orders"))
    checks.append(CheckResult("row_count", orders >= limits.min_orders, orders, limits.min_orders))

    loaded = con.execute(
        "SELECT coalesce(sum(rows_read), 0), coalesce(sum(rows_valid), 0),"
        " coalesce(sum(rows_quarantined), 0), coalesce(sum(rows_superseded), 0),"
        " coalesce(sum(rows_stale), 0), coalesce(sum(rows_duplicate), 0), count(*)"
        " FROM manifest WHERE status = 'loaded'"
    ).fetchone()
    if loaded is None:  # an aggregate always returns a row
        raise RuntimeError("manifest aggregate returned no row")
    read, valid, quarantined, superseded, stale, duplicates, n_loaded = (int(v) for v in loaded)
    q_rate = _rate(quarantined, read)
    checks.append(
        CheckResult(
            "quarantine_rate", q_rate <= limits.max_quarantine_rate, q_rate,
            limits.max_quarantine_rate,
        )
    )  # fmt: skip

    rejected = con.execute(
        "SELECT file_name, reject_reason FROM manifest WHERE status = 'rejected' ORDER BY file_seq"
    ).fetchall()
    checks.append(
        CheckResult(
            "rejected_files", len(rejected) <= limits.max_rejected_files, len(rejected),
            limits.max_rejected_files,
        )
    )  # fmt: skip

    for column in sorted(limits.max_null_rate):
        if column not in NULLABLE_COLUMNS:  # defence in depth; config validation rejects these
            raise ValueError(f"column {column!r} is not checkable")
        nulls = int(_scalar(con, f"SELECT count(*) FROM orders WHERE {column} IS NULL"))  # noqa: S608
        rate = _rate(nulls, orders)
        limit = limits.max_null_rate[column]
        checks.append(CheckResult(f"null_rate.{column}", rate <= limit, rate, limit))

    bounds = con.execute("SELECT min(amount), max(amount) FROM orders").fetchone()
    lo, hi = (None, None) if bounds is None else bounds
    in_range = lo is not None and lo >= limits.amount_min and hi <= limits.amount_max
    checks.append(
        CheckResult(
            "amount_range", in_range,
            {"min": lo, "max": hi},
            {"min": limits.amount_min, "max": limits.amount_max},
        )
    )  # fmt: skip

    latest = _scalar(con, "SELECT max(event_time) FROM orders")
    lag = None if latest is None else round((as_of_utc - latest).total_seconds() / 3600, 3)
    # A negative lag means events newer than as_of; beyond the allowed skew that is a failure,
    # not "very fresh" (e.g. data loaded with a later as_of, or a producer clock running ahead).
    fresh = lag is not None and -max_future_skew_hours <= lag <= limits.max_freshness_lag_hours
    checks.append(
        CheckResult(
            "freshness_lag_hours", fresh, lag,
            {"min": -max_future_skew_hours, "max": limits.max_freshness_lag_hours},
        )
    )  # fmt: skip

    d_rate = _rate(duplicates, valid)
    checks.append(
        CheckResult("duplicate_rate", d_rate <= limits.max_duplicate_rate, d_rate,
                    limits.max_duplicate_rate)
    )  # fmt: skip

    drops = con.execute(
        "SELECT d.file_name, m.file_name FROM duplicate_drops d"
        " JOIN manifest m ON m.file_seq = d.original_file_seq ORDER BY d.file_name"
    ).fetchall()
    by_field = con.execute(
        "SELECT field, count(*) FROM quarantine GROUP BY field ORDER BY field"
    ).fetchall()

    return Report(
        as_of=as_of.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        passed=all(c.passed for c in checks),
        checks=checks,
        files={
            "loaded": n_loaded,
            "rejected": [{"file": f, "reason": r} for f, r in rejected],
            "duplicate_drops": [{"file": f, "duplicate_of": o} for f, o in drops],
        },
        rows={
            "read": read,
            "valid": valid,
            "quarantined": quarantined,
            "superseded_in_file": superseded,
            "stale": stale,
            "duplicate_in_file": duplicates,
            "orders": orders,
        },
        quarantine_by_field={str(f): int(n) for f, n in by_field},
    )


def write_report(report: Report, path: Path) -> None:
    """Write atomically: a reader never sees a half-written report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(report.to_json())
    os.replace(tmp, path)
