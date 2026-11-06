"""Deterministic CSV export of every warehouse table.

The DuckDB file itself is not byte-stable (block layout depends on the write history), so the
restart-safety guarantee is stated over these exports and the quality report: same state, same
bytes, on every platform (LF line endings, UTF-8, fixed column and row order).
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

import duckdb

# Table -> ORDER BY clause giving a total order over the table's primary key.
TABLES: dict[str, str] = {
    "manifest": "file_seq",
    "duplicate_drops": "file_name, sha256",
    "quarantine": "file_seq, line, field",
    "orders": "order_id",
}


def _cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat() + "Z"  # stored as naive UTC
    return str(value)


def export_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table, order_by in TABLES.items():
        cursor = con.execute(f"SELECT * FROM {table} ORDER BY {order_by}")  # noqa: S608
        header = [d[0] for d in cursor.description]
        path = out_dir / f"{table}.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(header)
            writer.writerows([_cell(v) for v in row] for row in cursor.fetchall())
        written.append(path)
    return written
