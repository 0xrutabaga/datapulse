"""DuckDB warehouse: schema, manifest and the one-transaction-per-file load."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from datapulse.crash import maybe_crash
from datapulse.dedup import latest_per_key
from datapulse.ingest import FileFormat, FileResult

SCHEMA_VERSION = 2

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS manifest (
    file_seq INTEGER PRIMARY KEY,
    sha256 VARCHAR NOT NULL UNIQUE,
    file_name VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('loaded', 'rejected')),
    rows_read INTEGER NOT NULL,
    rows_valid INTEGER NOT NULL,
    rows_quarantined INTEGER NOT NULL,
    rows_superseded INTEGER NOT NULL,
    rows_stale INTEGER NOT NULL,
    rows_duplicate INTEGER NOT NULL,
    extra_columns VARCHAR NOT NULL,
    reject_reason VARCHAR
);
CREATE TABLE IF NOT EXISTS duplicate_drops (
    file_name VARCHAR NOT NULL,
    sha256 VARCHAR NOT NULL,
    original_file_seq INTEGER NOT NULL,
    PRIMARY KEY (file_name, sha256)
);
CREATE TABLE IF NOT EXISTS quarantine (
    file_seq INTEGER NOT NULL,
    file_name VARCHAR NOT NULL,
    line INTEGER NOT NULL,
    field VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    raw VARCHAR NOT NULL,
    PRIMARY KEY (file_seq, line, field)
);
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    quantity INTEGER NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR NOT NULL,
    event_time TIMESTAMP NOT NULL,
    discount_code VARCHAR,
    src_file_seq INTEGER NOT NULL,
    src_line INTEGER NOT NULL
);
"""

STAGING_DDL = """
CREATE TEMP TABLE IF NOT EXISTS staging (
    order_id VARCHAR, customer_id VARCHAR, status VARCHAR, quantity INTEGER,
    amount DECIMAL(12, 2), currency VARCHAR, event_time TIMESTAMP, discount_code VARCHAR,
    src_file_seq INTEGER, src_line INTEGER
);
"""


def _newer(new: str, old: str) -> str:
    """SQL predicate: row `new` beats row `old` by (event_time, src_file_seq, src_line).

    This is the same rule as `dedup.latest_per_key`, applied against the stored state, which is
    what makes reloading identical rows a no-op and late (older) events harmless.
    """
    return (
        f"({new}.event_time > {old}.event_time"
        f" OR ({new}.event_time = {old}.event_time AND {new}.src_file_seq > {old}.src_file_seq)"
        f" OR ({new}.event_time = {old}.event_time AND {new}.src_file_seq = {old}.src_file_seq"
        f" AND {new}.src_line > {old}.src_line))"
    )


# Built once at import time from constant fragments; no user input reaches these strings.
UPSERT = f"""
INSERT INTO orders AS o
SELECT * FROM staging
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = excluded.customer_id,
    status = excluded.status,
    quantity = excluded.quantity,
    amount = excluded.amount,
    currency = excluded.currency,
    event_time = excluded.event_time,
    discount_code = excluded.discount_code,
    src_file_seq = excluded.src_file_seq,
    src_line = excluded.src_line
WHERE {_newer("excluded", "o")}
"""  # noqa: S608

_STALE_WHERE = _newer("s", "o")
STALE_COUNT = (
    f"SELECT count(*) FROM staging s JOIN orders o USING (order_id) WHERE NOT {_STALE_WHERE}"  # noqa: S608
)


class SchemaVersionError(RuntimeError):
    pass


@contextmanager
def connect(db_path: Path, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse, create tables if needed, and always close the handle.

    Closing matters on Windows, where an open DuckDB file cannot be deleted or reopened by
    another process.
    """
    if not read_only:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=read_only)
    try:
        if not read_only:
            con.execute(DDL)
            con.execute(
                "INSERT INTO meta VALUES ('schema_version', ?) ON CONFLICT DO NOTHING",
                [str(SCHEMA_VERSION)],
            )
        row = con.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
        if row is None or row[0] != str(SCHEMA_VERSION):
            found = None if row is None else row[0]
            raise SchemaVersionError(f"warehouse schema version {found}, expected {SCHEMA_VERSION}")
        yield con
    finally:
        con.close()


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    file_seq: int
    file_name: str
    status: str


def find_by_hash(con: duckdb.DuckDBPyConnection, sha256: str) -> ManifestEntry | None:
    row = con.execute(
        "SELECT file_seq, file_name, status FROM manifest WHERE sha256 = ?", [sha256]
    ).fetchone()
    return None if row is None else ManifestEntry(int(row[0]), str(row[1]), str(row[2]))


def next_file_seq(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute("SELECT coalesce(max(file_seq), 0) + 1 FROM manifest").fetchone()
    if row is None:  # an aggregate always returns one row
        raise RuntimeError("manifest aggregate returned no row")
    return int(row[0])


def record_duplicate_drop(
    con: duckdb.DuckDBPyConnection, file_name: str, sha256: str, original: ManifestEntry
) -> None:
    con.execute(
        "INSERT INTO duplicate_drops VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
        [file_name, sha256, original.file_seq],
    )


def load_file(
    con: duckdb.DuckDBPyConnection,
    *,
    file_seq: int,
    file_name: str,
    fmt: FileFormat,
    sha256: str,
    result: FileResult,
) -> int:
    """Write one file's quarantine rows, warehouse upsert and manifest entry atomically.

    Returns the number of stale rows (valid rows that lost to newer state already stored).
    Either everything below commits together or none of it does; the manifest entry is what
    marks the file as done, so a crash anywhere before COMMIT means the file is simply loaded
    again on the next run.
    """
    winners = latest_per_key(result.valid)
    superseded = len(result.valid) - len(winners)
    # Exact re-sends inside the file: same key and same event time as an earlier valid row.
    duplicates = len(result.valid) - len({(r.order_id, r.event_time) for r in result.valid})
    con.execute(STAGING_DDL)
    con.begin()
    try:
        if result.quarantine:
            con.executemany(
                "INSERT INTO quarantine VALUES (?, ?, ?, ?, ?, ?)",
                [
                    [file_seq, file_name, q.line, q.field, q.reason, q.raw]
                    for q in result.quarantine
                ],
            )
        maybe_crash("after_quarantine", file_name)

        stale = 0
        if winners:
            con.execute("DELETE FROM staging")
            con.executemany(
                "INSERT INTO staging VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    [
                        r.order_id, r.customer_id, r.status, r.quantity, r.amount, r.currency,
                        r.event_time, r.discount_code, r.file_seq, r.line,
                    ]
                    for r in winners
                ],
            )  # fmt: skip
            row = con.execute(STALE_COUNT).fetchone()
            stale = int(row[0]) if row else 0
            con.execute(UPSERT)
        maybe_crash("after_upsert", file_name)

        con.execute(
            "INSERT INTO manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                file_seq, sha256, file_name, fmt,
                "rejected" if result.reject_reason else "loaded",
                result.rows_read, len(result.valid), result.rows_quarantined, superseded, stale,
                duplicates,
                ",".join(result.extra_columns), result.reject_reason,
            ],
        )  # fmt: skip
        maybe_crash("before_commit", file_name)
        con.commit()
    except BaseException:
        con.rollback()
        raise
    maybe_crash("after_commit", file_name)
    return stale
