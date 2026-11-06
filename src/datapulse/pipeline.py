"""Incremental run over a landing directory."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import duckdb

from datapulse import store
from datapulse.config import IngestSettings
from datapulse.ingest import file_format, read_file, sha256_hex

Action = Literal["loaded", "rejected", "already_loaded", "duplicate_drop", "not_ready", "waiting"]


@dataclass(frozen=True, slots=True)
class FileOutcome:
    file_name: str
    action: Action
    detail: str = ""


@dataclass(slots=True)
class RunSummary:
    outcomes: list[FileOutcome] = field(default_factory=list)

    def count(self, action: Action) -> int:
        return sum(1 for o in self.outcomes if o.action == action)


@dataclass(frozen=True, slots=True)
class RunOptions:
    """Per-run settings. `as_of` bounds acceptable event times; None means now (UTC)."""

    as_of: datetime | None = None
    ingest: IngestSettings = field(default_factory=IngestSettings)


class NotReady(Exception):
    """The file does not meet the producer contract yet; try again on a later run."""


def discover(landing: Path) -> list[Path]:
    """Supported files directly in `landing`, in code-point order of their names.

    Name order is the processing order, which decides `file_seq` and so the tie-break between
    events with equal event time. Sorting by name (not mtime or directory order) makes it the
    same on every machine and file system. Files with other suffixes (e.g. `*.part` uploads in
    progress) and names starting with "." are ignored.
    """
    if not landing.is_dir():
        raise NotADirectoryError(f"landing directory not found: {landing}")
    files = [
        p
        for p in landing.iterdir()
        if p.is_file() and not p.name.startswith(".") and file_format(p) is not None
    ]
    return sorted(files, key=lambda p: p.name)


def snapshot(path: Path, settle_seconds: float, now: float | None = None) -> bytes:
    """Read a landing file once, enforcing the producer contract.

    The file must not have been modified for `settle_seconds`, and its size and mtime must be
    the same before and after the read; otherwise `NotReady`. The returned bytes are the only
    copy used afterwards: they are hashed and parsed, so the manifest hash always describes the
    rows that were loaded.
    """
    before = path.stat()
    age = (time.time() if now is None else now) - before.st_mtime
    if age < settle_seconds:
        raise NotReady(f"modified {max(age, 0):.1f}s ago, settle interval is {settle_seconds}s")
    data = path.read_bytes()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or len(
        data
    ) != after.st_size:
        raise NotReady("changed while being read")
    return data


def process_file(
    con: duckdb.DuckDBPyConnection, path: Path, not_after: datetime, settle_seconds: float
) -> FileOutcome:
    fmt = file_format(path)
    if fmt is None:
        raise ValueError(f"unsupported file type: {path.name}")
    data = snapshot(path, settle_seconds)
    digest = sha256_hex(data)
    seen = store.find_by_hash(con, digest)
    if seen is not None:
        if seen.file_name == path.name:
            detail = "previously rejected" if seen.status == "rejected" else ""
            return FileOutcome(path.name, "already_loaded", detail)
        store.record_duplicate_drop(con, path.name, digest, seen)
        return FileOutcome(path.name, "duplicate_drop", f"same content as {seen.file_name}")

    file_seq = store.next_file_seq(con)
    result = read_file(data, fmt, file_seq, not_after)
    stale = store.load_file(
        con, file_seq=file_seq, file_name=path.name, fmt=fmt, sha256=digest, result=result
    )
    if result.reject_reason:
        return FileOutcome(path.name, "rejected", result.reject_reason)
    detail = f"{len(result.valid)} valid, {result.rows_quarantined} quarantined, {stale} stale" + (
        f", extra columns: {', '.join(result.extra_columns)}" if result.extra_columns else ""
    )
    return FileOutcome(path.name, "loaded", detail)


def run(landing: Path, db_path: Path, options: RunOptions | None = None) -> RunSummary:
    """Load every new file in `landing` into `db_path`, one transaction per file.

    Files load strictly in name order: at the first file that is not ready, the run stops and
    the remaining files are reported as `waiting`, so load order (and therefore tie-breaking)
    never depends on which producer happened to finish first.
    """
    options = options or RunOptions()
    as_of = options.as_of or datetime.now(UTC)
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    skew = timedelta(hours=options.ingest.max_future_skew_hours)
    not_after = (as_of.astimezone(UTC) + skew).replace(tzinfo=None)

    files = discover(landing)
    summary = RunSummary()
    with store.connect(db_path) as con:
        for index, path in enumerate(files):
            try:
                outcome = process_file(con, path, not_after, options.ingest.settle_seconds)
            except NotReady as exc:
                summary.outcomes.append(FileOutcome(path.name, "not_ready", str(exc)))
                summary.outcomes.extend(
                    FileOutcome(p.name, "waiting", f"behind {path.name}")
                    for p in files[index + 1 :]
                )
                break
            summary.outcomes.append(outcome)
    return summary
