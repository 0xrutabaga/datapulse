"""Incremental run over a landing directory."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import duckdb

from datapulse import store
from datapulse.ingest import file_format, read_file, sha256_of

Action = Literal["loaded", "rejected", "already_loaded", "duplicate_drop"]


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


def discover(landing: Path) -> list[Path]:
    """Supported files directly in `landing`, in code-point order of their names.

    Name order is the processing order, which decides `file_seq` and so the tie-break between
    events with equal event time. Sorting by name (not mtime or directory order) makes it the
    same on every machine and file system. Other files, e.g. `*.part` uploads still in
    progress, are ignored.
    """
    if not landing.is_dir():
        raise NotADirectoryError(f"landing directory not found: {landing}")
    files = [p for p in landing.iterdir() if p.is_file() and file_format(p) is not None]
    return sorted(files, key=lambda p: p.name)


def process_file(con: duckdb.DuckDBPyConnection, path: Path) -> FileOutcome:
    fmt = file_format(path)
    if fmt is None:
        raise ValueError(f"unsupported file type: {path.name}")
    digest = sha256_of(path)
    seen = store.find_by_hash(con, digest)
    if seen is not None:
        if seen.file_name == path.name:
            return FileOutcome(path.name, "already_loaded")
        store.record_duplicate_drop(con, path.name, digest, seen)
        return FileOutcome(path.name, "duplicate_drop", f"same content as {seen.file_name}")

    file_seq = store.next_file_seq(con)
    result = read_file(path, fmt, file_seq)
    stale = store.load_file(
        con, file_seq=file_seq, file_name=path.name, fmt=fmt, sha256=digest, result=result
    )
    if result.reject_reason:
        return FileOutcome(path.name, "rejected", result.reject_reason)
    detail = f"{len(result.valid)} valid, {result.rows_quarantined} quarantined, {stale} stale" + (
        f", extra columns: {', '.join(result.extra_columns)}" if result.extra_columns else ""
    )
    return FileOutcome(path.name, "loaded", detail)


def run(landing: Path, db_path: Path) -> RunSummary:
    """Load every new file in `landing` into `db_path`, one transaction per file."""
    files = discover(landing)
    summary = RunSummary()
    with store.connect(db_path) as con:
        for path in files:
            summary.outcomes.append(process_file(con, path))
    return summary
