"""Read one landing file into validated rows and quarantine entries.

Parsing is line-oriented for both formats so that every problem can be reported with the exact
physical line number. A consequence: CSV fields containing embedded newlines are not supported
(see README, Limitations).
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import ValidationError

from datapulse.schema import ALL_COLUMNS, REQUIRED_COLUMNS, OrderEvent, ValidRow

FileFormat = Literal["csv", "jsonl"]
SUPPORTED_SUFFIXES: dict[str, FileFormat] = {".csv": "csv", ".jsonl": "jsonl"}
ROW_FIELD = "_row"  # quarantine `field` value for problems that are not about one column
RAW_LIMIT = 500


@dataclass(frozen=True, slots=True)
class QuarantineEntry:
    line: int
    field: str
    reason: str
    raw: str


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One physical line: either parsed values or a parse error."""

    line: int
    raw: str
    values: dict[str, object] | None = None
    error: str | None = None


@dataclass(slots=True)
class ParsedFile:
    columns: tuple[str, ...]
    records: list[RawRecord]
    file_error: str | None = None  # set when the file cannot be read at all (e.g. bad header)


@dataclass(slots=True)
class FileResult:
    """Outcome of reading and validating one file, before anything touches the database."""

    rows_read: int
    valid: list[ValidRow] = field(default_factory=list)
    quarantine: list[QuarantineEntry] = field(default_factory=list)
    extra_columns: tuple[str, ...] = ()
    reject_reason: str | None = None

    @property
    def rows_quarantined(self) -> int:
        return len({q.line for q in self.quarantine})


def file_format(path: Path) -> FileFormat | None:
    return SUPPORTED_SUFFIXES.get(path.suffix.lower())


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _physical_lines(data: bytes) -> list[tuple[int, bytes]]:
    """Split on LF, drop a trailing CR (CRLF files) and skip blank lines; keep 1-based numbers."""
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    out: list[tuple[int, bytes]] = []
    for number, line in enumerate(data.split(b"\n"), start=1):
        line = line.removesuffix(b"\r")
        if line.strip():
            out.append((number, line))
    return out


def _shown(line: bytes) -> str:
    return line.decode("utf-8", errors="backslashreplace")[:RAW_LIMIT]


def parse_csv(data: bytes) -> ParsedFile:
    lines = _physical_lines(data)
    if not lines:
        return ParsedFile(columns=(), records=[])
    header_no, header_bytes = lines[0]
    try:
        header = [h.strip() for h in next(csv.reader([header_bytes.decode("utf-8")]))]
    except (UnicodeDecodeError, csv.Error) as exc:
        return ParsedFile((), [], file_error=f"unreadable header on line {header_no}: {exc}")
    if len(set(header)) != len(header):
        return ParsedFile((), [], file_error="duplicate column names in header")

    records: list[RawRecord] = []
    for number, raw in lines[1:]:
        shown = _shown(raw)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            records.append(RawRecord(number, shown, error=f"invalid UTF-8: {exc.reason}"))
            continue
        try:
            cells = next(csv.reader([text], strict=True))
        except csv.Error as exc:
            records.append(RawRecord(number, shown, error=f"malformed CSV: {exc}"))
            continue
        if len(cells) != len(header):
            error = f"expected {len(header)} fields, got {len(cells)}"
            records.append(RawRecord(number, shown, error=error))
            continue
        # An empty cell means "no value": required columns then fail as missing.
        values: dict[str, object] = {k: v for k, v in zip(header, cells, strict=True) if v != ""}
        records.append(RawRecord(number, shown, values=values))
    return ParsedFile(columns=tuple(header), records=records)


def parse_jsonl(data: bytes) -> ParsedFile:
    records: list[RawRecord] = []
    seen: dict[str, None] = {}  # ordered union of keys across parsable lines
    for number, raw in _physical_lines(data):
        shown = _shown(raw)
        try:
            # parse_float=Decimal keeps "19.99" exact instead of a binary float.
            obj = json.loads(raw.decode("utf-8"), parse_float=Decimal)
        except UnicodeDecodeError as exc:
            records.append(RawRecord(number, shown, error=f"invalid UTF-8: {exc.reason}"))
            continue
        except json.JSONDecodeError as exc:
            records.append(RawRecord(number, shown, error=f"invalid JSON: {exc.msg}"))
            continue
        if not isinstance(obj, dict):
            records.append(RawRecord(number, shown, error="expected a JSON object"))
            continue
        values = {str(k): v for k, v in obj.items() if v is not None}
        seen.update(dict.fromkeys(obj))
        records.append(RawRecord(number, shown, values=values))
    return ParsedFile(columns=tuple(seen), records=records)


def check_columns(columns: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return (missing required columns, unexpected extra columns), both sorted."""
    present = set(columns)
    missing = tuple(sorted(set(REQUIRED_COLUMNS) - present))
    extra = tuple(sorted(present - set(ALL_COLUMNS)))
    return missing, extra


def validate_record(record: RawRecord, file_seq: int) -> ValidRow | list[QuarantineEntry]:
    if record.values is None:
        return [QuarantineEntry(record.line, ROW_FIELD, record.error or "unparsable", record.raw)]
    try:
        event = OrderEvent.model_validate(record.values)
    except ValidationError as exc:
        reasons: dict[str, list[str]] = {}
        for err in exc.errors(include_url=False):
            name = ".".join(str(part) for part in err["loc"]) or ROW_FIELD
            reasons.setdefault(name, []).append(err["msg"])
        return [
            QuarantineEntry(record.line, name, "; ".join(msgs), record.raw)
            for name, msgs in sorted(reasons.items())
        ]
    return ValidRow.from_event(event, file_seq=file_seq, line=record.line)


def read_file(path: Path, fmt: FileFormat, file_seq: int) -> FileResult:
    """Parse and validate a whole file. Pure apart from reading `path`."""
    data = path.read_bytes()
    parsed = parse_csv(data) if fmt == "csv" else parse_jsonl(data)
    result = FileResult(rows_read=len(parsed.records))
    if parsed.file_error is not None:
        result.reject_reason = parsed.file_error
        return result

    parsable = any(r.values is not None for r in parsed.records)
    if parsed.columns and (fmt == "csv" or parsable):
        missing, extra = check_columns(parsed.columns)
        result.extra_columns = extra
        if missing:
            reason = f"schema drift: missing required columns {', '.join(missing)}"
            if extra:
                reason += f"; unexpected columns {', '.join(extra)}"
            result.reject_reason = reason
            return result

    for record in parsed.records:
        outcome = validate_record(record, file_seq)
        if isinstance(outcome, ValidRow):
            result.valid.append(outcome)
        else:
            result.quarantine.extend(outcome)
    return result
