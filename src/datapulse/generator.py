"""Seeded synthetic landing data with the failure modes real feeds have.

One file per day, alternating CSV and JSONL. On top of clean order lifecycles
(placed -> paid -> shipped -> delivered, or cancelled) the generator injects:

* exact duplicate rows, in the same file and in a later file;
* late data: events delivered one or two files after the day they happened;
* restatements: the same order and event time re-sent later with a corrected amount;
* corrupt rows of eleven kinds (bad number, negative amount, unknown enum, naive timestamp,
  missing key, out-of-range quantity, bad currency, truncated row, invalid UTF-8, a bare
  number as timestamp, an event far in the future);
* a schema-drifted partner file (a required column renamed, a column added);
* an extra-column file that is still loadable (the JSONL file for day 6);
* a byte-identical re-drop of an earlier file under a new name.

Output bytes depend only on the arguments (``random.Random(seed)``, LF line endings, no clock),
so the same seed gives identical files on every platform. `Generation.expected_orders` is the
ground truth computed from what was emitted, used by the integration tests.
"""

from __future__ import annotations

import csv
import io
import json
import os
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from datapulse.schema import ALL_COLUMNS

T0 = datetime(2026, 1, 1, tzinfo=UTC)
BANGKOK = timezone(timedelta(hours=7))
PRICES = [Decimal(p) for p in ("4.99", "9.50", "12.00", "19.99", "35.25", "79.00", "149.90")]
CURRENCIES = ["USD", "EUR", "GBP", "THB"]
CHANNELS = ("web", "app", "store")
CORRUPTIONS = (
    "bad_amount",
    "negative_amount",
    "bad_status",
    "naive_time",
    "missing_order_id",
    "zero_quantity",
    "bad_currency",
    "truncated",
    "bad_utf8",
    "epoch_time",
    "future_time",
)
BAD_BYTES_MARK = "@@BADBYTES@@"
EXTRA_COLUMN_DAY = 5  # 0-based: the JSONL file for 2026-01-06 carries an extra `channel` key
RESEND_DAY = 2  # 0-based: the CSV file for 2026-01-03 is dropped a second time

Event = dict[str, Any]


@dataclass(slots=True)
class Generation:
    files: list[Path]
    as_of: datetime
    expected_orders: dict[str, Event] = field(default_factory=dict)
    expected_quarantined_lines: int = 0
    rejected_files: list[str] = field(default_factory=list)
    duplicate_files: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _Row:
    event: Event
    corruption: str | None = None
    offset_tz: bool = False


def _day_name(day: int, ext: str, suffix: str = "") -> str:
    return f"orders_{(T0 + timedelta(days=day)):%Y-%m-%d}{suffix}.{ext}"


def _lifecycle(rng: random.Random, index: int, n_days: int) -> list[Event]:
    quantity = rng.randint(1, 5)
    base: Event = {
        "order_id": f"ORD-{100000 + index:06d}",
        "customer_id": f"CUS-{rng.randint(10000, 99999):05d}",
        "quantity": quantity,
        "amount": (rng.choice(PRICES) * quantity).quantize(Decimal("0.01")),
        "currency": rng.choice(CURRENCIES),
        "discount_code": f"SAVE{rng.randint(10, 40)}" if rng.random() < 0.25 else None,
    }
    t = T0 + timedelta(minutes=rng.randint(0, n_days * 24 * 60 - 360))
    events = [{**base, "status": "placed", "event_time": t}]
    steps = (("paid", 5, 180, 0.92), ("shipped", 360, 2160, 0.90), ("delivered", 720, 4320, 0.85))
    for status, lo, hi, p in steps:
        t = t + timedelta(minutes=rng.randint(lo, hi))
        if rng.random() < p:
            events.append({**base, "status": status, "event_time": t})
        else:
            events.append({**base, "status": "cancelled", "event_time": t})
            break
    return events


def _fmt_time(t: datetime, offset_tz: bool) -> str:
    if offset_tz:
        return t.astimezone(BANGKOK).isoformat()
    return t.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _rendered(row: _Row) -> dict[str, Any]:
    """Field values as the producer writes them, with the row's corruption applied."""
    e = row.event
    out: dict[str, Any] = {
        "order_id": e["order_id"],
        "customer_id": e["customer_id"],
        "status": e["status"],
        "quantity": e["quantity"],
        "amount": e["amount"],
        "currency": e["currency"],
        "event_time": _fmt_time(e["event_time"], row.offset_tz),
        "discount_code": e["discount_code"],
    }
    match row.corruption:
        case "bad_amount":
            out["amount"] = "12,50"
        case "negative_amount":
            out["amount"] = Decimal("-5.00")
        case "bad_status":
            out["status"] = "refunded"
        case "naive_time":
            out["event_time"] = e["event_time"].astimezone(UTC).replace(tzinfo=None).isoformat()
        case "missing_order_id":
            out["order_id"] = None
        case "zero_quantity":
            out["quantity"] = 0
        case "bad_currency":
            out["currency"] = "usd"
        case "epoch_time":
            out["event_time"] = int(e["event_time"].timestamp())  # Unix seconds, not ISO 8601
        case "future_time":
            out["event_time"] = "2099-01-01T00:00:00Z"
        case "bad_utf8":
            out["customer_id"] = BAD_BYTES_MARK
        case _:
            pass
    return out


def _csv_bytes(rows: list[_Row], columns: list[str], rename: dict[str, str] | None = None) -> bytes:
    rename = rename or {}
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([rename.get(c, c) for c in columns])
    for row in rows:
        values = _rendered(row)
        cells = ["" if values.get(c) is None else _csv_cell(values.get(c)) for c in columns]
        if row.corruption == "truncated":
            cells = cells[:-2]
        writer.writerow(cells)
    return buf.getvalue().encode("utf-8").replace(BAD_BYTES_MARK.encode(), b"CUS-\xff\xfe1")


def _csv_cell(value: Any) -> str:
    if isinstance(value, Decimal):
        return f"{value:.2f}"
    return str(value)


def _jsonl_bytes(rows: list[_Row], *, channel: bool = False) -> bytes:
    lines: list[str] = []
    for i, row in enumerate(rows):
        values = {k: v for k, v in _rendered(row).items() if v is not None}
        if isinstance(values.get("amount"), Decimal):
            values["amount"] = float(values["amount"])  # written as a JSON number
        if channel:
            values["channel"] = CHANNELS[i % len(CHANNELS)]
        text = json.dumps(values, ensure_ascii=False)
        if row.corruption == "truncated":
            text = text[: len(text) // 2]
        lines.append(text)
    data = ("\n".join(lines) + "\n").encode("utf-8")
    return data.replace(BAD_BYTES_MARK.encode(), b"CUS-\xff\xfe1")


def _expected(accepted: list[tuple[str, list[_Row], int]]) -> dict[str, Event]:
    """Latest valid event per order by (UTC event time, file order, line), computed directly."""
    best: dict[str, tuple[tuple[datetime, int, int], Event]] = {}
    for rank, (_, rows, first_line) in enumerate(accepted):
        for offset, row in enumerate(rows):
            if row.corruption is not None:
                continue
            key = (row.event["event_time"], rank, first_line + offset)
            current = best.get(row.event["order_id"])
            if current is None or key > current[0]:
                best[row.event["order_id"]] = (key, row.event)
    return {order_id: best[order_id][1] for order_id in sorted(best)}


def generate(out_dir: Path, *, seed: int = 42, n_orders: int = 400, n_days: int = 8) -> Generation:
    if n_days < 6:
        raise ValueError("n_days must be at least 6 so every injected case has a file")
    rng = random.Random(seed)  # noqa: S311 - reproducible test data, not security
    days: list[list[_Row]] = [[] for _ in range(n_days)]

    for index in range(n_orders):
        for event in _lifecycle(rng, index, n_days):
            day = (event["event_time"] - T0) // timedelta(days=1)
            if day >= n_days:
                continue  # not delivered yet
            offset_tz = rng.random() < 0.15
            if rng.random() < 0.08:  # late arrival
                day = min(day + rng.randint(1, 2), n_days - 1)
            days[day].append(_Row(event, offset_tz=offset_tz))
            roll = rng.random()
            if roll < 0.04:  # exact duplicate in the same file
                days[day].append(_Row(event, offset_tz=offset_tz))
            elif roll < 0.07 and day + 1 < n_days:  # exact duplicate in a later file
                days[day + 1].append(_Row(event, offset_tz=offset_tz))
            elif roll < 0.09 and day + 1 < n_days:  # restatement: corrected amount, same time
                fixed = {**event, "amount": event["amount"] + Decimal("1.00")}
                days[day + 1].append(_Row(fixed))

    corrupt_counter = 0
    for rows in days:
        rng.shuffle(rows)
        for _ in range(max(1, round(len(rows) * 0.02))):
            base = rng.choice(rows).event
            kind = CORRUPTIONS[corrupt_counter % len(CORRUPTIONS)]
            corrupt_counter += 1
            rows.insert(rng.randint(0, len(rows)), _Row(base, corruption=kind))

    out_dir.mkdir(parents=True, exist_ok=True)
    columns = list(ALL_COLUMNS)
    payloads: dict[str, bytes] = {}
    accepted: list[tuple[str, list[_Row], int]] = []  # (name, rows, line of first row)
    for day, rows in enumerate(days):
        if day % 2 == 0:
            name = _day_name(day, "csv")
            payloads[name] = _csv_bytes(rows, columns)
            accepted.append((name, rows, 2))
        else:
            name = _day_name(day, "jsonl")
            payloads[name] = _jsonl_bytes(rows, channel=day == EXTRA_COLUMN_DAY)
            accepted.append((name, rows, 1))

    generation = Generation(files=[], as_of=T0 + timedelta(days=n_days))

    # Schema drift: a partner feed renamed `amount` and added `channel`. Rejected as a whole.
    partner_rows = [_Row(_lifecycle(rng, 800000 + i, n_days)[0]) for i in range(20)]
    drift_name = _day_name(4, "csv", "_partner")
    payloads[drift_name] = _csv_bytes(
        partner_rows, [*columns, "channel"], rename={"amount": "total_amount"}
    )
    generation.rejected_files.append(drift_name)

    resend_name = _day_name(RESEND_DAY, "csv", "_resend")
    payloads[resend_name] = payloads[_day_name(RESEND_DAY, "csv")]
    generation.duplicate_files.append(resend_name)

    for name in sorted(payloads):
        path = out_dir / name
        path.write_bytes(payloads[name])
        # Generated files stand for drops that finished landing long ago: give them a fixed,
        # old mtime so they pass the settle interval of the producer contract.
        stamp = generation.as_of.timestamp()
        os.utime(path, (stamp, stamp))
        generation.files.append(path)

    # Processing order is name order; the rejected file also takes a file_seq, which does not
    # change the relative order of the accepted files.
    accepted.sort(key=lambda item: item[0])
    generation.expected_orders = _expected(accepted)
    generation.expected_quarantined_lines = sum(
        1 for _, rows, _ in accepted for r in rows if r.corruption is not None
    )
    return generation
