from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from datapulse.ingest import ROW_FIELD, QuarantineEntry, RawRecord, validate_record
from datapulse.schema import ValidRow

GOOD: dict[str, object] = {
    "order_id": "ORD-100001",
    "customer_id": "CUS-12345",
    "status": "paid",
    "quantity": "2",
    "amount": "19.98",
    "currency": "EUR",
    "event_time": "2026-01-02T10:00:00Z",
}


def check(
    values: dict[str, object] | None, error: str | None = None
) -> ValidRow | list[QuarantineEntry]:
    return validate_record(RawRecord(line=7, raw="raw", values=values, error=error), file_seq=3)


def test_valid_row_keeps_lineage_and_exact_decimal() -> None:
    row = check(GOOD)
    assert isinstance(row, ValidRow)
    assert (row.file_seq, row.line) == (3, 7)
    assert row.amount == Decimal("19.98")
    assert row.quantity == 2
    assert row.discount_code is None


def test_offset_timestamp_is_normalised_to_naive_utc() -> None:
    row = check({**GOOD, "event_time": "2026-01-02T17:00:00+07:00"})
    assert isinstance(row, ValidRow)
    assert row.event_time == datetime(2026, 1, 2, 10, 0, 0)


@pytest.mark.parametrize(
    ("field", "value", "reason_part"),
    [
        ("amount", "12,50", "decimal"),
        ("amount", "-5.00", "greater than or equal to 0"),
        ("amount", "1.005", "decimal places"),
        ("status", "refunded", "'placed'"),
        ("event_time", "2026-01-02T10:00:00", "UTC offset"),
        ("quantity", "0", "greater than or equal to 1"),
        ("quantity", True, "whole number"),
        ("currency", "usd", "'USD'"),
        ("order_id", "ORD-1", "pattern"),
        ("discount_code", "save 10", "pattern"),
    ],
)
def test_invalid_field_is_quarantined_with_field_and_reason(
    field: str, value: object, reason_part: str
) -> None:
    entries = check({**GOOD, field: value})
    assert isinstance(entries, list)
    assert [(e.line, e.field) for e in entries] == [(7, field)]
    assert reason_part in entries[0].reason


def test_missing_required_field() -> None:
    values = dict(GOOD)
    del values["customer_id"]
    entries = check(values)
    assert isinstance(entries, list)
    assert [(e.field, e.reason) for e in entries] == [("customer_id", "Field required")]


def test_every_bad_field_is_reported() -> None:
    entries = check({**GOOD, "status": "x", "currency": "x"})
    assert isinstance(entries, list)
    assert sorted(e.field for e in entries) == ["currency", "status"]


def test_parse_error_becomes_row_level_entry() -> None:
    entries = check(None, error="invalid JSON: Expecting value")
    assert entries == [QuarantineEntry(7, ROW_FIELD, "invalid JSON: Expecting value", "raw")]
