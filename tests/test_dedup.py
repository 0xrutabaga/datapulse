from __future__ import annotations

import itertools
from datetime import datetime
from decimal import Decimal

from datapulse.dedup import latest_per_key
from datapulse.schema import ValidRow


def row(order: str, hour: int, file_seq: int, line: int, status: str = "paid") -> ValidRow:
    return ValidRow(
        order_id=order,
        customer_id="CUS-00001",
        status=status,
        quantity=1,
        amount=Decimal("1.00"),
        currency="USD",
        event_time=datetime(2026, 1, 1, hour),
        discount_code=None,
        file_seq=file_seq,
        line=line,
    )


def test_latest_event_time_wins_even_if_it_arrived_first() -> None:
    newer = row("ORD-000001", 10, file_seq=1, line=2, status="shipped")
    late_older = row("ORD-000001", 9, file_seq=2, line=2, status="placed")
    assert latest_per_key([newer, late_older]) == [newer]


def test_tie_on_event_time_broken_by_file_then_line() -> None:
    a = row("ORD-000001", 9, file_seq=1, line=50)
    b = row("ORD-000001", 9, file_seq=2, line=3)
    c = row("ORD-000001", 9, file_seq=2, line=4)
    assert latest_per_key([a, b, c]) == [c]
    assert latest_per_key([a, b]) == [b]


def test_result_is_independent_of_input_order_and_sorted_by_key() -> None:
    rows = [
        row("ORD-000002", 9, 1, 2),
        row("ORD-000001", 9, 1, 3),
        row("ORD-000001", 11, 1, 4),
        row("ORD-000002", 9, 1, 5),
        row("ORD-000003", 8, 1, 6),
    ]
    expected = latest_per_key(rows)
    assert [r.order_id for r in expected] == ["ORD-000001", "ORD-000002", "ORD-000003"]
    for perm in itertools.permutations(rows):
        assert latest_per_key(perm) == expected


def test_empty_input() -> None:
    assert latest_per_key([]) == []
