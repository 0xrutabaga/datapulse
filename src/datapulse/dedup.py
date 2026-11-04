"""Deterministic "latest wins" deduplication by natural key (order_id)."""

from __future__ import annotations

from collections.abc import Iterable

from datapulse.schema import ValidRow


def latest_per_key(rows: Iterable[ValidRow]) -> list[ValidRow]:
    """Keep one row per order_id: the greatest `ValidRow.precedence`.

    Precedence is (event_time, file_seq, line), so a later event always wins, and an exact tie
    on event time is broken by source order (later file, then later line). The tuple is a total
    order over distinct source positions, so the result does not depend on input order.
    Output is sorted by order_id.
    """
    best: dict[str, ValidRow] = {}
    for row in rows:
        current = best.get(row.order_id)
        if current is None or row.precedence > current.precedence:
            best[row.order_id] = row
    return [best[key] for key in sorted(best)]
