"""The order-event contract: one pydantic model, plus the column sets used for drift checks."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

Status = Literal["placed", "paid", "shipped", "delivered", "cancelled"]
Currency = Literal["USD", "EUR", "GBP", "THB"]

REQUIRED_COLUMNS: tuple[str, ...] = (
    "order_id",
    "customer_id",
    "status",
    "quantity",
    "amount",
    "currency",
    "event_time",
)
OPTIONAL_COLUMNS: tuple[str, ...] = ("discount_code",)
ALL_COLUMNS: tuple[str, ...] = REQUIRED_COLUMNS + OPTIONAL_COLUMNS

# Accepted text forms. Anything else is quarantined rather than coerced: pydantic's lax mode
# would otherwise read 20260101 as Unix time, "1_0" as 10 and "5.0" as an integer.
ISO_8601 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})")
INTEGER = re.compile(r"-?\d+")
DECIMAL = re.compile(r"-?\d+(\.\d+)?")


class OrderEvent(BaseModel):
    """One state change of an order, as delivered by an upstream producer.

    `event_time` must be an ISO 8601 string with seconds, at most microsecond precision and a UTC
    offset; it is normalised to naive UTC so that DuckDB stores a plain TIMESTAMP and comparisons
    never depend on the session time zone.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    order_id: Annotated[str, Field(pattern=r"^ORD-\d{6}$")]
    customer_id: Annotated[str, Field(pattern=r"^CUS-\d{5}$")]
    status: Status
    quantity: Annotated[int, Field(ge=1, le=1000)]
    amount: Annotated[Decimal, Field(ge=0, le=100_000, max_digits=12, decimal_places=2)]
    currency: Currency
    event_time: AwareDatetime
    discount_code: Annotated[str | None, Field(pattern=r"^[A-Z0-9]{4,12}$")] = None

    @field_validator("quantity", mode="before")
    @classmethod
    def _strict_integer(cls, value: Any) -> Any:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and INTEGER.fullmatch(value):
            return int(value)
        raise ValueError("must be a whole number written with digits only")

    @field_validator("amount", mode="before")
    @classmethod
    def _strict_decimal(cls, value: Any) -> Any:
        if isinstance(value, int) and not isinstance(value, bool):
            return Decimal(value)
        if isinstance(value, Decimal) and value.is_finite():
            return value  # JSON numbers arrive as Decimal (parse_float=Decimal)
        if isinstance(value, str) and DECIMAL.fullmatch(value):
            return Decimal(value)
        raise ValueError("must be a decimal number like 19.99")

    @field_validator("event_time", mode="before")
    @classmethod
    def _iso_8601_only(cls, value: Any) -> Any:
        if isinstance(value, str) and ISO_8601.fullmatch(value):
            return value
        raise ValueError(
            "must be an ISO 8601 string with seconds, at most 6 fractional digits and a UTC"
            " offset, e.g. 2026-01-02T10:00:00Z"
        )

    @field_validator("event_time", mode="after")
    @classmethod
    def _to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    def utc_event_time(self) -> datetime:
        """Event time as naive UTC, the form stored in the warehouse."""
        return self.event_time.astimezone(UTC).replace(tzinfo=None)


@dataclass(frozen=True, slots=True)
class ValidRow:
    """A validated event with its lineage (which file and which line it came from)."""

    order_id: str
    customer_id: str
    status: str
    quantity: int
    amount: Decimal
    currency: str
    event_time: datetime  # naive UTC
    discount_code: str | None
    file_seq: int
    line: int

    @property
    def precedence(self) -> tuple[datetime, int, int]:
        """Ordering for "latest wins": event time, then file order, then line order."""
        return (self.event_time, self.file_seq, self.line)

    @classmethod
    def from_event(cls, event: OrderEvent, *, file_seq: int, line: int) -> ValidRow:
        return cls(
            order_id=event.order_id,
            customer_id=event.customer_id,
            status=event.status,
            quantity=event.quantity,
            amount=event.amount,
            currency=event.currency,
            event_time=event.utc_event_time(),
            discount_code=event.discount_code,
            file_seq=file_seq,
            line=line,
        )
