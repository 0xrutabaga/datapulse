"""The order-event contract: one pydantic model, plus the column sets used for drift checks."""

from __future__ import annotations

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


class OrderEvent(BaseModel):
    """One state change of an order, as delivered by an upstream producer.

    `event_time` must carry a UTC offset; it is normalised to naive UTC so that DuckDB stores a
    plain TIMESTAMP and comparisons never depend on the session time zone.
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

    @field_validator("quantity", "amount", mode="before")
    @classmethod
    def _reject_bool(cls, value: Any) -> Any:
        # JSON `true` would otherwise be accepted as 1 in lax mode.
        if isinstance(value, bool):
            raise ValueError("boolean is not a number")
        return value

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
