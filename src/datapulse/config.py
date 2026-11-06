"""Data-quality thresholds, loaded from a TOML file (stdlib `tomllib`) and checked by pydantic."""

from __future__ import annotations

import tomllib
from decimal import Decimal
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Columns of `orders` that may appear in `max_null_rate`. Names are interpolated into SQL as
# identifiers, so only this fixed allow-list is accepted.
NULLABLE_COLUMNS: frozenset[str] = frozenset({"discount_code"})

Rate = Annotated[float, Field(ge=0.0, le=1.0)]


class Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    min_orders: Annotated[int, Field(ge=0)] = 1
    max_quarantine_rate: Rate = 0.05
    max_rejected_files: Annotated[int, Field(ge=0)] = 0
    max_null_rate: dict[str, Rate] = Field(default_factory=dict)
    amount_min: Decimal = Decimal("0")
    amount_max: Decimal = Decimal("100000")
    max_freshness_lag_hours: Annotated[float, Field(gt=0)] = 24.0
    # Valid rows repeating an (order_id, event_time) already seen in the same file.
    max_duplicate_rate: Rate = 0.10

    @field_validator("max_null_rate")
    @classmethod
    def _known_columns(cls, value: dict[str, float]) -> dict[str, float]:
        unknown = sorted(set(value) - NULLABLE_COLUMNS)
        if unknown:
            raise ValueError(
                f"unknown nullable column(s) {unknown}; allowed {sorted(NULLABLE_COLUMNS)}"
            )
        return value


class IngestSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # Events later than as_of + this skew are quarantined; freshness also fails beyond it.
    max_future_skew_hours: Annotated[float, Field(ge=0)] = 1.0
    # A landing file is read only once its mtime is at least this old (producer contract).
    settle_seconds: Annotated[float, Field(ge=0)] = 5.0


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ingest: IngestSettings = Field(default_factory=IngestSettings)
    checks: Thresholds = Field(default_factory=Thresholds)


def load_config(path: Path | None) -> Config:
    """Load `path`, or return defaults when no path is given. Unknown keys are an error."""
    if path is None:
        return Config()
    with path.open("rb") as fh:
        return Config.model_validate(tomllib.load(fh))
