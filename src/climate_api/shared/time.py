"""Time helpers shared across Climate API modules."""

from datetime import UTC, date, datetime
from typing import Any

import numpy as np


def _normalize_datetime_for_period(value: datetime) -> datetime:
    """Convert aware datetimes to UTC before deriving dataset-native periods."""
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value


def datetime_to_period_string(value: datetime, period_type: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    value = _normalize_datetime_for_period(value)
    if period_type == "hourly":
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if period_type == "daily":
        return value.date().isoformat()
    if period_type == "monthly":
        return f"{value.year:04d}-{value.month:02d}"
    if period_type == "yearly":
        return str(value.year)
    return value.isoformat()


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(UTC)


def utc_today() -> date:
    """Return the current UTC calendar date."""
    return utc_now().date()


def parse_hourly_period_string(value: str) -> datetime:
    """Parse a dataset-native hourly period string or full ISO datetime."""
    if len(value) == 13:
        return datetime.strptime(value, "%Y-%m-%dT%H")
    return datetime.fromisoformat(value)


def normalize_period_string(value: str, period_type: str) -> str:
    """Normalize an input period string to the dataset-native period format."""
    if period_type == "hourly":
        try:
            return datetime_to_period_string(parse_hourly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid hourly period '{value}'; expected YYYY-MM-DDTHH or ISO datetime") from exc
    if period_type == "daily":
        try:
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid daily period '{value}'; expected YYYY-MM-DD or ISO datetime") from exc
    if period_type == "monthly":
        try:
            if len(value) == 7:
                datetime.fromisoformat(f"{value}-01")
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid monthly period '{value}'; expected YYYY-MM or ISO datetime") from exc
    if period_type == "yearly":
        try:
            if len(value) == 4:
                int(value)
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid yearly period '{value}'; expected YYYY or ISO datetime") from exc
    return value


def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    # TODO: this and numpy_period_string should be merged
    s = np.datetime_as_string(datetimes, unit="s")

    # Map periods to string lengths: YYYY-MM-DDTHH (13), YYYY-MM-DD (10), etc.
    lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
    return s.astype(f"U{lengths[period_type]}")
