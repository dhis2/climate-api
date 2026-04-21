"""Time helpers shared across Climate API modules."""

from datetime import datetime
from typing import Any

import numpy as np


def datetime_to_period_string(value: datetime, period_type: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    if period_type == "hourly":
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if period_type == "daily":
        return value.date().isoformat()
    if period_type == "monthly":
        return f"{value.year:04d}-{value.month:02d}"
    if period_type == "yearly":
        return str(value.year)
    return value.isoformat()


def normalize_period_string(value: str, period_type: str) -> str:
    """Normalize an input period string to the dataset-native period format."""
    if period_type == "hourly":
        return datetime_to_period_string(datetime.fromisoformat(value), period_type)
    if period_type == "daily":
        return datetime.fromisoformat(value).date().isoformat()
    if period_type == "monthly":
        if len(value) == 7:
            datetime.fromisoformat(f"{value}-01")
            return value
        return datetime_to_period_string(datetime.fromisoformat(value), period_type)
    if period_type == "yearly":
        if len(value) == 4:
            int(value)
            return value
        return datetime_to_period_string(datetime.fromisoformat(value), period_type)
    return value


def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    # TODO: this and numpy_period_string should be merged
    s = np.datetime_as_string(datetimes, unit="s")

    # Map periods to string lengths: YYYY-MM-DDTHH (13), YYYY-MM-DD (10), etc.
    lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
    return s.astype(f"U{lengths[period_type]}")
