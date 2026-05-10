"""Time helpers shared across Climate API modules."""

import re
from datetime import UTC, date, datetime
from typing import Any, cast

import numpy as np
import pandas as pd

_WEEKLY_PERIOD_PATTERN = re.compile(r"^(?P<year>\d{4})-W(?P<week>\d{2})$")


def _normalize_datetime_for_period(value: datetime) -> datetime:
    """Convert aware datetimes to UTC before deriving dataset-native periods."""
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value


def _coerce_numpy_datetime(value: object) -> datetime:
    """Convert a numpy or Python datetime-like scalar to a datetime."""
    if isinstance(value, datetime):
        return value
    np_value = np.datetime64(cast(Any, value))
    return datetime.fromisoformat(np.datetime_as_string(np_value, unit="s"))


def datetime_to_period_string(value: datetime, resolution: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    value = _normalize_datetime_for_period(value)
    if "T" in resolution.upper():
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if resolution == "P1D":
        return value.date().isoformat()
    if resolution == "P1W":
        iso_year, iso_week, _ = value.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    if resolution == "P1M":
        return f"{value.year:04d}-{value.month:02d}"
    if resolution == "P1Y":
        return str(value.year)
    raise ValueError(f"Unsupported resolution '{resolution}'")


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


def parse_weekly_period_string(value: str) -> datetime:
    """Parse a dataset-native weekly period string or full ISO datetime."""
    match = _WEEKLY_PERIOD_PATTERN.fullmatch(value)
    if match is not None:
        iso_year = int(match.group("year"))
        iso_week = int(match.group("week"))
        return datetime.combine(date.fromisocalendar(iso_year, iso_week, 1), datetime.min.time())
    return datetime.fromisoformat(value)


def normalize_period_string(value: str, resolution: str) -> str:
    """Normalize an input period string to the dataset-native period format."""
    if "T" in resolution.upper():
        try:
            return datetime_to_period_string(parse_hourly_period_string(value), resolution)
        except ValueError as exc:
            raise ValueError(f"Invalid hourly period '{value}'; expected YYYY-MM-DDTHH or ISO datetime") from exc
    if resolution == "P1D":
        try:
            return datetime_to_period_string(datetime.fromisoformat(value), resolution)
        except ValueError as exc:
            raise ValueError(f"Invalid daily period '{value}'; expected YYYY-MM-DD or ISO datetime") from exc
    if resolution == "P1W":
        try:
            return datetime_to_period_string(parse_weekly_period_string(value), resolution)
        except ValueError as exc:
            raise ValueError(f"Invalid weekly period '{value}'; expected YYYY-Www or ISO datetime") from exc
    if resolution == "P1M":
        try:
            if len(value) == 7:
                datetime.fromisoformat(f"{value}-01")
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), resolution)
        except ValueError as exc:
            raise ValueError(f"Invalid monthly period '{value}'; expected YYYY-MM or ISO datetime") from exc
    if resolution == "P1Y":
        try:
            if len(value) == 4:
                int(value)
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), resolution)
        except ValueError as exc:
            raise ValueError(f"Invalid yearly period '{value}'; expected YYYY or ISO datetime") from exc
    raise ValueError(f"Unsupported resolution '{resolution}'")


def parse_period_string_to_datetime(value: str) -> datetime:
    """Parse a dataset-native period string to a UTC datetime."""
    normalized = value.strip()
    if _WEEKLY_PERIOD_PATTERN.fullmatch(normalized) is not None:
        return parse_weekly_period_string(normalized).replace(tzinfo=UTC)
    if "T" not in normalized:
        if len(normalized) == 4:
            normalized = f"{normalized}-01-01T00:00:00"
        elif len(normalized) == 7:
            normalized = f"{normalized}-01T00:00:00"
        else:
            normalized = f"{normalized}T00:00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], resolution: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    if resolution != "P1W":
        lengths = {"PT1H": 13, "P1D": 10, "P1M": 7, "P1Y": 4}
        length = lengths.get(resolution, 13 if "T" in resolution.upper() else 10)
        return np.datetime_as_string(datetimes, unit="s").astype(f"U{length}")

    dt_index = pd.DatetimeIndex(np.atleast_1d(np.asarray(datetimes, dtype="datetime64[ns]")))
    iso = dt_index.isocalendar()
    strings = iso["year"].astype(str).str.zfill(4) + "-W" + iso["week"].astype(str).str.zfill(2)
    return cast(np.ndarray[Any, Any], strings.to_numpy().astype("U8"))
