"""Time helpers shared across Climate API modules."""

import re
from datetime import UTC, date, datetime
from typing import Any, cast

import numpy as np
import pandas as pd

_WEEKLY_PERIOD_PATTERN = re.compile(r"^(?P<year>\d{4})-W(?P<week>\d{2})$")
_ISO_8601_DURATION_RE = re.compile(
    r"P(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?"
)


def _iso_duration_to_offset(period_type: str) -> pd.DateOffset:
    """Convert an ISO 8601 duration string to a pandas DateOffset."""
    m = _ISO_8601_DURATION_RE.fullmatch(period_type)
    if not m:
        raise ValueError(f"Invalid ISO 8601 duration '{period_type}'")
    g = m.groupdict(default="0")
    return pd.DateOffset(
        years=int(g["years"]),
        months=int(g["months"]),
        weeks=int(g["weeks"]),
        days=int(g["days"]),
        hours=int(g["hours"]),
        minutes=int(g["minutes"]),
        seconds=int(g["seconds"]),
    )


def _iso_resolution_to_frequency(period_type: str) -> str:
    """Convert an ISO 8601 duration to a pandas frequency alias for xarray resampling.

    Weekly durations are anchored on Monday for ISO week compatibility.
    Month and year durations use period-start labels (MS, YS).
    """
    m = _ISO_8601_DURATION_RE.fullmatch(period_type)
    if not m:
        raise ValueError(f"Invalid ISO 8601 duration '{period_type}'")
    g = m.groupdict(default="0")
    years, months, weeks, days, hours, minutes, seconds = (
        int(g[k]) for k in ("years", "months", "weeks", "days", "hours", "minutes", "seconds")
    )
    if years:
        return f"{years}YS"
    if months:
        return f"{months}MS"
    if weeks:
        return "W-MON" if weeks == 1 else f"{weeks}W-MON"
    if days:
        return f"{days}D"
    if hours:
        return f"{hours}h"
    if minutes:
        return f"{minutes}min"
    if seconds:
        return f"{seconds}s"
    raise ValueError(f"ISO 8601 duration '{period_type}' specifies no time components")


def _period_family(period_type: str) -> str:
    """Map an ISO 8601 duration to the canonical period type used for string formatting.

    Compound durations are resolved by their largest component (years beat months, etc.).
    Sub-daily durations (any with a T designator) map to the hourly family.
    """
    if "T" in period_type:  # sub-daily: PTnH, PTnM, PnDTnH, …
        return "PT1H"
    if "Y" in period_type:
        return "P1Y"
    if "M" in period_type:  # months (date portion — no T present)
        return "P1M"
    if "W" in period_type:
        return "P1W"
    if "D" in period_type:
        return "P1D"
    raise ValueError(f"Unsupported period_type '{period_type}'")


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


def datetime_to_period_string(value: datetime, period_type: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    value = _normalize_datetime_for_period(value)
    family = _period_family(period_type)
    if family == "PT1H":  # hourly
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if family == "P1D":  # daily
        return value.date().isoformat()
    if family == "P1W":  # weekly
        iso_year, iso_week, _ = value.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    if family == "P1M":  # monthly
        return f"{value.year:04d}-{value.month:02d}"
    return str(value.year)  # yearly


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


def normalize_period_string(value: str, period_type: str) -> str:
    """Normalize an input period string to the dataset-native period format."""
    family = _period_family(period_type)
    if family == "PT1H":  # hourly
        try:
            return datetime_to_period_string(parse_hourly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid hourly period '{value}'; expected YYYY-MM-DDTHH or ISO datetime") from exc
    if family == "P1D":  # daily
        try:
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid daily period '{value}'; expected YYYY-MM-DD or ISO datetime") from exc
    if family == "P1W":  # weekly
        try:
            return datetime_to_period_string(parse_weekly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid weekly period '{value}'; expected YYYY-Www or ISO datetime") from exc
    if family == "P1M":  # monthly
        try:
            if len(value) == 7:
                datetime.fromisoformat(f"{value}-01")
                return value
            return datetime_to_period_string(datetime.fromisoformat(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid monthly period '{value}'; expected YYYY-MM or ISO datetime") from exc
    # yearly
    try:
        if len(value) == 4:
            int(value)
            return value
        return datetime_to_period_string(datetime.fromisoformat(value), period_type)
    except ValueError as exc:
        raise ValueError(f"Invalid yearly period '{value}'; expected YYYY or ISO datetime") from exc


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


def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    family = _period_family(period_type)
    if family != "P1W":
        lengths = {"PT1H": 13, "P1D": 10, "P1M": 7, "P1Y": 4}
        return np.datetime_as_string(datetimes, unit="s").astype(f"U{lengths[family]}")

    dt_index = pd.DatetimeIndex(np.atleast_1d(np.asarray(datetimes, dtype="datetime64[ns]")))
    iso = dt_index.isocalendar()
    strings = iso["year"].astype(str).str.zfill(4) + "-W" + iso["week"].astype(str).str.zfill(2)
    return cast(np.ndarray[Any, Any], strings.to_numpy().astype("U8"))
