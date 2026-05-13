"""Time helpers shared across Climate API modules."""

import re
from datetime import UTC, date, datetime
from typing import Any, cast

import numpy as np
import pandas as pd

# Convenience defaults for well-known period type names.
# Custom datasets that use a non-standard period_type can declare their own
# ISO 8601 step directly in the template YAML via the `period_step` field —
# no changes to core code are needed in that case.
_PERIOD_TYPE_ISO_STEP: dict[str, str] = {
    "hourly": "PT1H",
    "daily": "P1D",
    "dekadal": "P10D",
    "weekly": "P7D",
    "monthly": "P1M",
    "yearly": "P1Y",
}

_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def period_type_to_iso_step(period_type: str) -> str | None:
    """Return the ISO 8601 duration step for a known period type, or None if unrecognised."""
    return _PERIOD_TYPE_ISO_STEP.get(period_type)


def resolve_iso_step(dataset: dict[str, Any]) -> str | None:
    """Return the ISO 8601 duration step for a dataset.

    Resolution order:
    1. ``extents.temporal.resolution`` — the authoritative field already present in
       every template (e.g. ``P1D``, ``PT1H``).  Custom datasets with any period type
       are fully supported here without touching core code.
    2. Built-in lookup table — fallback for templates that pre-date the
       ``extents.temporal.resolution`` field or omit it.

    See issue #94: the long-term direction is to derive ``period_type`` itself from
    ``extents.temporal.resolution`` and remove the duplication.
    """
    resolution = dataset.get("extents", {})
    if isinstance(resolution, dict):
        resolution = resolution.get("temporal", {})
        if isinstance(resolution, dict):
            resolution = resolution.get("resolution")
    if resolution:
        return str(resolution)
    return period_type_to_iso_step(str(dataset.get("period_type", "")))


def _iso_step_to_approx_hours(step: str) -> float:
    """Return the approximate duration in hours for an ISO 8601 duration string.

    Months and years use calendar averages (30.4375 days/month, 365.25 days/year).
    Raises ValueError for unrecognised formats.
    """
    m = _ISO_DURATION_RE.fullmatch(step)
    if not m:
        raise ValueError(f"Cannot parse ISO 8601 duration: '{step}'")
    years, months, weeks, days, hours, minutes, seconds = (int(g or 0) for g in m.groups())
    return (
        years * 365.25 * 24 + months * 30.4375 * 24 + weeks * 7 * 24 + days * 24 + hours + minutes / 60 + seconds / 3600
    )


def time_chunk_for_iso_step(step: str) -> int:
    """Return a suitable zarr time chunk size for a given ISO 8601 duration step.

    Targets roughly one week of data for sub-daily steps, one month for daily/sub-weekly
    steps, and one year for weekly and coarser steps.  This keeps individual chunk files
    at a manageable size while covering a natural analysis window in one read.
    """
    hours = _iso_step_to_approx_hours(step)
    if hours < 24:
        return max(1, round(24 * 7 / hours))  # ~1 week
    if hours < 24 * 7:
        return max(1, round(24 * 30 / hours))  # ~1 month
    return max(1, round(24 * 365.25 / hours))  # ~1 year


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


def datetime_to_period_string(value: datetime, period_type: str) -> str:
    """Convert a datetime to the dataset-native period string format."""
    value = _normalize_datetime_for_period(value)
    if period_type == "hourly":
        return value.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H")
    if period_type == "daily":
        return value.date().isoformat()
    if period_type == "weekly":
        iso_year, iso_week, _ = value.isocalendar()
        return f"{iso_year:04d}-W{iso_week:02d}"
    if period_type == "monthly":
        return f"{value.year:04d}-{value.month:02d}"
    if period_type == "yearly":
        return str(value.year)
    raise ValueError(f"Unsupported period_type '{period_type}'")


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
    if period_type == "weekly":
        try:
            return datetime_to_period_string(parse_weekly_period_string(value), period_type)
        except ValueError as exc:
            raise ValueError(f"Invalid weekly period '{value}'; expected YYYY-Www or ISO datetime") from exc
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
    raise ValueError(f"Unsupported period_type '{period_type}'")


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
    if period_type != "weekly":
        lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
        return np.datetime_as_string(datetimes, unit="s").astype(f"U{lengths[period_type]}")

    dt_index = pd.DatetimeIndex(np.atleast_1d(np.asarray(datetimes, dtype="datetime64[ns]")))
    iso = dt_index.isocalendar()
    strings = iso["year"].astype(str).str.zfill(4) + "-W" + iso["week"].astype(str).str.zfill(2)
    return cast(np.ndarray[Any, Any], strings.to_numpy().astype("U8"))
