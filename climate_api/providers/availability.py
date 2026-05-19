"""Provider availability policies used by sync planning.

These functions keep source-specific release cadence rules out of the generic
sync engine. They are intentionally small and metadata-driven so dataset YAML can
choose the right policy per upstream provider.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from climate_api.shared.time import datetime_to_period_string, utc_now, utc_today


def lagged_latest_available(*, dataset: dict[str, Any], requested_end: str) -> str:
    """Return latest available period by applying YAML-declared lag metadata."""
    availability = _availability_metadata(dataset)
    period_type = str(dataset.get("period_type", "daily"))

    if period_type == "hourly":
        lag_hours = availability.get("lag_hours")
        if isinstance(lag_hours, int) and lag_hours > 0:
            latest = utc_now() - timedelta(hours=lag_hours)
            return datetime_to_period_string(latest, period_type)
        return requested_end

    lag_days = availability.get("lag_days")
    if period_type in {"daily", "monthly"} and isinstance(lag_days, int) and lag_days > 0:
        latest_date = utc_today() - timedelta(days=lag_days)
        if period_type == "monthly":
            return f"{latest_date.year:04d}-{latest_date.month:02d}"
        return latest_date.isoformat()

    if period_type == "yearly":
        latest_year_offset = availability.get("latest_year_offset")
        if isinstance(latest_year_offset, int) and latest_year_offset >= 0:
            return str(utc_today().year - latest_year_offset)

    return requested_end


def _availability_metadata(dataset: dict[str, Any]) -> dict[str, Any]:
    """Return sync availability metadata from a dataset template."""
    availability = dataset.get("sync", {}).get("availability")
    return availability if isinstance(availability, dict) else {}
