import pytest

from climate_api.shared.time import datetime_to_period_string, normalize_period_string


def test_normalize_period_string_raises_targeted_monthly_error() -> None:
    with pytest.raises(ValueError, match="Invalid monthly period '2024-13'; expected YYYY-MM or ISO datetime"):
        normalize_period_string("2024-13", "monthly")


def test_normalize_period_string_accepts_dataset_native_hourly_period() -> None:
    assert normalize_period_string("2026-04-21T13", "hourly") == "2026-04-21T13"


def test_normalize_period_string_converts_aware_hourly_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T13:30:00+02:00", "hourly") == "2026-04-21T11"


def test_normalize_period_string_converts_aware_daily_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T00:30:00+02:00", "daily") == "2026-04-20"


def test_datetime_to_period_string_converts_aware_monthly_datetime_to_utc_period() -> None:
    from datetime import datetime

    value = datetime.fromisoformat("2026-05-01T00:30:00+02:00")

    assert datetime_to_period_string(value, "monthly") == "2026-04"


def test_normalize_period_string_rejects_unsupported_period_type() -> None:
    with pytest.raises(ValueError, match="Unsupported period_type 'weekly'"):
        normalize_period_string("2026-W17", "weekly")
