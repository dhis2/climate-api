import pytest

from climate_api.shared.time import normalize_period_string


def test_normalize_period_string_raises_targeted_monthly_error() -> None:
    with pytest.raises(ValueError, match="Invalid monthly period '2024-13'; expected YYYY-MM or ISO datetime"):
        normalize_period_string("2024-13", "monthly")


def test_normalize_period_string_accepts_dataset_native_hourly_period() -> None:
    assert normalize_period_string("2026-04-21T13", "hourly") == "2026-04-21T13"
