import pytest

from climate_api.shared.time import (
    _iso_duration_to_offset,
    _period_family,
    datetime_to_period_string,
    normalize_period_string,
    parse_period_string_to_datetime,
)


def test_iso_duration_to_offset() -> None:
    import pandas as pd

    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("PT1H")).isoformat() == "2024-02-29T23:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("P1D")).isoformat() == "2024-02-29T00:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("P1W")).isoformat() == "2024-02-23T00:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("P1M")).isoformat() == "2024-02-01T00:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("P1Y")).isoformat() == "2023-03-01T00:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("P2W")).isoformat() == "2024-02-16T00:00:00"
    assert (pd.Timestamp("2024-03-11") - _iso_duration_to_offset("P10D")).isoformat() == "2024-03-01T00:00:00"
    assert (pd.Timestamp("2024-03-01") - _iso_duration_to_offset("PT6H")).isoformat() == "2024-02-29T18:00:00"


def test_period_family_maps_canonical_values() -> None:
    assert _period_family("PT1H") == "PT1H"
    assert _period_family("P1D") == "P1D"
    assert _period_family("P1W") == "P1W"
    assert _period_family("P1M") == "P1M"
    assert _period_family("P1Y") == "P1Y"


def test_period_family_maps_non_canonical_multipliers() -> None:
    assert _period_family("PT6H") == "PT1H"
    assert _period_family("P10D") == "P1D"
    assert _period_family("P2W") == "P1W"
    assert _period_family("P3M") == "P1M"
    assert _period_family("P2Y") == "P1Y"


def test_period_family_maps_compound_to_largest_component() -> None:
    assert _period_family("P1Y6M") == "P1Y"
    assert _period_family("P1DT12H") == "PT1H"


def test_datetime_to_period_string_uses_family_for_non_canonical_period_type() -> None:
    from datetime import datetime

    value = datetime(2026, 1, 11)
    assert datetime_to_period_string(value, "P10D") == "2026-01-11"
    assert datetime_to_period_string(value, "PT6H") == "2026-01-11T00"
    assert datetime_to_period_string(value, "P3M") == "2026-01"


def test_normalize_period_string_accepts_non_canonical_period_type() -> None:
    assert normalize_period_string("2026-01-11", "P10D") == "2026-01-11"
    assert normalize_period_string("2026-04-21T13", "PT6H") == "2026-04-21T13"


def test_normalize_period_string_raises_targeted_monthly_error() -> None:
    with pytest.raises(ValueError, match="Invalid monthly period '2024-13'; expected YYYY-MM or ISO datetime"):
        normalize_period_string("2024-13", "P1M")


def test_normalize_period_string_accepts_dataset_native_hourly_period() -> None:
    assert normalize_period_string("2026-04-21T13", "PT1H") == "2026-04-21T13"


def test_normalize_period_string_converts_aware_hourly_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T13:30:00+02:00", "PT1H") == "2026-04-21T11"


def test_normalize_period_string_converts_aware_daily_datetime_to_utc_period() -> None:
    assert normalize_period_string("2026-04-21T00:30:00+02:00", "P1D") == "2026-04-20"


def test_normalize_period_string_accepts_dataset_native_weekly_period() -> None:
    assert normalize_period_string("2026-W17", "P1W") == "2026-W17"


def test_normalize_period_string_converts_datetime_to_weekly_period() -> None:
    assert normalize_period_string("2026-04-21T13:30:00+00:00", "P1W") == "2026-W17"


def test_datetime_to_period_string_converts_aware_monthly_datetime_to_utc_period() -> None:
    from datetime import datetime

    value = datetime.fromisoformat("2026-05-01T00:30:00+02:00")

    assert datetime_to_period_string(value, "P1M") == "2026-04"


def test_normalize_period_string_rejects_invalid_weekly_period() -> None:
    with pytest.raises(ValueError, match="Invalid weekly period '2026-W54'; expected YYYY-Www or ISO datetime"):
        normalize_period_string("2026-W54", "P1W")


def test_parse_period_string_to_datetime_accepts_dataset_native_hourly_period() -> None:
    parsed = parse_period_string_to_datetime("2026-04-21T13")

    assert parsed.isoformat() == "2026-04-21T13:00:00+00:00"


def test_parse_period_string_to_datetime_accepts_dataset_native_weekly_period() -> None:
    parsed = parse_period_string_to_datetime("2026-W17")

    assert parsed.isoformat() == "2026-04-20T00:00:00+00:00"
