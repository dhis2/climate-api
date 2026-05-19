from datetime import UTC, date, datetime

import pytest

from climate_api.providers import availability


def test_lagged_latest_available_formats_hourly_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> "FixedDateTime":  # noqa: ANN401
            return cls(2026, 4, 21, 12, 34, tzinfo=UTC)

    monkeypatch.setattr(availability, "utc_now", lambda: FixedDateTime(2026, 4, 21, 12, 34, tzinfo=UTC))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "hourly",
            "sync": {"availability": {"lag_hours": 5}},
        },
        requested_end="2026-04-21T12:00:00",
    )

    assert result == "2026-04-21T07"


def test_lagged_latest_available_formats_daily_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 21)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 21))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "daily",
            "sync": {"availability": {"lag_days": 2}},
        },
        requested_end="2026-04-21",
    )

    assert result == "2026-04-19"


def test_lagged_latest_available_formats_yearly_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 21)

    monkeypatch.setattr(availability, "utc_today", lambda: FixedDate(2026, 4, 21))

    result = availability.lagged_latest_available(
        dataset={
            "period_type": "yearly",
            "sync": {"availability": {"latest_year_offset": 1}},
        },
        requested_end="2028",
    )

    assert result == "2025"
