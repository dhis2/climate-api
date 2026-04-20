from datetime import UTC, date, datetime

import pytest

from climate_api.ingestions import services, sync_engine
from climate_api.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    DatasetDetailRecord,
    DatasetPublication,
    PublicationStatus,
    SyncAction,
    SyncKind,
)


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path="/tmp/chirps3_precipitation_daily.zarr",
        asset_paths=["/tmp/chirps3_precipitation_daily.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
            extent_id="sle",
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end=end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime.fromisoformat(created_at),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def _dataset_detail(dataset_id: str) -> DatasetDetailRecord:
    return DatasetDetailRecord(
        dataset_id=dataset_id,
        source_dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        short_name="CHIRPS3 precip",
        variable="precip",
        period_type="daily",
        units="mm",
        resolution="5 km x 5 km",
        source="CHIRPS v3",
        source_url="https://example.com/chirps",
        extent=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-11"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        last_updated=datetime(2026, 1, 11, tzinfo=UTC),
        links=[],
        publication=DatasetPublication(
            status=PublicationStatus.PUBLISHED,
            published_at=datetime(2026, 1, 11, tzinfo=UTC),
        ),
        versions=[],
    )


def test_sync_dataset_returns_up_to_date_when_no_new_period_is_due(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    monkeypatch.setattr(
        services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31"),
    )
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily"},
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-01-31", prefer_zarr=True, publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.message == "Managed dataset is already current with upstream source state."
    assert result.dataset.dataset_id == dataset_id
    assert result.sync_detail.sync_kind == SyncKind.TEMPORAL
    assert result.sync_detail.action == SyncAction.NO_OP
    assert result.sync_detail.reason == "no_new_period"


def test_sync_dataset_creates_new_version_from_next_period(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync_kind": "temporal"},
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10")

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))
    result = services.sync_dataset(dataset_id=dataset_id, end="2026-02-10", prefer_zarr=True, publish=True)

    assert captured["start"] == "2026-01-01"
    assert captured["end"] == "2026-02-10"
    assert captured["extent_id"] == "sle"
    assert captured["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert captured["country_code"] is None
    assert result.sync_id == "a2"
    assert result.status == "completed"
    assert result.message == "Managed dataset was rematerialized against the latest planned upstream state."
    assert result.sync_detail.sync_kind == SyncKind.TEMPORAL
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert result.sync_detail.reason == "new_periods_available"
    assert result.sync_detail.requested_start == "2026-01-01"
    assert result.sync_detail.requested_end == "2026-02-10"


def test_sync_dataset_release_policy_returns_up_to_date_when_release_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "worldpop_population_yearly_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="worldpop_population_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "worldpop_population_yearly", "period_type": "yearly", "sync_kind": "release"},
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2024", prefer_zarr=True, publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.sync_detail.sync_kind == SyncKind.RELEASE
    assert result.sync_detail.action == SyncAction.NO_OP
    assert result.sync_detail.reason == "no_new_release"


def test_sync_dataset_release_policy_clamps_future_year_by_template_availability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "release_dataset_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="release_dataset_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)

    class FakeDateTime:
        @staticmethod
        def today() -> object:
            class _Today:
                year = 2026

                def isoformat(self) -> str:
                    return "2026-04-15"

                def __sub__(self, other: object) -> object:
                    raise AssertionError("date subtraction is not expected in this yearly sync test")

            return _Today()

    monkeypatch.setattr(sync_engine, "date", FakeDateTime)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "release_dataset_yearly",
            "period_type": "yearly",
            "sync_kind": "release",
            "sync_availability": {"latest_year_offset": 1},
        },
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(
            artifact_id="a2",
            source_dataset_id="release_dataset_yearly",
            managed_dataset_id=dataset_id,
            end="2025",
        )

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026", prefer_zarr=True, publish=True)

    assert captured["start"] == "2026-01-01"
    assert captured["end"] == "2025"
    assert result.status == "completed"
    assert result.sync_detail.sync_kind == SyncKind.RELEASE
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert result.sync_detail.requested_end == "2025"
    assert result.sync_detail.latest_available_end == "2025"


def test_latest_available_end_clamps_monthly_lag_to_month_period(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 15)

    monkeypatch.setattr(sync_engine, "date", FixedDate)

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "monthly_dataset",
            "period_type": "monthly",
            "sync_availability": {"lag_days": 0},
        },
        requested_end="2026-05",
    )

    assert result == "2026-05"

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "monthly_dataset",
            "period_type": "monthly",
            "sync_availability": {"lag_days": 1},
        },
        requested_end="2026-05",
    )

    assert result == "2026-04"
