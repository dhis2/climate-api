from datetime import UTC, date, datetime

import pytest
from fastapi.testclient import TestClient

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
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync_kind": "temporal",
            "cache_info": {},
        },
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
    assert (
        result.sync_detail.message
        == "Data already exists through 2026-01-31; target 2026-01-31 does not require a new download."
    )


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
    assert (
        result.sync_detail.message
        == "Data exists through 2026-01-31. Sync will rematerialize the dataset through 2026-02-10."
    )
    assert result.sync_detail.current_start == "2026-01-01"
    assert result.sync_detail.current_end == "2026-01-31"
    assert result.sync_detail.target_end == "2026-02-10"
    assert result.sync_detail.target_end_source == "request"
    assert result.sync_detail.delta_start == "2026-02-01"
    assert result.sync_detail.delta_end == "2026-02-10"


def test_sync_dataset_append_policy_downloads_only_delta_but_preserves_full_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync_kind": "temporal",
            "sync_execution": "append",
            "cache_info": {},
        },
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
    assert captured["download_start"] == "2026-02-01"
    assert captured["download_end"] == "2026-02-10"
    assert result.sync_detail.action == SyncAction.APPEND
    assert result.sync_detail.reason == "new_periods_available_for_append"
    assert (
        result.sync_detail.message == "Data exists through 2026-01-31. Sync will download missing periods "
        "2026-02-01 through 2026-02-10 and rebuild coverage through 2026-02-10."
    )
    assert result.sync_detail.current_start == "2026-01-01"
    assert result.sync_detail.current_end == "2026-01-31"
    assert result.sync_detail.target_end == "2026-02-10"
    assert result.sync_detail.target_end_source == "request"
    assert result.sync_detail.delta_start == "2026-02-01"
    assert result.sync_detail.delta_end == "2026-02-10"


def test_sync_dataset_append_policy_falls_back_to_rematerialize_for_multiscales(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        lambda _: {
            "id": "worldpop_population_yearly",
            "period_type": "yearly",
            "sync_kind": "temporal",
            "sync_execution": "append",
            "cache_info": {"multiscales": {"levels": 4}},
        },
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(
            artifact_id="a2",
            source_dataset_id="worldpop_population_yearly",
            managed_dataset_id=dataset_id,
            end="2025",
        )

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2025", prefer_zarr=True, publish=True)

    assert "download_start" in captured
    assert captured["download_start"] is None
    assert result.sync_detail.action == SyncAction.REMATERIALIZE


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
    assert result.sync_detail.target_end == "2025"
    assert result.sync_detail.target_end_source == "request_clamped_by_availability"


def test_sync_dataset_static_policy_returns_not_syncable_without_period_arithmetic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "static_dataset_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="static_dataset",
        managed_dataset_id=dataset_id,
        end="static-release",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "static_dataset", "period_type": "unsupported-static-period", "sync_kind": "static"},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("static sync should not create artifacts"))
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="ignored", prefer_zarr=True, publish=True)

    assert result.sync_id is None
    assert result.status == "not_syncable"
    assert result.sync_detail.sync_kind == SyncKind.STATIC
    assert result.sync_detail.action == SyncAction.NOT_SYNCABLE
    assert result.sync_detail.reason == "static_dataset"


def test_plan_sync_requires_sync_kind() -> None:
    latest = _artifact(artifact_id="a1", end="2026-01-31")

    with pytest.raises(ValueError, match="must define sync_kind"):
        sync_engine.plan_sync(
            source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily"},
            latest_artifact=latest,
            requested_end="2026-02-10",
        )


def test_plan_sync_dataset_returns_plan_without_creating_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync_kind": "temporal"},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("sync plan should not create artifacts"))

    result = services.plan_sync_dataset(dataset_id=dataset_id, end="2026-02-10")

    assert result.sync_kind == SyncKind.TEMPORAL
    assert result.action == SyncAction.REMATERIALIZE
    assert result.reason == "new_periods_available"
    assert result.current_start == "2026-01-01"
    assert result.current_end == "2026-01-31"
    assert result.target_end == "2026-02-10"
    assert result.target_end_source == "request"
    assert result.delta_start == "2026-02-01"
    assert result.delta_end == "2026-02-10"


def test_sync_plan_route_returns_plan_without_creating_artifact(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync_kind": "temporal"},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("sync plan should not create artifacts"))

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "2026-02-10"})

    assert response.status_code == 200
    assert response.json() == {
        "source_dataset_id": "chirps3_precipitation_daily",
        "extent_id": "sle",
        "sync_kind": "temporal",
        "action": "rematerialize",
        "reason": "new_periods_available",
        "message": "Data exists through 2026-01-31. Sync will rematerialize the dataset through 2026-02-10.",
        "current_start": "2026-01-01",
        "current_end": "2026-01-31",
        "target_end": "2026-02-10",
        "target_end_source": "request",
        "delta_start": "2026-02-01",
        "delta_end": "2026-02-10",
    }


def test_sync_route_executes_rematerialize_and_returns_structured_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync_kind": "temporal"},
    )
    monkeypatch.setattr(
        services,
        "create_artifact",
        lambda **_: _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10"),
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    response = client.post(
        f"/sync/{dataset_id}",
        json={"end": "2026-02-10", "prefer_zarr": True, "publish": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sync_id"] == "a2"
    assert payload["status"] == "completed"
    assert payload["dataset"]["dataset_id"] == dataset_id
    assert payload["sync_detail"]["sync_kind"] == "temporal"
    assert payload["sync_detail"]["action"] == "rematerialize"
    assert payload["sync_detail"]["target_end"] == "2026-02-10"


def test_latest_available_end_preserves_requested_month_without_lag(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_plan_sync_marks_default_target_end_source(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(sync_engine, "date", FixedDate)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync_kind": "temporal",
            "sync_execution": "append",
            "cache_info": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2024-02-29"),
        requested_end=None,
    )

    assert result.target_end == "2026-04-20"
    assert result.target_end_source == "default_today"
    assert result.delta_start == "2024-03-01"
    assert result.delta_end == "2026-04-20"


def test_plan_sync_marks_request_target_clamped_by_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: lambda: "2026-03-31")

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync_kind": "temporal",
            "sync_execution": "append",
            "cache_info": {},
            "sync_availability": {
                "latest_available_function": "climate_api.providers.availability.chirps3_daily_latest_available"
            },
        },
        latest_artifact=_artifact(artifact_id="a1", end="2026-02-28"),
        requested_end="2026-04-21",
    )

    assert result.target_end == "2026-03-31"
    assert result.target_end_source == "request_clamped_by_availability"
    assert result.delta_start == "2026-03-01"
    assert result.delta_end == "2026-03-31"


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
            "sync_availability": {"lag_days": 1},
        },
        requested_end="2026-05",
    )

    assert result == "2026-04"


def test_latest_available_end_uses_provider_availability_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_latest_available(*, dataset: dict[str, object], requested_end: str) -> str:
        calls.append({"dataset": dataset, "requested_end": requested_end})
        return "2026-02-05"

    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: fake_latest_available)

    source_dataset = {
        "id": "provider_dataset",
        "period_type": "daily",
        "sync_availability": {"latest_available_function": "provider.latest_available"},
    }
    result = sync_engine._latest_available_end(source_dataset=source_dataset, requested_end="2026-02-10")

    assert result == "2026-02-05"
    assert calls == [{"dataset": source_dataset, "requested_end": "2026-02-10"}]


def test_latest_available_end_clamps_provider_availability_to_requested_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: lambda: "2026-03-01")

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "provider_dataset",
            "period_type": "daily",
            "sync_availability": {"latest_available_function": "provider.latest_available"},
        },
        requested_end="2026-02-10",
    )

    assert result == "2026-02-10"
