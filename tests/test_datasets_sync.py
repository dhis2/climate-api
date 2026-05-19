from datetime import UTC, date, datetime, tzinfo
from pathlib import Path

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
    SyncDetail,
    SyncKind,
    SyncResponse,
)


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
    path: str = "/tmp/chirps3_precipitation_daily.zarr",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path=path,
        asset_paths=[path],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
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
            "sync": {"kind": "temporal"},
            "ingestion": {},
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
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
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
    assert captured["bbox"] == [1.0, 2.0, 3.0, 4.0]
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
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
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


def test_sync_dataset_append_policy_falls_back_to_rematerialize_for_pyramid_zarr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Simulate a pyramid zarr on disk by creating a "0/" subdirectory.
    zarr_path = tmp_path / "worldpop_population_yearly.zarr"
    (zarr_path / "0").mkdir(parents=True)

    dataset_id = "worldpop_population_yearly_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="worldpop_population_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
        path=str(zarr_path),
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "worldpop_population_yearly",
            "period_type": "yearly",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
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

    warnings: list[str] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = services.sync_dataset(dataset_id=dataset_id, end="2025", prefer_zarr=True, publish=True)

    assert "download_start" in captured
    assert captured["download_start"] is None
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert any("falling back to rematerialize" in message for message in warnings)


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
        lambda _: {"id": "worldpop_population_yearly", "period_type": "yearly", "sync": {"kind": "release"}},
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2024", prefer_zarr=True, publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.sync_detail.sync_kind == SyncKind.RELEASE
    assert result.sync_detail.action == SyncAction.NO_OP
    assert result.sync_detail.reason == "no_new_release"


def test_default_hourly_target_end_is_utc_aware(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDateTime":
            return cls(2026, 4, 21, 13, 47, 31, tzinfo=tz if tz is UTC else None)

    monkeypatch.setattr(sync_engine, "utc_now", lambda: FixedDateTime(2026, 4, 21, 13, 47, 31, tzinfo=UTC))

    result = sync_engine._default_target_end(period_type="hourly")

    assert result == "2026-04-21T13"


def test_default_target_end_rejects_unsupported_period_type() -> None:
    with pytest.raises(ValueError, match="Unsupported period_type 'fortnightly' for sync"):
        sync_engine._default_target_end(period_type="fortnightly")


def test_next_period_start_preserves_hourly_period_format() -> None:
    result = sync_engine._next_period_start("2026-04-21T13", period_type="hourly")

    assert result == "2026-04-21T14"


def test_default_weekly_target_end_uses_iso_week_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "utc_now", lambda: datetime(2026, 4, 21, 13, 47, 31, tzinfo=UTC))

    result = sync_engine._default_target_end(period_type="weekly")

    assert result == "2026-W17"


def test_next_period_start_preserves_weekly_period_format() -> None:
    result = sync_engine._next_period_start("2026-W17", period_type="weekly")

    assert result == "2026-W18"


def test_next_period_start_rolls_weekly_period_across_iso_year_boundary() -> None:
    result = sync_engine._next_period_start("2020-W53", period_type="weekly")

    assert result == "2021-W01"


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
        lambda _: {"id": "static_dataset", "period_type": "unsupported-static-period", "sync": {"kind": "static"}},
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

    with pytest.raises(ValueError, match="must define sync.kind"):
        sync_engine.plan_sync(
            source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily"},
            latest_artifact=latest,
            requested_end="2026-02-10",
        )


def test_plan_sync_static_policy_ignores_period_normalization() -> None:
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="static_dataset",
        managed_dataset_id="static_dataset_sle",
        end="static-release",
    )

    result = sync_engine.plan_sync(
        source_dataset={"id": "static_dataset", "period_type": "unsupported-static-period", "sync": {"kind": "static"}},
        latest_artifact=latest,
        requested_end=None,
    )

    assert result.sync_kind == SyncKind.STATIC
    assert result.action == SyncAction.NOT_SYNCABLE
    assert result.reason == "static_dataset"


def test_plan_sync_dataset_returns_plan_without_creating_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
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
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("sync plan should not create artifacts"))

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "2026-02-10"})

    assert response.status_code == 200
    assert response.json() == {
        "source_dataset_id": "chirps3_precipitation_daily",
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


def test_sync_plan_route_returns_400_for_invalid_end_period(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "not-a-period"})

    assert response.status_code == 400


def test_plan_sync_treats_blank_end_as_default_target(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(sync_engine, "utc_today", lambda: FixedDate(2026, 4, 20))

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2024-02-29"),
        requested_end="",
    )

    assert result.target_end == "2026-04-20"
    assert result.target_end_source == "default_today"


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
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
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


def test_plan_sync_marks_default_target_end_source(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(sync_engine, "utc_today", lambda: FixedDate(2026, 4, 20))

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2024-02-29"),
        requested_end=None,
    )

    assert result.target_end == "2026-04-20"
    assert result.target_end_source == "default_today"
    assert result.delta_start == "2024-03-01"
    assert result.delta_end == "2026-04-20"


def test_latest_available_end_uses_plugin_periods_for_plugin_datasets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For plugin datasets, _latest_available_end calls plugin.periods() instead of
    latest_available_function."""
    monkeypatch.setattr(
        sync_engine,
        "_plugin_latest_available_period",
        lambda *, source_dataset, next_period_start, requested_end, current_end: "2026-02-08",
    )

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "daily",
            "sync": {"kind": "temporal"},
            "ingestion": {"plugin": "some.Plugin", "params": {}},
        },
        requested_end="2026-02-10",
        current_end="2026-02-06",
    )

    assert result == "2026-02-08"


def test_latest_available_end_plugin_returns_current_end_when_no_new_periods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When plugin.periods() returns an empty list, _latest_available_end returns current_end
    so the NOOP check fires correctly."""
    monkeypatch.setattr(
        sync_engine,
        "_plugin_latest_available_period",
        lambda *, source_dataset, next_period_start, requested_end, current_end: current_end,
    )

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "daily",
            "sync": {"kind": "temporal"},
            "ingestion": {"plugin": "some.Plugin", "params": {}},
        },
        requested_end="2026-02-10",
        current_end="2026-02-06",
    )

    assert result == "2026-02-06"


def test_plugin_latest_available_period_returns_last_period() -> None:
    """_plugin_latest_available_period returns the last item from plugin.periods()."""

    class FakePlugin:
        max_concurrency = 1
        commit_batch_size = 1
        rechunk_time = None

        async def probe(self, *_a: object, **_k: object) -> object:  # type: ignore[override]
            raise NotImplementedError

        async def periods(self, start: str, end: str) -> list[str]:
            return [d for d in ["2026-02-07", "2026-02-08", "2026-02-09"] if start <= d <= end]

        async def fetch_period(self, *_a: object, **_k: object) -> object:  # type: ignore[override]
            raise NotImplementedError

    import climate_api.ingest.orchestrator as orch_mod

    orig = orch_mod.load_plugin
    orch_mod.load_plugin = lambda path, params, extra_params=None: FakePlugin()  # type: ignore[assignment]
    try:
        result = sync_engine._plugin_latest_available_period(
            source_dataset={"ingestion": {"plugin": "fake.Plugin", "params": {}}},
            next_period_start="2026-02-07",
            requested_end="2026-02-10",
            current_end="2026-02-06",
        )
    finally:
        orch_mod.load_plugin = orig

    assert result == "2026-02-09"


def test_plugin_latest_available_period_returns_current_end_when_empty() -> None:
    """When plugin.periods() returns [], _plugin_latest_available_period returns current_end."""

    class EmptyPlugin:
        max_concurrency = 1
        commit_batch_size = 1
        rechunk_time = None

        async def probe(self, *_a: object, **_k: object) -> object:  # type: ignore[override]
            raise NotImplementedError

        async def periods(self, start: str, end: str) -> list[str]:
            return []

        async def fetch_period(self, *_a: object, **_k: object) -> object:  # type: ignore[override]
            raise NotImplementedError

    import climate_api.ingest.orchestrator as orch_mod

    orig = orch_mod.load_plugin
    orch_mod.load_plugin = lambda *_a, **_kw: EmptyPlugin()  # type: ignore[assignment]
    try:
        result = sync_engine._plugin_latest_available_period(
            source_dataset={"ingestion": {"plugin": "fake.Plugin", "params": {}}},
            next_period_start="2026-02-07",
            requested_end="2026-02-10",
            current_end="2026-02-06",
        )
    finally:
        orch_mod.load_plugin = orig

    assert result == "2026-02-06"


def test_plugin_latest_available_period_returns_none_on_instantiation_failure() -> None:
    """TypeError during load_plugin (e.g. plugin needs country_code) → returns None."""
    import climate_api.ingest.orchestrator as orch_mod

    orig = orch_mod.load_plugin

    def explode(path: str, params: dict, extra_params: object = None) -> object:
        raise TypeError("country_code is required")

    orch_mod.load_plugin = explode  # type: ignore[assignment]
    try:
        result = sync_engine._plugin_latest_available_period(
            source_dataset={"ingestion": {"plugin": "worldpop.Plugin", "params": {}}},
            next_period_start="2026",
            requested_end="2026",
            current_end="2025",
        )
    finally:
        orch_mod.load_plugin = orig

    assert result is None


def test_plan_sync_uses_plugin_periods_for_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    """For an ICECHUNK artifact backed by a plugin, plan_sync calls plugin.periods() to
    determine whether new data is available, not a static lag function."""
    monkeypatch.setattr(
        sync_engine,
        "_plugin_latest_available_period",
        lambda *, source_dataset, next_period_start, requested_end, current_end: "2024-01-01T05",
    )

    artifact = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")
    result = sync_engine.plan_sync(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal"},
            "ingestion": {
                "plugin": "climate_api.ingest.plugins.era5_land.Era5LandPlugin",
                "params": {"variable": "t2m"},
            },
        },
        latest_artifact=artifact,
        requested_end="2024-01-01T10",
    )

    assert result.action == "append"
    assert result.target_end == "2024-01-01T05"
    assert result.delta_start == "2024-01-01T04"
    assert result.delta_end == "2024-01-01T05"


def test_plan_sync_noop_when_plugin_reports_no_new_periods(monkeypatch: pytest.MonkeyPatch) -> None:
    """plan_sync returns NO_OP when plugin.periods() is empty (nothing new since current_end)."""
    monkeypatch.setattr(
        sync_engine,
        "_plugin_latest_available_period",
        lambda *, source_dataset, next_period_start, requested_end, current_end: current_end,
    )

    artifact = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")
    result = sync_engine.plan_sync(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal"},
            "ingestion": {
                "plugin": "climate_api.ingest.plugins.era5_land.Era5LandPlugin",
                "params": {"variable": "t2m"},
            },
        },
        latest_artifact=artifact,
        requested_end="2024-01-01T10",
    )

    assert result.action == "no_op"


def test_run_sync_raises_clear_error_when_append_invariants_are_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    latest_artifact = _artifact(
        artifact_id="a1",
        source_dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-02-10",
    )

    broken_plan = SyncDetail(
        source_dataset_id="chirps3_precipitation_daily",
        sync_kind=SyncKind.TEMPORAL,
        action=SyncAction.APPEND,
        reason="new_periods_available_for_append",
        message="broken test plan",
        current_start=None,
        current_end="2026-02-10",
        target_end="2026-02-11",
        target_end_source="request",
        delta_start="2026-02-11",
        delta_end="2026-02-11",
    )
    monkeypatch.setattr(sync_engine, "plan_sync", lambda **_: broken_plan)

    with pytest.raises(ValueError, match="Sync execution requires current_start"):
        sync_engine.run_sync(
            latest_artifact=latest_artifact,
            source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
            requested_end="2026-02-11",
            prefer_zarr=True,
            publish=True,
            create_artifact_fn=lambda **_: pytest.fail("create_artifact should not be called"),
            get_dataset_fn=lambda _: pytest.fail("get_dataset should not be called"),
        )


# ---------------------------------------------------------------------------
# Icechunk store-based sync
# ---------------------------------------------------------------------------


def _icechunk_artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "era5land_temperature_hourly",
    managed_dataset_id: str = "era5land_temperature_hourly_nor",
    end: str = "2024-01-01T03",
    path: str = "/tmp/era5land_temperature_hourly.icechunk",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="2m temperature (ERA5-Land)",
        variable="t2m",
        format=ArtifactFormat.ICECHUNK,
        path=path,
        asset_paths=[path],
        variables=["t2m"],
        request_scope=ArtifactRequestScope(
            start="2024-01-01T00",
            end=end,
            bbox=(4.0, 57.5, 31.5, 71.5),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2024-01-01T00", end=end),
            spatial=CoverageSpatial(xmin=4.0, ymin=57.5, xmax=31.5, ymax=71.5),
        ),
        created_at=datetime.fromisoformat("2024-01-01T04:00:00+00:00"),
        publication=ArtifactPublication(status=PublicationStatus.PUBLISHED),
    )


def test_plan_sync_uses_current_end_override_instead_of_artifact_metadata() -> None:
    """current_end parameter takes precedence over latest_artifact.coverage.temporal.end."""
    artifact = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal"},
        },
        latest_artifact=artifact,
        requested_end="2024-01-01T06",
        current_end="2024-01-01T05",
    )

    assert result.current_end == "2024-01-01T05"
    assert result.delta_start == "2024-01-01T06"
    assert result.delta_end == "2024-01-01T06"
    assert result.target_end == "2024-01-01T06"


def test_plan_sync_falls_back_to_artifact_end_when_no_override() -> None:
    artifact = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal"},
        },
        latest_artifact=artifact,
        requested_end="2024-01-01T06",
    )

    assert result.current_end == "2024-01-01T03"
    assert result.delta_start == "2024-01-01T04"


def test_supports_append_returns_true_for_icechunk_format_without_yaml_execution_flag() -> None:
    """ICECHUNK format always supports append — no sync.execution: append needed in YAML."""
    artifact = _icechunk_artifact(artifact_id="a1")

    result = sync_engine._supports_append(
        source_dataset={"id": "era5land_temperature_hourly", "period_type": "hourly", "sync": {"kind": "temporal"}},
        latest_artifact=artifact,
    )

    assert result is True


def test_supports_append_requires_yaml_execution_flag_for_zarr_format() -> None:
    zarr_artifact = _artifact(artifact_id="a1", end="2026-01-10")

    without_flag = sync_engine._supports_append(
        source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
        latest_artifact=zarr_artifact,
    )
    with_flag = sync_engine._supports_append(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
        },
        latest_artifact=zarr_artifact,
    )

    assert without_flag is False
    assert with_flag is True


def test_sync_dataset_reads_committed_end_from_icechunk_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """sync_dataset passes the store-authoritative current_end to run_sync for ICECHUNK artifacts."""
    dataset_id = "era5land_temperature_hourly_nor"
    latest = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal"},
        },
    )

    # Store has T00-T05; artifact record only knows about T03.
    import climate_api.ingest.store as ingest_store

    monkeypatch.setattr(
        ingest_store,
        "read_committed_period_ids",
        lambda path, period_type: {
            "2024-01-01T00",
            "2024-01-01T01",
            "2024-01-01T02",
            "2024-01-01T03",
            "2024-01-01T04",
            "2024-01-01T05",
        },
    )

    captured: dict[str, object] = {}

    def fake_run_sync(**kwargs: object) -> SyncResponse:
        captured.update(kwargs)
        return SyncResponse(
            sync_id=None,
            status="up_to_date",
            message="ok",
            dataset=_dataset_detail(dataset_id),
            sync_detail=SyncDetail(
                source_dataset_id="era5land_temperature_hourly",
                sync_kind=SyncKind.TEMPORAL,
                action=SyncAction.NO_OP,
                reason="no_new_period",
                message="ok",
                current_start="2024-01-01T00",
                current_end="2024-01-01T05",
                target_end="2024-01-01T05",
                target_end_source="request",
            ),
        )

    monkeypatch.setattr(services, "run_sync", fake_run_sync)

    services.sync_dataset(dataset_id=dataset_id, end="2024-01-01T05", prefer_zarr=False, publish=False)

    assert captured["current_end"] == "2024-01-01T05"


def test_sync_dataset_icechunk_store_empty_uses_none_current_end(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the store has no committed periods yet, current_end is None (full ingest)."""
    dataset_id = "era5land_temperature_hourly_nor"
    latest = _icechunk_artifact(artifact_id="a1", end="2024-01-01T03")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "era5land_temperature_hourly", "period_type": "hourly", "sync": {"kind": "temporal"}},
    )
    import climate_api.ingest.store as ingest_store

    monkeypatch.setattr(ingest_store, "read_committed_period_ids", lambda *_: set())

    captured: dict[str, object] = {}

    def fake_run_sync(**kwargs: object) -> SyncResponse:
        captured.update(kwargs)
        return SyncResponse(
            sync_id="a2",
            status="completed",
            message="ok",
            dataset=_dataset_detail(dataset_id),
            sync_detail=SyncDetail(
                source_dataset_id="era5land_temperature_hourly",
                sync_kind=SyncKind.TEMPORAL,
                action=SyncAction.REMATERIALIZE,
                reason="new_periods_available",
                message="ok",
                current_start="2024-01-01T00",
                current_end="2024-01-01T03",
                target_end="2024-01-01T06",
                target_end_source="request",
            ),
        )

    monkeypatch.setattr(services, "run_sync", fake_run_sync)

    services.sync_dataset(dataset_id=dataset_id, end="2024-01-01T06", prefer_zarr=False, publish=False)

    assert captured["current_end"] is None


def _patch_icechunk_artifact_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    captured: dict[str, object],
) -> None:
    """Patch all inline imports used by _create_icechunk_artifact."""
    import numpy as np
    import xarray as xr

    import climate_api.ingest.orchestrator as orchestrator_mod
    import climate_api.ingest.store as store_mod
    from climate_api.ingestions import services as svc

    def fake_run_ingest_sync(**kwargs: object) -> None:
        captured.update(kwargs)
        Path(str(kwargs["store_path"])).mkdir(exist_ok=True)

    monkeypatch.setattr(orchestrator_mod, "run_ingest_sync", fake_run_ingest_sync)
    monkeypatch.setattr(orchestrator_mod, "load_plugin", lambda path, params, extra_params=None: object())
    monkeypatch.setattr(store_mod, "open_or_create_repo", lambda _: _FakeRepo())
    monkeypatch.setattr(
        svc,
        "coverage_from_open_dataset",
        lambda ds, **_: {
            "has_data": True,
            "coverage": {
                "temporal": {"start": "2024-01-01T00", "end": "2024-01-01T06"},
                "spatial": {"xmin": 4.0, "ymin": 57.5, "xmax": 31.5, "ymax": 71.5},
            },
        },
    )
    monkeypatch.setattr(
        xr,
        "open_zarr",
        lambda *_a, **_k: xr.Dataset({"t2m": xr.DataArray(np.zeros((1,)), dims=["time"])}),
    )
    monkeypatch.setattr(svc, "get_extent", lambda: None)
    monkeypatch.setattr(svc.downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(svc, "_store_artifact_record", lambda record, **_: record)
    monkeypatch.setattr(svc, "_upsert_icechunk_artifact_record", lambda record, **_: record)


class _FakeRepo:
    def readonly_session(self, _: str) -> "_FakeSession":
        return _FakeSession()


class _FakeSession:
    store = None


def test_create_icechunk_artifact_uses_ingest_start_for_delta_efficiency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """_create_icechunk_artifact passes ingest_start to run_ingest_sync to skip prior periods."""
    from climate_api.ingestions import services as svc

    captured: dict[str, object] = {}
    _patch_icechunk_artifact_dependencies(monkeypatch, tmp_path, captured)

    dataset = {
        "id": "era5land_temperature_hourly",
        "name": "2m temperature (ERA5-Land)",
        "variable": "t2m",
        "period_type": "hourly",
        "ingestion": {"plugin": "climate_api.ingest.plugins.era5_land.Era5LandPlugin", "params": {"variable": "t2m"}},
    }

    svc._create_icechunk_artifact(
        dataset=dataset,  # type: ignore[arg-type]
        start="2024-01-01T00",
        end="2024-01-01T06",
        bbox=None,
        request_scope=ArtifactRequestScope(start="2024-01-01T00", end="2024-01-01T06", bbox=None),
        publish=False,
        ingest_start="2024-01-01T04",
    )

    assert captured["start"] == "2024-01-01T04"


def test_create_icechunk_artifact_uses_full_start_when_no_ingest_start(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Without ingest_start, run_ingest_sync receives the artifact's full start."""
    from climate_api.ingestions import services as svc

    captured: dict[str, object] = {}
    _patch_icechunk_artifact_dependencies(monkeypatch, tmp_path, captured)

    dataset = {
        "id": "era5land_temperature_hourly",
        "name": "2m temperature (ERA5-Land)",
        "variable": "t2m",
        "period_type": "hourly",
        "ingestion": {"plugin": "climate_api.ingest.plugins.era5_land.Era5LandPlugin", "params": {"variable": "t2m"}},
    }

    svc._create_icechunk_artifact(
        dataset=dataset,  # type: ignore[arg-type]
        start="2024-01-01T00",
        end="2024-01-01T06",
        bbox=None,
        request_scope=ArtifactRequestScope(start="2024-01-01T00", end="2024-01-01T06", bbox=None),
        publish=False,
    )

    assert captured["start"] == "2024-01-01T00"
