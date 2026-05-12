"""Tests for remote zarr dataset registration and serving."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_api.data_manager.services.utils import get_time_dim
from climate_api.data_registry.services import datasets as registry
from climate_api.ingestions import services
from climate_api.ingestions.schemas import ArtifactFormat, SyncAction, SyncKind
from climate_api.ingestions.sync_engine import plan_sync
from climate_api.providers.remote_zarr import is_remote_source, open_remote_dataset

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_GEFS_STORE: dict[str, Any] = {
    "kind": "remote_zarr",
    "provider": "dynamical",
    "catalog_id": "noaa-gefs-forecast-35-day",
    "store_url": "s3://dynamical-noaa-gefs/noaa-gefs-forecast-35-day/v0.2.0.icechunk/",
    "store_format": "icechunk",
    "storage_options": {"anon": True, "client_kwargs": {"region_name": "us-west-2"}},
}

_GEFS_DATASET: dict[str, Any] = {
    "id": "gefs_precipitation",
    "name": "Total precipitation surface forecast (NOAA GEFS ensemble)",
    "variable": "precipitation_surface",
    "period_type": "daily",
    "sync": {"kind": "remote"},
    "store": _GEFS_STORE,
}


def _make_gefs_ds() -> xr.Dataset:
    """Return a minimal fake GEFS dataset with init_time/latitude/longitude dimensions."""
    return xr.Dataset(
        {"precipitation_surface": (["init_time", "latitude", "longitude"], np.ones((3, 4, 4), dtype="float32"))},
        coords={
            "init_time": pd.date_range("2026-05-01", periods=3, freq="D"),
            "latitude": [10.0, 5.0, 0.0, -5.0],
            "longitude": [30.0, 35.0, 40.0, 45.0],
        },
    )


def _make_gefs_ds_with_lead_time() -> xr.Dataset:
    """Return a fake GEFS dataset that includes a lead_time dimension (35-day forecast)."""
    lead_times = pd.to_timedelta(np.arange(0, 841, 6), unit="h")  # 0–840 h in 6-h steps
    return xr.Dataset(
        {
            "precipitation_surface": (
                ["init_time", "lead_time", "latitude", "longitude"],
                np.ones((3, len(lead_times), 4, 4), dtype="float32"),
            )
        },
        coords={
            "init_time": pd.date_range("2026-05-01", periods=3, freq="D"),
            "lead_time": lead_times,
            "latitude": [10.0, 5.0, 0.0, -5.0],
            "longitude": [30.0, 35.0, 40.0, 45.0],
        },
    )


# ---------------------------------------------------------------------------
# providers.remote_zarr
# ---------------------------------------------------------------------------


def test_is_remote_source_returns_true_for_remote_zarr_template():
    assert is_remote_source(_GEFS_DATASET) is True


def test_is_remote_source_returns_false_for_ingestion_template():
    dataset = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"function": "dhis2eo.data.chc.chirps3.daily.download"},
    }
    assert is_remote_source(dataset) is False


def test_is_remote_source_returns_false_for_missing_source():
    assert is_remote_source({"id": "foo"}) is False


def test_open_remote_dataset_raises_runtime_error_when_icechunk_missing():
    source = {**_GEFS_STORE, "store_format": "icechunk"}
    with patch.dict("sys.modules", {"icechunk": None}):
        with pytest.raises(RuntimeError, match="icechunk"):
            open_remote_dataset(source)


def test_open_remote_dataset_zarr_url_calls_xr_open_zarr():
    source = {"store_url": "s3://bucket/store.zarr", "store_format": "zarr"}
    fake_ds = _make_gefs_ds()
    with patch("xarray.open_zarr", return_value=fake_ds) as mock_open:
        result = open_remote_dataset(source)
        mock_open.assert_called_once_with("s3://bucket/store.zarr", consolidated=None)
        assert result is fake_ds


# ---------------------------------------------------------------------------
# data_manager.services.utils — get_time_dim
# ---------------------------------------------------------------------------


def test_get_time_dim_finds_init_time():
    ds = _make_gefs_ds()
    assert get_time_dim(ds) == "init_time"


# ---------------------------------------------------------------------------
# data_registry.services.datasets — template validation
# ---------------------------------------------------------------------------


def test_registry_loads_gefs_template(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    yaml_content = """
- id: gefs_test
  name: GEFS test
  variable: tp
  period_type: daily
  sync:
    kind: remote
  store:
    kind: remote_zarr
    store_url: "s3://bucket/store.icechunk/"
    store_format: icechunk
"""
    (tmp_path / "gefs_test.yaml").write_text(yaml_content, encoding="utf-8")
    monkeypatch.setattr(registry, "CONFIGS_DIR", tmp_path)
    datasets = registry.list_datasets()
    assert any(d["id"] == "gefs_test" for d in datasets)


def test_registry_rejects_remote_zarr_without_store_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    yaml_content = """
- id: bad_remote
  name: Bad remote
  variable: tp
  period_type: daily
  sync:
    kind: remote
  store:
    kind: remote_zarr
"""
    (tmp_path / "bad.yaml").write_text(yaml_content, encoding="utf-8")
    monkeypatch.setattr(registry, "CONFIGS_DIR", tmp_path)
    with pytest.raises(ValueError, match="store_url"):
        registry.list_datasets()


def test_registry_rejects_ingestion_template_without_function(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    yaml_content = """
- id: bad_ingestion
  name: Bad
  variable: tp
  period_type: daily
  sync:
    kind: temporal
  ingestion: {}
"""
    (tmp_path / "bad.yaml").write_text(yaml_content, encoding="utf-8")
    monkeypatch.setattr(registry, "CONFIGS_DIR", tmp_path)
    with pytest.raises(ValueError, match="ingestion.function"):
        registry.list_datasets()


# ---------------------------------------------------------------------------
# ingestions.services — _register_remote_zarr_artifact
# ---------------------------------------------------------------------------


def test_register_remote_zarr_artifact_creates_remote_zarr_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")

    fake_ds = _make_gefs_ds()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        record = services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)

    assert record.format == ArtifactFormat.REMOTE_ZARR
    assert record.path == _GEFS_STORE["store_url"]
    assert record.dataset_id == "gefs_precipitation"
    assert record.coverage.temporal.start == "2026-05-01"
    assert record.coverage.temporal.end == "2026-05-03"


def test_register_remote_zarr_artifact_extends_end_by_lead_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temporal end must be last init_time + max lead_time, not just last init_time."""
    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")

    fake_ds = _make_gefs_ds_with_lead_time()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        record = services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)

    # last init_time = 2026-05-03, max lead_time = 840 h = 35 days → end = 2026-06-07
    assert record.coverage.temporal.start == "2026-05-01"
    assert record.coverage.temporal.end == "2026-06-07"


def test_register_remote_zarr_artifact_upserts_on_re_register(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")

    fake_ds = _make_gefs_ds()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        first = services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)
        second = services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)

    assert first.artifact_id == second.artifact_id
    records = services._load_records()
    assert sum(r.dataset_id == "gefs_precipitation" for r in records) == 1


def test_create_artifact_routes_to_remote_zarr_for_remote_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")

    fake_ds = _make_gefs_ds()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        record = services.create_artifact(
            dataset=_GEFS_DATASET,
            start="2026-05-01",
            end=None,
            bbox=None,
            country_code=None,
            overwrite=False,
            prefer_zarr=True,
            publish=False,
        )

    assert record.format == ArtifactFormat.REMOTE_ZARR


# ---------------------------------------------------------------------------
# ingestions.services — storage exists + zarr serving
# ---------------------------------------------------------------------------


def test_artifact_storage_exists_returns_true_for_remote_zarr():
    from datetime import UTC, datetime

    from climate_api.ingestions.schemas import (
        ArtifactCoverage,
        ArtifactPublication,
        ArtifactRecord,
        ArtifactRequestScope,
        CoverageSpatial,
        CoverageTemporal,
    )

    record = ArtifactRecord(
        artifact_id="abc",
        dataset_id="gefs_precipitation",
        dataset_name="GEFS",
        variable="precipitation_surface",
        format=ArtifactFormat.REMOTE_ZARR,
        path="s3://bucket/store.icechunk/",
        asset_paths=["s3://bucket/store.icechunk/"],
        variables=["precipitation_surface"],
        request_scope=ArtifactRequestScope(start="2026-05-01", end="2026-05-03"),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-05-01", end="2026-05-03"),
            spatial=CoverageSpatial(xmin=-180.0, ymin=-90.0, xmax=180.0, ymax=90.0),
        ),
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    assert services._artifact_storage_exists(record) is True


def test_zarr_store_info_for_remote_artifact_returns_remote_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")
    monkeypatch.setattr(
        registry, "get_dataset", lambda dataset_id: _GEFS_DATASET if dataset_id == "gefs_precipitation" else None
    )

    fake_ds = _make_gefs_ds()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)

    from climate_api.publications.services import managed_dataset_id_for

    record = services._load_records()[0]
    dataset_id = managed_dataset_id_for(record)

    info = services.get_dataset_zarr_store_info_or_404(dataset_id)

    assert info["format"] == ArtifactFormat.REMOTE_ZARR
    assert info["remote_url"] == _GEFS_STORE["store_url"]
    assert info["provider"] == "dynamical"


def test_zarr_store_file_for_remote_artifact_proxies_from_icechunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
    import json as _json

    monkeypatch.setattr(services, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", tmp_path / "artifacts" / "records.json")
    monkeypatch.setattr(
        registry, "get_dataset", lambda dataset_id: _GEFS_DATASET if dataset_id == "gefs_precipitation" else None
    )

    fake_ds = _make_gefs_ds()
    with patch("climate_api.providers.remote_zarr.open_remote_dataset", return_value=fake_ds):
        services._register_remote_zarr_artifact(dataset=_GEFS_DATASET, publish=False)

    from climate_api.publications.services import managed_dataset_id_for

    record = services._load_records()[0]
    dataset_id = managed_dataset_id_for(record)

    fake_zarr_json = _json.dumps({"zarr_format": 3, "node_type": "group"}).encode()

    class _FakeBuffer:
        def as_array_like(self) -> bytes:
            return fake_zarr_json

    class _FakeStore:
        async def get(self, key: str, prototype: object, byte_range: object = None) -> _FakeBuffer:  # type: ignore[override]
            return _FakeBuffer()

    fake_store = _FakeStore()

    async def _run() -> object:
        from starlette.requests import Request as StarletteRequest

        scope = {"type": "http", "method": "GET", "headers": []}
        request = StarletteRequest(scope)
        with patch("climate_api.providers.remote_zarr.open_icechunk_store", return_value=fake_store):
            return await services.get_dataset_zarr_store_file_or_404(request, dataset_id, "zarr.json")

    response = asyncio.run(_run())

    from fastapi.responses import JSONResponse

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# ingestions.sync_engine — REMOTE sync kind
# ---------------------------------------------------------------------------


def test_plan_sync_remote_always_rematerializes():
    from datetime import UTC, datetime

    from climate_api.ingestions.schemas import (
        ArtifactCoverage,
        ArtifactPublication,
        ArtifactRecord,
        ArtifactRequestScope,
        CoverageSpatial,
        CoverageTemporal,
    )

    artifact = ArtifactRecord(
        artifact_id="abc",
        dataset_id="gefs_precipitation",
        dataset_name="GEFS",
        variable="precipitation_surface",
        format=ArtifactFormat.REMOTE_ZARR,
        path="s3://bucket/store.icechunk/",
        asset_paths=["s3://bucket/store.icechunk/"],
        variables=["precipitation_surface"],
        request_scope=ArtifactRequestScope(start="2026-05-01", end="2026-05-03"),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-05-01", end="2026-05-03"),
            spatial=CoverageSpatial(xmin=-180.0, ymin=-90.0, xmax=180.0, ymax=90.0),
        ),
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    detail = plan_sync(
        source_dataset=_GEFS_DATASET,
        latest_artifact=artifact,
        requested_end=None,
    )
    assert detail.sync_kind == SyncKind.REMOTE
    assert detail.action == SyncAction.REMATERIALIZE
    assert detail.reason == "remote_store_refresh"
