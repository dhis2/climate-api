import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from climate_api.data_accessor.services.accessor import _coverage_from_dataset, open_zarr_dataset
from climate_api.data_manager.services import downloader
from climate_api.ingestions import services as ingestion_services


def test_resolve_download_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text("data_dir: ./data\nextent:\n  id: test\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert downloader._resolve_download_dir() == tmp_path / "data" / "downloads"


def test_resolve_download_dir_uses_xdg_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        assert downloader._resolve_download_dir() == Path(xdg) / "climate-api" / "downloads"


def test_resolve_artifacts_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-api.yaml"
    config_file.write_text("data_dir: ./data\nextent:\n  id: test\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_API_CONFIG", str(config_file))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert ingestion_services._resolve_artifacts_dir() == tmp_path / "data" / "artifacts"


def test_resolve_artifacts_dir_uses_xdg_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.delenv("CLIMATE_API_CONFIG", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        assert ingestion_services._resolve_artifacts_dir() == Path(xdg) / "climate-api" / "artifacts"


# ---------------------------------------------------------------------------
# _get_cache_prefix
# ---------------------------------------------------------------------------


def test_get_cache_prefix_uses_dataset_id() -> None:
    dataset: dict[str, Any] = {"id": "chirps3_precipitation_daily", "ingestion": {}}
    assert downloader._get_cache_prefix(dataset) == "chirps3_precipitation_daily"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset() -> xr.Dataset:
    return xr.Dataset(
        {"pop_total": (["time", "lat", "lon"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "time": pd.date_range("2020-01-01", periods=2, freq="YS"),
            "lat": [10.0, 9.0, 8.0],
            "lon": [30.0, 31.0, 32.0],
        },
    )


# ---------------------------------------------------------------------------
# open_zarr_dataset
# ---------------------------------------------------------------------------


def test_open_zarr_dataset_flat(tmp_path: Path) -> None:
    """Flat zarr store is opened directly and exposes its data variables."""
    ds = _make_dataset()
    zarr_path = tmp_path / "flat.zarr"
    ds.to_zarr(str(zarr_path), mode="w")

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_falls_back_to_level_0(tmp_path: Path) -> None:
    """Pyramid zarr with no data vars at root falls back to opening /0."""
    zarr_path = tmp_path / "pyramid.zarr"
    zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    _make_dataset().to_zarr(str(zarr_path / "0"), mode="w", zarr_format=3)

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_with_root_time_still_opens_level_0(tmp_path: Path) -> None:
    """Root-level time coord does not confuse the fallback.

    The fallback triggers on empty data_vars, not empty dims, so a root group
    that only has a 'time' coordinate array still falls back to /0.
    """
    import shutil

    ds = _make_dataset()
    zarr_path = tmp_path / "pyramid.zarr"
    zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    ds.to_zarr(str(zarr_path / "0"), mode="w", zarr_format=3)
    shutil.copytree(str(zarr_path / "0" / "time"), str(zarr_path / "time"))

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
    finally:
        result.close()


# ---------------------------------------------------------------------------
# _coverage_from_dataset — WGS84 reprojection
# ---------------------------------------------------------------------------


def test_coverage_from_dataset_populates_spatial_wgs84_for_projected_crs() -> None:
    # Small UTM33N (EPSG:25833) bounding box covering south-central Norway.
    x = np.array([100_000.0, 200_000.0])  # easting metres
    y = np.array([6_500_000.0, 6_600_000.0])  # northing metres
    times = pd.date_range("2020-01-01", periods=1, freq="D")
    data = np.ones((1, len(y), len(x)))
    ds = xr.Dataset(
        {"temperature": (["time", "y", "x"], data)},
        coords={"time": times, "y": y, "x": x},
    )

    result = _coverage_from_dataset(ds=ds, period_type="daily", native_crs="EPSG:25833")

    wgs84 = result["coverage"]["spatial_wgs84"]
    assert wgs84 is not None
    # Reprojected bounds must be in WGS84 degree range.
    assert -180 <= wgs84["xmin"] <= 180
    assert -180 <= wgs84["xmax"] <= 180
    assert -90 <= wgs84["ymin"] <= 90
    assert -90 <= wgs84["ymax"] <= 90
    # Rough sanity check: UTM33N easting ~100–200 km, northing ~6500–6600 km.
    assert 7.0 < wgs84["xmin"] < 10.0
    assert 8.0 < wgs84["xmax"] < 12.0
    assert 58.0 < wgs84["ymin"] < 62.0
    assert 58.0 < wgs84["ymax"] < 62.0


def test_coverage_from_dataset_leaves_spatial_wgs84_none_for_wgs84() -> None:
    x = np.array([-10.0, -9.0])
    y = np.array([7.0, 8.0])
    times = pd.date_range("2020-01-01", periods=1, freq="D")
    data = np.ones((1, len(y), len(x)))
    ds = xr.Dataset(
        {"temperature": (["time", "y", "x"], data)},
        coords={"time": times, "y": y, "x": x},
    )

    result = _coverage_from_dataset(ds=ds, period_type="daily", native_crs="EPSG:4326")

    assert result["coverage"]["spatial_wgs84"] is None
