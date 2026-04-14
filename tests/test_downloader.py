from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from fastapi import HTTPException
from topozarr.pyramid import Pyramid
from xarray import DataTree

from eo_api.data_accessor.services.accessor import open_zarr_dataset
from eo_api.data_manager.services import downloader


def test_download_dataset_returns_400_when_country_code_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.delenv("COUNTRY_CODE", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "requires a country code" in str(exc_info.value.detail)


def test_download_dataset_returns_400_for_missing_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        bbox: list[float],
    ) -> None:
        del start, end, dirname, prefix, overwrite, bbox

    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.delenv("DOWNLOAD_BBOX", raising=False)
    monkeypatch.delenv("DEFAULT_DOWNLOAD_BBOX", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)
    monkeypatch.setattr(downloader, "_get_default_bbox", _raise_default_bbox_error)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "A bbox is required" in str(exc_info.value.detail)


def test_download_dataset_returns_502_for_upstream_provider_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code
        raise RuntimeError("provider timeout")

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code="SLE",
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 502
    assert "Upstream dataset download failed: provider timeout" == str(exc_info.value.detail)


def _raise_default_bbox_error() -> list[float]:
    raise RuntimeError("missing default bbox")


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


def _write_nc_files(tmp_path: Path) -> list[Path]:
    paths = []
    for year in (2020, 2021):
        ds = xr.Dataset(
            {"pop_total": (["time", "lat", "lon"], np.ones((1, 3, 3), dtype="float32"))},
            coords={
                "time": [pd.Timestamp(f"{year}-01-01")],
                "lat": [10.0, 9.0, 8.0],
                "lon": [30.0, 31.0, 32.0],
            },
        )
        path = tmp_path / f"my_dataset_{year}.nc"
        ds.to_netcdf(path)
        paths.append(path)
    return paths


_FLAT_DATASET: dict[str, Any] = {
    "id": "my_dataset",
    "variable": "pop_total",
    "period_type": "yearly",
    "cache_info": {},
}

_PYRAMID_DATASET: dict[str, Any] = {
    "id": "my_dataset",
    "variable": "pop_total",
    "period_type": "yearly",
    "cache_info": {"multiscales": {"levels": 2, "method": "mean"}},
}


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
    """Root-level time coord (copied for zarr-layer) does not confuse the fallback.

    The fallback triggers on empty data_vars, not empty dims, so a root group
    that only has a 'time' coordinate array still falls back to /0.
    """
    ds = _make_dataset()
    zarr_path = tmp_path / "pyramid.zarr"
    zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    ds.to_zarr(str(zarr_path / "0"), mode="w", zarr_format=3)
    # Simulate what build_dataset_zarr does: copy time to root
    import shutil

    shutil.copytree(str(zarr_path / "0" / "time"), str(zarr_path / "time"))

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
    finally:
        result.close()


# ---------------------------------------------------------------------------
# build_dataset_zarr — flat path
# ---------------------------------------------------------------------------


def test_build_dataset_zarr_flat_creates_zarr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Flat zarr is written with the correct variable and no pyramid level dirs."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)

    downloader.build_dataset_zarr(_FLAT_DATASET)

    zarr_path = tmp_path / "my_dataset.zarr"
    assert zarr_path.exists()
    assert not (zarr_path / "0").exists()

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()


# ---------------------------------------------------------------------------
# build_dataset_zarr — pyramid path
# ---------------------------------------------------------------------------


def _make_fake_pyramid(ds: xr.Dataset, zarr_path: Path) -> Pyramid:
    """Return a Pyramid whose .dt.to_zarr writes a minimal two-level DataTree store."""
    level0 = ds
    level1 = ds.coarsen(lat=2, lon=2, boundary="trim").mean()  # pyright: ignore[reportAttributeAccessIssue]
    dt = DataTree.from_dict({"0": level0, "1": level1})
    return Pyramid(datatree=dt, encoding={})


def test_build_dataset_zarr_pyramid_copies_time_to_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pyramid zarr build copies the time coordinate to the store root for zarr-layer."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)

    def fake_create_pyramid(ds: xr.Dataset, levels: int, x_dim: str, y_dim: str, method: str) -> Pyramid:
        return _make_fake_pyramid(ds, tmp_path / "my_dataset.zarr")

    monkeypatch.setattr(downloader, "create_pyramid", fake_create_pyramid)

    downloader.build_dataset_zarr(_PYRAMID_DATASET)

    zarr_path = tmp_path / "my_dataset.zarr"
    assert (zarr_path / "0").exists(), "pyramid level 0 should exist"
    assert (zarr_path / "time").exists(), "time coordinate must be copied to zarr root"


def test_build_dataset_zarr_pyramid_is_openable_via_level_0(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """open_zarr_dataset returns the dataset from level 0 of the pyramid store."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)

    def fake_create_pyramid(ds: xr.Dataset, levels: int, x_dim: str, y_dim: str, method: str) -> Pyramid:
        return _make_fake_pyramid(ds, tmp_path / "my_dataset.zarr")

    monkeypatch.setattr(downloader, "create_pyramid", fake_create_pyramid)

    downloader.build_dataset_zarr(_PYRAMID_DATASET)

    result = open_zarr_dataset(str(tmp_path / "my_dataset.zarr"))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["time"] == 2
    finally:
        result.close()
