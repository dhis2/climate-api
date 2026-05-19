"""Unit tests for WorldPop and CHIRPS3 IngestionPlugins.

All tests exercise the pure-Python logic (period generation, URL construction,
probe estimation) without making network calls. fetch_period tests use
monkeypatching to replace the network/rioxarray layer with a minimal stub.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_api.ingest.protocol import GridSpec, IngestionPlugin

# ---------------------------------------------------------------------------
# WorldPopPlugin
# ---------------------------------------------------------------------------


class TestWorldPopPlugin:
    def _make_plugin(self, country_code: str = "NOR", version: str = "global2") -> Any:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        return WorldPopPlugin(country_code=country_code, version=version)

    # Construction

    def test_country_code_uppercased(self) -> None:
        plugin = self._make_plugin(country_code="nor")
        assert plugin.country_code == "NOR"

    def test_satisfies_protocol(self) -> None:
        plugin = self._make_plugin()
        assert isinstance(plugin, IngestionPlugin)

    def test_max_concurrency_is_conservative(self) -> None:
        plugin = self._make_plugin()
        assert plugin.max_concurrency == 1

    def test_commit_batch_size_is_one(self) -> None:
        plugin = self._make_plugin()
        assert plugin.commit_batch_size == 1

    # URL construction

    def test_url_global2_structure(self) -> None:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        plugin = WorldPopPlugin(country_code="NOR", version="global2")
        url = plugin._url_for_year(2024)
        assert "Global_2015_2030" in url
        assert "/NOR/" in url
        assert "nor_pop_2024" in url
        assert url.endswith(".tif")

    def test_url_global1_structure(self) -> None:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        plugin = WorldPopPlugin(country_code="GHA", version="global1")
        url = plugin._url_for_year(2015)
        assert "Global_2000_2020" in url
        assert "/GHA/" in url
        assert "gha_ppp_2015" in url
        assert url.endswith(".tif")

    def test_url_unknown_version_raises(self) -> None:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        plugin = WorldPopPlugin(country_code="NOR", version="badversion")
        with pytest.raises(ValueError, match="Unknown WorldPop version"):
            plugin._url_for_year(2020)

    # Period generation

    def test_build_periods_global2_basic(self) -> None:
        plugin = self._make_plugin(version="global2")
        periods = plugin._build_periods("2018", "2020")
        assert periods == ["2018", "2019", "2020"]

    def test_build_periods_single_year(self) -> None:
        plugin = self._make_plugin(version="global2")
        assert plugin._build_periods("2023", "2023") == ["2023"]

    def test_build_periods_clamps_to_global2_range(self) -> None:
        plugin = self._make_plugin(version="global2")
        periods = plugin._build_periods("2010", "2035")
        assert periods[0] == "2015"
        assert periods[-1] == "2030"

    def test_build_periods_clamps_to_global1_range(self) -> None:
        plugin = self._make_plugin(version="global1")
        periods = plugin._build_periods("1995", "2025")
        assert periods[0] == "2000"
        assert periods[-1] == "2020"

    def test_build_periods_empty_when_out_of_range(self) -> None:
        plugin = self._make_plugin(version="global2")
        assert plugin._build_periods("2031", "2035") == []

    def test_build_periods_uses_year_prefix_only(self) -> None:
        # period strings like "2024-01-01" should be handled by stripping to year
        plugin = self._make_plugin(version="global2")
        periods = plugin._build_periods("2024-01-01", "2025-12-31")
        assert periods == ["2024", "2025"]

    # probe / GridSpec

    def test_probe_estimate_returns_gridspec(self) -> None:
        plugin = self._make_plugin()
        spec = plugin._probe_estimate([4.0, 57.5, 31.5, 71.5])
        assert isinstance(spec, GridSpec)
        assert spec.crs == 4326
        assert spec.time_dim is True
        assert spec.dtype == np.dtype("float32")
        assert spec.nodata == 0.0
        assert spec.shape[0] > 0 and spec.shape[1] > 0

    def test_probe_estimate_shape_proportional_to_bbox(self) -> None:
        plugin = self._make_plugin()
        small = plugin._probe_estimate([0.0, 0.0, 1.0, 1.0])
        large = plugin._probe_estimate([0.0, 0.0, 10.0, 10.0])
        # 10x wider bbox should yield ~10x more columns
        assert large.shape[1] > small.shape[1] * 5

    def test_probe_is_async_and_returns_gridspec(self) -> None:
        plugin = self._make_plugin()

        async def run() -> GridSpec:
            return await plugin.probe([4.0, 57.5, 31.5, 71.5])

        spec = asyncio.run(run())
        assert isinstance(spec, GridSpec)

    # fetch_period (mocked network)

    def _make_fake_da(self, ny: int = 4, nx: int = 5) -> Any:
        """Build a minimal DataArray that mimics what rioxarray returns."""
        data = np.ones((1, ny, nx), dtype="float32")
        y_coords = np.linspace(71.0, 57.5, ny)
        x_coords = np.linspace(4.0, 31.0, nx)
        da = xr.DataArray(
            data,
            dims=["band", "y", "x"],
            coords={"band": [1], "y": y_coords, "x": x_coords},
        )
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
        da = da.rio.write_crs("EPSG:4326")
        return da

    def test_fetch_period_returns_dataset_with_time_and_pop_total(self) -> None:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        fake_da = self._make_fake_da()
        fake_resp = MagicMock()
        fake_resp.raise_for_status = lambda: None
        fake_resp.content = b""

        with patch("requests.get", return_value=fake_resp), patch(
            "rioxarray.open_rasterio", return_value=fake_da
        ):
            ds = WorldPopPlugin(country_code="NOR")._fetch_sync(2024, [4.0, 57.5, 31.5, 71.5])

        assert "pop_total" in ds.data_vars
        assert "time" in ds.dims
        assert ds.sizes["time"] == 1
        time_val = pd.Timestamp(ds["time"].values[0])
        assert time_val.year == 2024

    def test_fetch_period_clears_encoding_except_time(self) -> None:
        from climate_api.ingest.plugins.worldpop import WorldPopPlugin

        fake_da = self._make_fake_da()
        fake_resp = MagicMock()
        fake_resp.raise_for_status = lambda: None
        fake_resp.content = b""

        with patch("requests.get", return_value=fake_resp), patch(
            "rioxarray.open_rasterio", return_value=fake_da
        ):
            ds = WorldPopPlugin(country_code="NOR")._fetch_sync(2024, [4.0, 57.5, 31.5, 71.5])

        assert ds["time"].encoding.get("units") == "days since 1970-01-01"


# ---------------------------------------------------------------------------
# Chirps3Plugin
# ---------------------------------------------------------------------------


class TestChirps3Plugin:
    def _make_plugin(self, stage: str = "final", flavor: str = "rnl") -> Any:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        return Chirps3Plugin(stage=stage, flavor=flavor)

    # Construction

    def test_default_stage_and_flavor(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        plugin = Chirps3Plugin()
        assert plugin.stage == "final"
        assert plugin.flavor == "rnl"

    def test_satisfies_protocol(self) -> None:
        plugin = self._make_plugin()
        assert isinstance(plugin, IngestionPlugin)

    def test_max_concurrency(self) -> None:
        assert self._make_plugin().max_concurrency == 4

    def test_commit_batch_size(self) -> None:
        assert self._make_plugin().commit_batch_size == 30

    def test_rechunk_time_declared(self) -> None:
        assert self._make_plugin().rechunk_time == 30

    def test_invalid_stage_raises(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        with pytest.raises(ValueError, match="stage"):
            Chirps3Plugin(stage="bad")

    def test_invalid_flavor_for_final_raises(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        with pytest.raises(ValueError, match="flavor"):
            Chirps3Plugin(stage="final", flavor="bad")

    def test_invalid_flavor_for_prelim_raises(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        with pytest.raises(ValueError, match="flavor"):
            Chirps3Plugin(stage="prelim", flavor="rnl")

    # URL construction

    def test_url_final_rnl_structure(self) -> None:
        plugin = self._make_plugin(stage="final", flavor="rnl")
        url = plugin._url_for_day(date(2024, 3, 15))
        assert "final/rnl/cogs/2024" in url
        assert "chirps-v3.0.rnl.2024.03.15.cog" in url

    def test_url_final_sat_structure(self) -> None:
        plugin = self._make_plugin(stage="final", flavor="sat")
        url = plugin._url_for_day(date(2024, 1, 1))
        assert "final/sat/cogs/2024" in url
        assert "chirps-v3.0.sat.2024.01.01.cog" in url

    def test_url_prelim_structure(self) -> None:
        plugin = self._make_plugin(stage="prelim", flavor="sat")
        url = plugin._url_for_day(date(2024, 11, 5))
        assert "prelim/sat/2024" in url
        assert "chirps-v3.0.prelim.2024.11.05.tif" in url

    # Period generation

    def test_build_periods_returns_daily_dates(self) -> None:
        plugin = self._make_plugin()
        # Use a fixed cutoff by patching today
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 3, 25)  # day > 20 → cutoff = end of Feb
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-02-01", "2024-03-31")
        # Cutoff: end of February 2024 (29 days — 2024 is leap)
        assert periods[0] == "2024-02-01"
        assert periods[-1] == "2024-02-29"
        assert len(periods) == 29

    def test_build_periods_respects_lag_before_threshold_day(self) -> None:
        plugin = self._make_plugin()
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 3, 10)  # day <= 20 → cutoff = end of Jan
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-01-01", "2024-03-31")
        assert periods[-1] == "2024-01-31"

    def test_build_periods_empty_when_start_after_cutoff(self) -> None:
        plugin = self._make_plugin()
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 3, 25)
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-03-01", "2024-03-31")
        assert periods == []

    def test_build_periods_consecutive(self) -> None:
        plugin = self._make_plugin()
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 4, 25)
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-03-01", "2024-03-05")
        assert periods == ["2024-03-01", "2024-03-02", "2024-03-03", "2024-03-04", "2024-03-05"]

    def test_build_periods_single_day(self) -> None:
        plugin = self._make_plugin()
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 4, 25)
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-03-01", "2024-03-01")
        assert periods == ["2024-03-01"]

    def test_build_periods_spans_months(self) -> None:
        plugin = self._make_plugin()
        with patch("climate_api.ingest.plugins.chirps3.date") as mock_date:
            mock_date.today.return_value = date(2024, 5, 25)
            mock_date.fromisoformat = date.fromisoformat
            mock_date.side_effect = date
            periods = plugin._build_periods("2024-03-30", "2024-04-02")
        assert periods == ["2024-03-30", "2024-03-31", "2024-04-01", "2024-04-02"]

    # probe / GridSpec

    def test_probe_estimate_returns_gridspec(self) -> None:
        plugin = self._make_plugin()
        spec = plugin._probe_estimate([-180.0, -50.0, 180.0, 50.0])
        assert isinstance(spec, GridSpec)
        assert spec.crs == 4326
        assert spec.time_dim is True
        assert spec.dtype == np.dtype("float32")
        assert spec.nodata == -9999.0
        assert spec.shape[0] > 0 and spec.shape[1] > 0

    def test_probe_estimate_shape_matches_chirps3_global_extent(self) -> None:
        plugin = self._make_plugin()
        # CHIRPS3 full extent: 360° × 100° at 0.05° → 7200 × 2000
        spec = plugin._probe_estimate([-180.0, -50.0, 180.0, 50.0])
        assert spec.shape == (2000, 7200)

    def test_probe_is_async_and_returns_gridspec(self) -> None:
        plugin = self._make_plugin()

        async def run() -> GridSpec:
            return await plugin.probe([-180.0, -50.0, 180.0, 50.0])

        spec = asyncio.run(run())
        assert isinstance(spec, GridSpec)

    # fetch_period (mocked network)

    def _make_fake_chirps_da(self, ny: int = 4, nx: int = 5) -> Any:
        data = np.ones((1, ny, nx), dtype="float32") * 5.0
        y_coords = np.linspace(10.0, 5.0, ny)
        x_coords = np.linspace(-5.0, 5.0, nx)
        da = xr.DataArray(
            data,
            dims=["band", "y", "x"],
            coords={"band": [1], "y": y_coords, "x": x_coords},
        )
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
        da = da.rio.write_crs("EPSG:4326")
        return da

    def test_fetch_period_returns_dataset_with_time_and_precip(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        fake_da = self._make_fake_chirps_da()
        with patch("rioxarray.open_rasterio", return_value=fake_da):
            ds = Chirps3Plugin()._fetch_sync("2024-03-15", [-5.0, 5.0, 5.0, 10.0])

        assert "precip" in ds.data_vars
        assert "time" in ds.dims
        assert ds.sizes["time"] == 1
        time_val = pd.Timestamp(ds["time"].values[0])
        assert time_val == pd.Timestamp("2024-03-15")

    def test_fetch_period_masks_nodata_as_nan(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        data = np.array([[[1.0, -9999.0], [3.0, 4.0]]], dtype="float32")
        da = xr.DataArray(data, dims=["band", "y", "x"], coords={"band": [1], "y": [2.0, 1.0], "x": [0.0, 1.0]})
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
        da = da.rio.write_crs("EPSG:4326")

        with patch("rioxarray.open_rasterio", return_value=da):
            ds = Chirps3Plugin()._fetch_sync("2024-01-01", [0.0, 1.0, 1.0, 2.0])

        precip = ds["precip"].values
        assert np.isnan(precip).any(), "nodata pixels should be NaN"
        assert not np.isnan(precip).all(), "non-nodata pixels should be finite"

    def test_fetch_period_time_encoding_pinned(self) -> None:
        from climate_api.ingest.plugins.chirps3 import Chirps3Plugin

        fake_da = self._make_fake_chirps_da()
        with patch("rioxarray.open_rasterio", return_value=fake_da):
            ds = Chirps3Plugin()._fetch_sync("2024-03-15", [-5.0, 5.0, 5.0, 10.0])

        assert ds["time"].encoding.get("units") == "days since 1970-01-01"
