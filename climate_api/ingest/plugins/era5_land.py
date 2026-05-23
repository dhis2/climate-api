"""ERA5-Land IngestionPlugin — streams hourly data from DestinE Earth Data Hub.

Authentication via .netrc (Unix) or _netrc (Windows). Register a free account
at https://earthdatahub.destine.eu/getting-started to obtain credentials.

The DestinE ERA5-Land zarr store uses 0–360 longitudes (not −180–180).
This plugin corrects the longitude range before returning data so all stored
periods share a consistent coordinate system.
"""

from __future__ import annotations

import logging
import math
import threading
from datetime import date
from typing import Any

import numpy as np
import xarray as xr

from climate_api.ingest.protocol import GridSpec, enumerate_periods

logger = logging.getLogger(__name__)

_DESTINE_ZARR_URL = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-land-no-antartica-v0.zarr"
_STORAGE_OPTIONS = {"client_kwargs": {"trust_env": True}}

# ERA5-Land native resolution: 0.1° × 0.1° (~9 km at equator).
_ERA5_LAND_RES_DEG = 0.1


class Era5LandPlugin:
    """IngestionPlugin for ERA5-Land hourly data from DestinE Earth Data Hub.

    Args:
        variable: ERA5-Land variable short name (e.g. 't2m', 'tp').
    """

    max_concurrency = 4
    commit_batch_size = 720  # one month of hourly periods
    rechunk_time = 12  # group 12 hourly periods per chunk after initial ingest

    def __init__(self, variable: str) -> None:
        self.variable = variable
        self._cache_ds: xr.Dataset | None = None
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Derive GridSpec from ERA5-Land's known 0.1° resolution — no data transfer."""
        xmin, ymin, xmax, ymax = map(float, bbox)
        # _select_bbox pads by one pixel in each direction so probe matches fetch shape.
        nx = max(1, math.ceil((xmax - xmin + 2 * _ERA5_LAND_RES_DEG) / _ERA5_LAND_RES_DEG))
        ny = max(1, math.ceil((ymax - ymin + 2 * _ERA5_LAND_RES_DEG) / _ERA5_LAND_RES_DEG))
        return GridSpec(
            shape=(ny, nx),
            crs=4326,
            dtype=np.dtype("float32"),
            nodata=None,
            time_dim=True,
            x_dim="x",
            y_dim="y",
        )

    def periods(self, start: str, end: str) -> list[str]:
        """Return hourly period IDs up to the last timestamp published in the remote store."""
        latest = self._latest_available()
        return enumerate_periods(start, min(end, latest), "hourly")

    def _latest_available(self) -> str:
        """Read the last valid_time from the remote Zarr store (metadata only, no data loaded)."""
        ds = xr.open_dataset(
            _DESTINE_ZARR_URL,
            engine="zarr",
            storage_options=_STORAGE_OPTIONS,
            chunks={},
        )
        return str(np.datetime64(ds.valid_time.values[-1], "h"))

    def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one hourly period from the remote zarr store."""
        hour = int(period_id[-2:]) if len(period_id) > 10 else 0
        date_part = period_id[:10]

        if self._cache_ds is None:
            with self._cache_lock:
                if self._cache_ds is None:
                    logger.info("Opening ERA5-Land remote store: %s", _DESTINE_ZARR_URL)
                    self._cache_ds = self._correct_longitude(self._open_remote())
        ds = self._cache_ds
        ds = self._select_bbox(ds, bbox).sel(valid_time=f"{date_part}T{hour:02d}")

        # Ensure a length-1 time dimension so append_dim="time" works correctly.
        if "valid_time" in ds.dims:
            ds = ds.rename({"valid_time": "time"})
        elif "valid_time" in ds.coords and "time" not in ds.dims:
            ds = ds.expand_dims("time").assign_coords(time=[ds.valid_time.values])

        ds = ds.rename({"longitude": "x", "latitude": "y"})
        ds = ds.load()
        return ds

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _open_remote(self) -> xr.Dataset:
        return xr.open_dataset(
            _DESTINE_ZARR_URL,
            engine="zarr",
            storage_options=_STORAGE_OPTIONS,
            chunks={},
        )[[self.variable]]

    def _correct_longitude(self, ds: xr.Dataset) -> xr.Dataset:
        """Unwrap 0–360 longitude to −180–180 and sort."""
        return ds.assign_coords(longitude=((ds.longitude + 180) % 360 - 180)).sortby("longitude")

    def _select_bbox(self, ds: xr.Dataset, bbox: list[float]) -> xr.Dataset:
        xmin, ymin, xmax, ymax = map(float, bbox)
        pad = _ERA5_LAND_RES_DEG
        return ds.sel(
            longitude=slice(xmin - pad, xmax + pad),
            latitude=slice(ymax + pad, ymin - pad),
        )
