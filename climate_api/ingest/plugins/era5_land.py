"""ERA5-Land IngestionPlugin — streams hourly data from DestinE Earth Data Hub.

Authentication via .netrc (Unix) or _netrc (Windows). Register a free account
at https://earthdatahub.destine.eu/getting-started to obtain credentials.

The DestinE ERA5-Land zarr store uses 0–360 longitudes (not −180–180).
This plugin corrects the longitude range before returning data so all stored
periods share a consistent coordinate system.
"""

from __future__ import annotations

import asyncio
import calendar
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any

import numpy as np
import xarray as xr

from climate_api.ingest.protocol import GridSpec

logger = logging.getLogger(__name__)

_DESTINE_ZARR_URL = "https://data.earthdatahub.destine.eu/era5/reanalysis-era5-land-no-antartica-v0.zarr"
_STORAGE_OPTIONS = {"client_kwargs": {"trust_env": True}}

# ERA5-Land on DestinE has roughly a 15-day publication lag.
_LAG_DAYS = 15

# Thread pool shared across probe/fetch calls so async methods don't block the
# event loop while waiting for remote I/O.
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="era5land")


class Era5LandPlugin:
    """IngestionPlugin for ERA5-Land hourly data from DestinE Earth Data Hub.

    Args:
        variable: ERA5-Land variable short name (e.g. 't2m', 'tp').
    """

    max_concurrency = 4
    commit_batch_size = 720  # one month of hourly periods

    def __init__(self, variable: str) -> None:
        self.variable = variable

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Open the remote zarr metadata-only and return the grid spec for bbox."""
        return await asyncio.get_running_loop().run_in_executor(_executor, self._probe_sync, bbox)

    async def periods(self, start: str, end: str) -> list[str]:
        """Return hourly period IDs available within the provider's lag window."""
        return self._build_periods(start, end)

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one hourly period from the remote zarr store."""
        return await asyncio.get_running_loop().run_in_executor(
            _executor, self._fetch_sync, period_id, bbox
        )

    # ------------------------------------------------------------------
    # Sync helpers (run inside the thread pool)
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
        lon_res = float(abs(ds.longitude.diff("longitude").median()))
        lat_res = float(abs(ds.latitude.diff("latitude").median()))
        return ds.sel(
            longitude=slice(xmin - lon_res, xmax + lon_res),
            latitude=slice(ymax + lat_res, ymin - lat_res),
        )

    def _probe_sync(self, bbox: list[float]) -> GridSpec:
        ds = self._open_remote()
        ds = self._correct_longitude(ds)
        ds = self._select_bbox(ds, bbox)
        da = ds[self.variable]
        ny = da.sizes["latitude"]
        nx = da.sizes["longitude"]
        return GridSpec(
            shape=(ny, nx),
            crs=4326,
            dtype=np.dtype(da.dtype),
            nodata=None,
            time_dim=True,
            x_dim="longitude",
            y_dim="latitude",
        )

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        """Fetch one hourly period: remote zarr → bbox clip → load → return."""
        hour = int(period_id[-2:]) if len(period_id) > 10 else 0
        date_part = period_id[:10]

        ds = self._open_remote()
        ds = self._correct_longitude(ds)
        ds = self._select_bbox(ds, bbox)
        ds = ds.sel(valid_time=f"{date_part}T{hour:02d}")

        # Ensure a length-1 time dimension so append_dim="time" works correctly.
        if "valid_time" in ds.dims:
            ds = ds.rename({"valid_time": "time"})
        elif "valid_time" in ds.coords and "time" not in ds.dims:
            ds = ds.expand_dims("time").assign_coords(time=[ds.valid_time.values])

        ds = ds.rename({"longitude": "x", "latitude": "y"})
        ds = ds.load()
        # Strip zarr v2 codec encoding (Blosc) so the orchestrator writes
        # with zarr v3-compatible defaults into the icechunk store.
        for name in list(ds.data_vars) + list(ds.coords):
            ds[name].encoding.clear()
        return ds

    # ------------------------------------------------------------------
    # Period generation
    # ------------------------------------------------------------------

    def _build_periods(self, start: str, end: str) -> list[str]:
        """Generate hourly period IDs, clamped to the provider's availability lag.

        start and end are period-ID strings of the form 'YYYY-MM-DDTHH'.
        The comparison is lexicographic so the filter respects the hour component.
        """
        cutoff = date.today() - timedelta(days=_LAG_DAYS)
        start_dt = date.fromisoformat(start[:10])
        end_dt = min(date.fromisoformat(end[:10]), cutoff)
        # Cutoff clamped to end-of-day of the cutoff date so we filter later.
        cutoff_period = f"{cutoff.isoformat()}T23"

        periods: list[str] = []
        current = start_dt
        while current <= end_dt:
            _, last_day = calendar.monthrange(current.year, current.month)
            for day_num in range(1, last_day + 1):
                d = current.replace(day=day_num)
                if d < start_dt or d > end_dt:
                    continue
                for hour in range(24):
                    p = f"{d.isoformat()}T{hour:02d}"
                    if p < start or p > end or p > cutoff_period:
                        continue
                    periods.append(p)
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        return periods
