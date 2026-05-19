"""WorldPop IngestionPlugin — yearly population count from WorldPop Global2.

Authentication: none required (public files on data.worldpop.org).

Files are per-country GeoTIFFs downloaded in full then clipped to bbox.
Global2 (R2025A) covers 2015–2030 at ~100m resolution (3 arc-seconds).
Global1 covers 2000–2020 at the same resolution (UN-adjusted unconstrained).

The country_code constructor parameter must match the ISO 3166-1 alpha-3
code used in WorldPop file names (e.g. 'NOR', 'GHA', 'KEN').
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np
import xarray as xr

from climate_api.ingest.protocol import GridSpec

logger = logging.getLogger(__name__)

# WorldPop Global2 at 100m: 3 arc-seconds = 1/1200 degree per pixel
_WORLDPOP_RES_DEG = 1.0 / 1200

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="worldpop")


class WorldPopPlugin:
    """IngestionPlugin for WorldPop yearly population count data.

    Args:
        country_code: ISO 3166-1 alpha-3 country code (e.g. 'NOR', 'GHA').
            Must match the casing used in WorldPop file paths (stored as
            upper-case for directory names, lower-case for filenames).
        version: Dataset version — 'global2' (2015–2030, default) or
            'global1' (2000–2020).
    """

    max_concurrency = 1
    commit_batch_size = 1

    def __init__(self, country_code: str, version: str = "global2") -> None:
        self.country_code = country_code.upper()
        self.version = version

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Estimate grid spec from known WorldPop resolution — no data transfer."""
        return self._probe_estimate(bbox)

    async def periods(self, start: str, end: str) -> list[str]:
        return self._build_periods(start, end)

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        return await asyncio.get_running_loop().run_in_executor(
            _executor, self._fetch_sync, int(period_id), bbox
        )

    # ------------------------------------------------------------------
    # Sync helpers (run inside the thread pool)
    # ------------------------------------------------------------------

    def _url_for_year(self, year: int) -> str:
        cc = self.country_code
        if self.version == "global2":
            filename = f"{cc.lower()}_pop_{year}_CN_100m_R2025A_v1.tif"
            return (
                f"https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/"
                f"{year}/{cc}/v1/100m/constrained/{filename}"
            )
        if self.version == "global1":
            filename = f"{cc.lower()}_ppp_{year}_UNadj.tif"
            return (
                f"https://data.worldpop.org/GIS/Population/Global_2000_2020/"
                f"{year}/{cc}/{filename}"
            )
        raise ValueError(f"Unknown WorldPop version: {self.version!r}")

    def _fetch_sync(self, year: int, bbox: list[float]) -> xr.Dataset:
        """Download a per-country GeoTIFF, clip to bbox, return as Dataset."""
        import requests
        import rioxarray

        url = self._url_for_year(year)
        logger.info("Fetching WorldPop %s %d: %s", self.country_code, year, url)
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()

        da = rioxarray.open_rasterio(io.BytesIO(resp.content))
        xmin, ymin, xmax, ymax = map(float, bbox)
        da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
        da = da.squeeze("band", drop=True)
        da = da.load()

        ds = da.to_dataset(name="pop_total")
        ds = ds.expand_dims(time=[np.datetime64(f"{year}-01-01", "D")])

        _CF_ENCODING_KEYS = {"scale_factor", "add_offset", "missing_value", "_FillValue", "coordinates"}
        for name in list(ds.data_vars) + list(ds.coords):
            ds[name].encoding.clear()
            ds[name].attrs = {k: v for k, v in ds[name].attrs.items() if k not in _CF_ENCODING_KEYS}
        ds["time"].encoding.update({"units": "days since 1970-01-01", "dtype": "int32"})
        return ds

    def _probe_estimate(self, bbox: list[float]) -> GridSpec:
        """Derive GridSpec from WorldPop's known 3 arc-second resolution."""
        xmin, ymin, xmax, ymax = map(float, bbox)
        nx = max(1, math.ceil((xmax - xmin) / _WORLDPOP_RES_DEG))
        ny = max(1, math.ceil((ymax - ymin) / _WORLDPOP_RES_DEG))
        return GridSpec(
            shape=(ny, nx),
            crs=4326,
            dtype=np.dtype("float32"),
            nodata=0.0,
            time_dim=True,
        )

    # ------------------------------------------------------------------
    # Period generation
    # ------------------------------------------------------------------

    def _build_periods(self, start: str, end: str) -> list[str]:
        """Return year strings in [start, end] clamped to version availability."""
        start_year = int(start[:4])
        end_year = int(end[:4])
        valid_range = (2015, 2030) if self.version == "global2" else (2000, 2020)
        return [
            str(y)
            for y in range(max(start_year, valid_range[0]), min(end_year, valid_range[1]) + 1)
        ]
