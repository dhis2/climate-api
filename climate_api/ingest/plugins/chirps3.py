"""CHIRPS3 IngestionPlugin — daily precipitation from CHC servers.

Authentication: none required (public COG files on data.chc.ucsb.edu).

Daily COG files are fetched with HTTP range requests so only the bbox
window is downloaded per period. CHIRPS3 "final/rnl" data is released
in complete months; availability lags roughly one to two months behind
today depending on which day of the month it is.

URL layout (final):
  https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/{flavor}/cogs/
  {YYYY}/chirps-v3.0.{flavor}.{YYYY}.{MM}.{DD}.cog

URL layout (prelim):
  https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/
  {YYYY}/chirps-v3.0.prelim.{YYYY}.{MM}.{DD}.tif
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

_CHIRPS3_NODATA = -9999.0
# CHIRPS3 resolution: 0.05° × 0.05° (~5 km at equator)
_CHIRPS3_RES_DEG = 0.05
# After the 20th of a month, the previous month is considered complete
_COMPLETE_AFTER_DAY = 20

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="chirps3")


class Chirps3Plugin:
    """IngestionPlugin for CHIRPS v3 daily precipitation.

    Args:
        stage: Data maturity stage — 'final' (default, stable) or 'prelim'
            (near-real-time, less reliable). Final data lags ~1–2 months.
        flavor: File variant within the stage — 'rnl' or 'sat' for final,
            'sat' for prelim. Defaults to 'rnl' (final/rnl recommended).
    """

    max_concurrency = 4
    commit_batch_size = 30
    rechunk_time = 30

    def __init__(self, stage: str = "final", flavor: str = "rnl") -> None:
        if stage not in {"final", "prelim"}:
            raise ValueError(f"stage must be 'final' or 'prelim', got {stage!r}")
        if stage == "final" and flavor not in {"rnl", "sat"}:
            raise ValueError(f"For stage='final', flavor must be 'rnl' or 'sat', got {flavor!r}")
        if stage == "prelim" and flavor != "sat":
            raise ValueError(f"For stage='prelim', flavor must be 'sat', got {flavor!r}")
        self.stage = stage
        self.flavor = flavor

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Estimate grid spec from known CHIRPS3 resolution — no data transfer."""
        return self._probe_estimate(bbox)

    async def periods(self, start: str, end: str) -> list[str]:
        return self._build_periods(start, end)

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        return await asyncio.get_running_loop().run_in_executor(_executor, self._fetch_sync, period_id, bbox)

    # ------------------------------------------------------------------
    # Sync helpers (run inside the thread pool)
    # ------------------------------------------------------------------

    def _url_for_day(self, d: date) -> str:
        if self.stage == "final":
            return (
                f"https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/"
                f"{self.flavor}/cogs/{d.year}/"
                f"chirps-v3.0.{self.flavor}.{d.year}.{d.month:02d}.{d.day:02d}.cog"
            )
        return (
            f"https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/"
            f"{d.year}/chirps-v3.0.prelim.{d.year}.{d.month:02d}.{d.day:02d}.tif"
        )

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        """Fetch one day via COG range request, clip to bbox, return as Dataset."""
        import rioxarray

        d = date.fromisoformat(period_id)
        url = self._url_for_day(d)
        logger.info("Fetching CHIRPS3 %s: %s", period_id, url)

        da = rioxarray.open_rasterio(url, chunks=None, masked=True, lock=False)
        assert isinstance(da, xr.DataArray)
        xmin, ymin, xmax, ymax = map(float, bbox)
        da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
        da = da.squeeze("band", drop=True)
        # Guard against files where the mask was not applied via metadata
        da = da.where(da != _CHIRPS3_NODATA)
        da = da.load()

        ds = da.to_dataset(name="precip")
        return ds.expand_dims(time=[np.datetime64(period_id, "D")])  # type: ignore[no-any-return]

    def _probe_estimate(self, bbox: list[float]) -> GridSpec:
        """Derive GridSpec from CHIRPS3's known 0.05° resolution."""
        import math

        xmin, ymin, xmax, ymax = map(float, bbox)
        nx = max(1, math.ceil((xmax - xmin) / _CHIRPS3_RES_DEG))
        ny = max(1, math.ceil((ymax - ymin) / _CHIRPS3_RES_DEG))
        return GridSpec(
            shape=(ny, nx),
            crs=4326,
            dtype=np.dtype("float32"),
            nodata=_CHIRPS3_NODATA,
            time_dim=True,
        )

    # ------------------------------------------------------------------
    # Period generation
    # ------------------------------------------------------------------

    def _availability_cutoff(self) -> date:
        """Return the last day of the most recent complete published month."""
        today = date.today()
        months_back = 1 if today.day > _COMPLETE_AFTER_DAY else 2
        y, m = today.year, today.month
        for _ in range(months_back):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
        last_day = calendar.monthrange(y, m)[1]
        return date(y, m, last_day)

    def _build_periods(self, start: str, end: str) -> list[str]:
        """Return daily ISO-date strings from start to end, clamped to availability."""
        cutoff = self._availability_cutoff()
        start_date = date.fromisoformat(start[:10])
        end_date = min(date.fromisoformat(end[:10]), cutoff)
        if start_date > end_date:
            return []
        periods: list[str] = []
        current = start_date
        while current <= end_date:
            periods.append(current.isoformat())
            current += timedelta(days=1)
        return periods
