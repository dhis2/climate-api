"""CHIRPS3 IngestionPlugin — daily precipitation from CHC servers.

Authentication: none required (public COG files on data.chc.ucsb.edu).

Daily COG files are fetched with HTTP range requests so only the bbox
window is downloaded per period. CHIRPS3 "final/rnl" data is released in
complete months; the plugin probes the CDN to find the actual latest month.

URL layout (final):
  https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/{flavor}/cogs/
  {YYYY}/chirps-v3.0.{flavor}.{YYYY}.{MM}.{DD}.cog

URL layout (prelim):
  https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/
  {YYYY}/chirps-v3.0.prelim.{YYYY}.{MM}.{DD}.tif
"""

from __future__ import annotations

import calendar
import logging
import time
from datetime import date
from typing import Any

import numpy as np
import xarray as xr

from climate_api.ingest.protocol import GridSpec, enumerate_periods

logger = logging.getLogger(__name__)

_CHIRPS3_NODATA = -9999.0
# CHIRPS3 resolution: 0.05° × 0.05° (~5 km at equator)
_CHIRPS3_RES_DEG = 0.05


class Chirps3Plugin:
    """IngestionPlugin for CHIRPS v3 daily precipitation.

    Args:
        stage: Data maturity stage — 'final' (default, stable) or 'prelim'
            (near-real-time, less reliable). Final data lags ~1–2 months.
        flavor: File variant within the stage — 'rnl' or 'sat' for final,
            'sat' for prelim. Defaults to 'rnl' (final/rnl recommended).
    """

    max_concurrency = 1
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

    def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Derive GridSpec from CHIRPS3's known 0.05° resolution — no data transfer."""
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

    def periods(self, start: str, end: str) -> list[str]:
        return enumerate_periods(start, end, "daily", cutoff=self._availability_cutoff())

    def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one day via COG range request, clip to bbox, return as Dataset."""
        import rioxarray

        d = date.fromisoformat(period_id)
        url = self._url_for_day(d)
        logger.info("Fetching CHIRPS3 %s: %s", period_id, url)

        da = None
        for attempt in range(3):
            try:
                da = rioxarray.open_rasterio(url, chunks=None, masked=True, lock=False)
                break
            except Exception as exc:
                msg = str(exc)
                if "429" in msg or "503" in msg:
                    wait = 10 * (2**attempt)
                    logger.warning("CHIRPS3 HTTP error (%s), retrying in %ds: %s", msg[:60], wait, url)
                    time.sleep(wait)
                    if attempt == 2:
                        raise
                else:
                    raise
        if not isinstance(da, xr.DataArray):
            raise TypeError(f"rioxarray.open_rasterio returned {type(da).__name__!r}, expected DataArray")
        xmin, ymin, xmax, ymax = map(float, bbox)
        da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
        da = da.squeeze("band", drop=True)
        # Guard against files where the mask was not applied via metadata
        da = da.where(da != _CHIRPS3_NODATA)
        da = da.load()

        ds = da.to_dataset(name="precip")
        return ds.expand_dims(time=[np.datetime64(period_id, "D")])  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # URL construction and availability
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

    def _availability_cutoff(self) -> date:
        """Return the last day of the most recently published complete month.

        Scans backward from the most recent months with a HEAD request on the
        last-day COG URL so the cutoff reflects the actual CDN state rather than
        a hardcoded lag assumption.
        """
        import requests

        today = date.today()
        y, m = today.year, today.month
        for _ in range(6):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
            last_day = calendar.monthrange(y, m)[1]
            candidate = date(y, m, last_day)
            try:
                resp = requests.head(self._url_for_day(candidate), timeout=10, allow_redirects=True)
                if resp.status_code == 200:
                    return candidate
            except Exception:
                continue
        # Fallback: 3 months back (safe)
        y, m = today.year, today.month
        for _ in range(3):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
        return date(y, m, calendar.monthrange(y, m)[1])
