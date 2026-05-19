"""Plugin protocol and shared data types for per-period Icechunk ingest."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import numpy as np

if TYPE_CHECKING:
    import xarray as xr


@dataclass
class GridSpec:
    """Source grid metadata returned by a plugin probe.

    The orchestrator uses this to fix the zarr chunk shape and write GeoZarr
    attributes before the first period is written. Set time_dim=False for
    static (time-invariant) datasets — the orchestrator branches on this flag
    and issues a single write with no append dimension.
    """

    shape: tuple[int, int]
    crs: int
    dtype: np.dtype
    nodata: float | None = None
    time_dim: bool = True
    x_dim: str = "x"
    y_dim: str = "y"
    attrs: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class IngestionPlugin(Protocol):
    """Minimal interface a plugin must implement for per-period Icechunk ingest.

    The climate-api layer owns the orchestration loop — plugins never touch
    zarr or Icechunk directly. Implement the three async methods and declare
    max_concurrency and commit_batch_size as class attributes.

    max_concurrency: maximum number of fetch_period calls in flight at once.
        Keep at 1 for sources with large per-period files or rate-limited APIs.
        Raise for sources where individual periods are small (< 50 MB).

    commit_batch_size: number of periods written between Icechunk commits.
        Use 1 for monthly sources. For daily sources use ~30; for hourly ~720.
        This controls crash-recovery granularity, not peak memory — to_zarr
        flushes each period immediately.
    """

    max_concurrency: int
    commit_batch_size: int

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        """Metadata-only source probe. Returns grid spec. No data transfer."""
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        """Return the ordered list of available period IDs from start to end.

        May query the upstream source to confirm which periods are published.
        The orchestrator uses the length of this list for progress reporting.
        """
        ...

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> "xr.Dataset":
        """Fetch one period. Return a dataset in the source CRS.

        The returned dataset must have a 'time' dimension with a single
        coordinate value. Spatial dimensions must match spec.x_dim / spec.y_dim.
        The orchestrator handles zarr writes — never call to_zarr here.
        """
        ...
