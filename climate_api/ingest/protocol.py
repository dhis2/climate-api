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

    extra_dims: optional non-spatial, non-time dimensions in the store, e.g.
        {"age_group": 20, "sex": 2}. The orchestrator does not use this field;
        it exists for plugin authors who need to document multidimensional
        stores and for future orchestrator extensions.
    """

    shape: tuple[int, int]
    crs: int
    dtype: np.dtype
    nodata: float | None = None
    time_dim: bool = True
    x_dim: str = "x"
    y_dim: str = "y"
    attrs: dict[str, Any] = field(default_factory=dict)
    extra_dims: dict[str, int] = field(default_factory=dict)


@runtime_checkable
class IngestionPlugin(Protocol):
    """Minimal interface a plugin must implement for per-period Icechunk ingest.

    The climate-api layer owns the orchestration loop — plugins never touch
    zarr or Icechunk directly. Implement the three async methods and declare
    max_concurrency and commit_batch_size as class attributes.

    max_concurrency: maximum number of fetch_period calls in flight at once.
        Keep at 1 for sources with large per-period files or rate-limited APIs.
        Raise for sources where individual periods are small (< 50 MB).

    commit_batch_size: cursor checkpoint interval.
        Every period is always committed individually to Icechunk. This
        attribute controls how frequently the orchestrator persists the job
        cursor so that a restart resumes from the last checkpoint rather than
        re-scanning the whole store. Use 1 for monthly sources, ~30 for daily,
        ~720 for hourly.

    rechunk_time (optional class attribute): target time chunk size for the
        post-ingest rechunk. When set, the orchestrator rewrites the store after
        all periods are committed so the time axis uses chunks of this size
        instead of the per-period chunk-of-1. Declare as a class attribute
        (``rechunk_time: int | None = None``) to skip rechunking, or set to a
        positive int (30 for daily, 720 for hourly). This attribute is read via
        ``getattr`` and is intentionally excluded from the Protocol so that
        plugins that omit it still pass the ``isinstance`` check.

    pyramid (optional class attribute): when ``True``, the orchestrator builds
        a multiscale pyramid after ingest completes. Level count is derived
        automatically from the spatial dimensions (same 512-pixel tile target
        and 2048×2048 threshold as the legacy downloader). Set on plugins whose
        data resolution produces tiles too large for efficient browser rendering
        without overviews. Like ``rechunk_time``, read via ``getattr``.
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
