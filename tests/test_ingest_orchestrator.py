"""Tests for the per-period Icechunk ingest orchestrator.

All tests use FakePlugin — no network access required.
The Icechunk store is written to a pytest tmp_path directory.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from climate_api.ingest.orchestrator import load_plugin, run_ingest, run_ingest_sync
from climate_api.ingest.protocol import GridSpec, IngestionPlugin
from climate_api.ingest.store import read_committed_period_ids

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_monthly_dataset(period_id: str, ny: int = 4, nx: int = 4) -> xr.Dataset:
    """Return a tiny single-period dataset matching FakePlugin's grid."""
    t = pd.Timestamp(f"{period_id}-01")
    data = np.random.default_rng(42).random((1, ny, nx)).astype("float32")
    return xr.Dataset(
        {"temperature": xr.DataArray(data, dims=["time", "y", "x"])},
        coords={"time": [t]},
    )


# ---------------------------------------------------------------------------
# Fake plugin
# ---------------------------------------------------------------------------


class FakePlugin:
    """In-memory IngestionPlugin that generates tiny xarray Datasets."""

    max_concurrency = 2
    commit_batch_size = 2
    rechunk_time: int | None = None

    def __init__(self, periods: list[str]) -> None:
        self._periods = periods
        self.fetched: list[str] = []

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        return GridSpec(shape=(4, 4), crs=4326, dtype=np.dtype("float32"), nodata=None)

    async def periods(self, start: str, end: str) -> list[str]:
        return [p for p in self._periods if start <= p <= end]

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        self.fetched.append(period_id)
        return _make_monthly_dataset(period_id)


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_fake_plugin_satisfies_protocol() -> None:
    plugin = FakePlugin(["2024-01", "2024-02"])
    assert isinstance(plugin, IngestionPlugin)


# ---------------------------------------------------------------------------
# Core orchestrator tests
# ---------------------------------------------------------------------------


def test_run_ingest_writes_all_periods(tmp_path: Path) -> None:
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert store_path.exists()
    committed = read_committed_period_ids(store_path, "monthly")
    assert committed == {"2024-01", "2024-02", "2024-03"}


def test_run_ingest_fetches_every_period_exactly_once(tmp_path: Path) -> None:
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert sorted(plugin.fetched) == ["2024-01", "2024-02", "2024-03"]


def test_run_ingest_is_idempotent(tmp_path: Path) -> None:
    plugin = FakePlugin(["2024-01", "2024-02"])
    store_path = tmp_path / "test.icechunk"

    for _ in range(2):
        asyncio.run(
            run_ingest(
                plugin=plugin,
                params={},
                bbox=[-180, -90, 180, 90],
                start="2024-01",
                end="2024-02",
                store_path=store_path,
                period_type="monthly",
            )
        )

    # Second run fetched nothing new.
    assert plugin.fetched == ["2024-01", "2024-02"]
    committed = read_committed_period_ids(store_path, "monthly")
    assert committed == {"2024-01", "2024-02"}


def test_run_ingest_resumes_from_cursor(tmp_path: Path) -> None:
    """Simulate a crash after the first batch (2024-01, 2024-02) and resume."""
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04"])
    store_path = tmp_path / "test.icechunk"

    # First run writes all four periods but we stop with a cursor pointing at batch 1.
    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-02",
            store_path=store_path,
            period_type="monthly",
        )
    )
    assert read_committed_period_ids(store_path, "monthly") == {"2024-01", "2024-02"}

    # Resume: provide a cursor pointing at the last committed period.
    cursor: dict[str, Any] = {"last_committed": "2024-02"}
    plugin2 = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04"])

    asyncio.run(
        run_ingest(
            plugin=plugin2,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-04",
            store_path=store_path,
            period_type="monthly",
            load_cursor=lambda: cursor,
        )
    )

    # Only the two new periods were fetched.
    assert sorted(plugin2.fetched) == ["2024-03", "2024-04"]
    committed = read_committed_period_ids(store_path, "monthly")
    assert committed == {"2024-01", "2024-02", "2024-03", "2024-04"}


def test_run_ingest_progress_reporting(tmp_path: Path) -> None:
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "test.icechunk"
    reports: list[dict[str, Any]] = []

    def on_progress(done: int, total: int, message: str) -> None:
        reports.append({"done": done, "total": total})

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
            on_progress=on_progress,
        )
    )

    totals = {r["total"] for r in reports}
    assert totals == {3}
    final = max(r["done"] for r in reports)
    assert final == 3


def test_run_ingest_cursor_saved_after_each_batch(tmp_path: Path) -> None:
    """commit_batch_size=2 → cursor is saved after periods 2 and 4."""
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04"])
    store_path = tmp_path / "test.icechunk"
    saved_cursors: list[dict[str, Any]] = []

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-04",
            store_path=store_path,
            period_type="monthly",
            save_cursor=saved_cursors.append,
        )
    )

    assert len(saved_cursors) == 2
    assert saved_cursors[0]["last_committed"] == "2024-02"
    assert saved_cursors[1]["last_committed"] == "2024-04"


def test_run_ingest_noop_when_no_periods(tmp_path: Path) -> None:
    plugin = FakePlugin([])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert not store_path.exists()


def test_run_ingest_cancels_on_request(tmp_path: Path) -> None:
    from climate_api.jobs.models import JobCancelledError

    # Plugin with more periods than the batch — cancellation hits mid-run.
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"])
    store_path = tmp_path / "test.icechunk"

    call_count = 0

    def cancel_after_two() -> bool:
        nonlocal call_count
        call_count += 1
        # First check (before period 0) → False; subsequent checks → True after first batch.
        return call_count > 1

    with pytest.raises(JobCancelledError):
        asyncio.run(
            run_ingest(
                plugin=plugin,
                params={},
                bbox=[-180, -90, 180, 90],
                start="2024-01",
                end="2024-06",
                store_path=store_path,
                period_type="monthly",
                is_cancel_requested=cancel_after_two,
            )
        )


# ---------------------------------------------------------------------------
# Sync wrapper
# ---------------------------------------------------------------------------


def test_run_ingest_sync_wrapper(tmp_path: Path) -> None:
    plugin = FakePlugin(["2024-01", "2024-02"])
    store_path = tmp_path / "test.icechunk"

    run_ingest_sync(
        plugin=plugin,
        params={},
        bbox=[-180, -90, 180, 90],
        start="2024-01",
        end="2024-02",
        store_path=store_path,
        period_type="monthly",
    )

    assert read_committed_period_ids(store_path, "monthly") == {"2024-01", "2024-02"}


# ---------------------------------------------------------------------------
# load_plugin
# ---------------------------------------------------------------------------


def test_load_plugin_imports_and_instantiates(tmp_path: Path) -> None:
    """load_plugin can resolve built-in plugins by dotted path."""
    plugin = load_plugin("climate_api.ingest.plugins.era5_land.Era5LandPlugin", {"variable": "t2m"})
    assert isinstance(plugin, IngestionPlugin)
    assert plugin.max_concurrency >= 1  # type: ignore[attr-defined]


def test_load_plugin_raises_for_invalid_path() -> None:
    with pytest.raises(ValueError, match="Invalid plugin path"):
        load_plugin("NotADottedPath", {})


def test_load_plugin_raises_for_non_protocol() -> None:
    with pytest.raises(TypeError, match="does not implement IngestionPlugin"):
        load_plugin("builtins.dict", {})


# ---------------------------------------------------------------------------
# read_committed_period_ids
# ---------------------------------------------------------------------------


def test_read_committed_period_ids_empty_when_no_store(tmp_path: Path) -> None:
    assert read_committed_period_ids(tmp_path / "nostore.icechunk", "monthly") == set()


# ---------------------------------------------------------------------------
# Era5LandPlugin._build_periods (unit tests, no network)
# ---------------------------------------------------------------------------


def test_era5land_build_periods_respects_hour_component() -> None:
    from climate_api.ingest.plugins.era5_land import Era5LandPlugin

    plugin = Era5LandPlugin(variable="t2m")
    periods = plugin._build_periods("2024-01-01T06", "2024-01-01T08")
    assert periods == ["2024-01-01T06", "2024-01-01T07", "2024-01-01T08"]


def test_era5land_build_periods_single_hour() -> None:
    from climate_api.ingest.plugins.era5_land import Era5LandPlugin

    plugin = Era5LandPlugin(variable="t2m")
    periods = plugin._build_periods("2024-01-01T00", "2024-01-01T00")
    assert periods == ["2024-01-01T00"]


def test_era5land_build_periods_spans_months() -> None:
    from climate_api.ingest.plugins.era5_land import Era5LandPlugin

    plugin = Era5LandPlugin(variable="t2m")
    periods = plugin._build_periods("2024-01-31T23", "2024-02-01T01")
    assert periods == ["2024-01-31T23", "2024-02-01T00", "2024-02-01T01"]


# ---------------------------------------------------------------------------
# Rechunking
# ---------------------------------------------------------------------------


def _time_chunk_size(store_path: Path) -> int:
    """Read the time chunk size of the first data variable from the committed store."""
    import icechunk
    import zarr

    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path)))
    session = repo.readonly_session("main")
    g = zarr.open_group(session.store, mode="r")
    for name in g.array_keys():
        arr = g[name]
        dims = list(arr.metadata.dimension_names or [])  # type: ignore[union-attr]
        if "time" in dims:
            return arr.chunks[dims.index("time")]  # type: ignore[union-attr]
    raise AssertionError("No array with a time dimension found")


def test_run_ingest_rechunks_store_after_all_periods(tmp_path: Path) -> None:
    """rechunk_time=N rewrites the store so the time chunk size is N after ingest."""
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-04",
            store_path=store_path,
            period_type="monthly",
            rechunk_time=2,
        )
    )

    assert read_committed_period_ids(store_path, "monthly") == {"2024-01", "2024-02", "2024-03", "2024-04"}
    assert _time_chunk_size(store_path) == 2


def test_run_ingest_rechunk_preserves_all_periods_in_store(tmp_path: Path) -> None:
    """After rechunking, read_committed_period_ids returns the same set as before."""
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
            rechunk_time=3,
        )
    )

    assert read_committed_period_ids(store_path, "monthly") == {"2024-01", "2024-02", "2024-03"}
    assert _time_chunk_size(store_path) == 3


def test_run_ingest_no_rechunk_when_rechunk_time_is_none(tmp_path: Path) -> None:
    """Without rechunk_time the time chunk stays at 1 (one period per commit)."""
    plugin = FakePlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert _time_chunk_size(store_path) == 1


def test_rechunk_store_noop_on_nonexistent_store(tmp_path: Path) -> None:
    from climate_api.ingest.store import rechunk_store

    rechunk_store(tmp_path / "nostore.icechunk", time_chunk=12)


def test_rechunk_store_changes_chunk_size(tmp_path: Path) -> None:
    """rechunk_store can be called directly to rechunk an existing store."""
    from climate_api.ingest.store import rechunk_store

    plugin = FakePlugin(["2024-01", "2024-02", "2024-03", "2024-04"])
    store_path = tmp_path / "test.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-04",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert _time_chunk_size(store_path) == 1

    rechunk_store(store_path, time_chunk=4)

    assert _time_chunk_size(store_path) == 4
    assert read_committed_period_ids(store_path, "monthly") == {"2024-01", "2024-02", "2024-03", "2024-04"}


def test_rechunk_store_skips_when_no_time_dimension(tmp_path: Path) -> None:
    """rechunk_store is a no-op when the store has no time dimension."""
    import icechunk
    import numpy as np
    import xarray as xr

    from climate_api.ingest.store import rechunk_store

    store_path = tmp_path / "static.icechunk"
    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.create(storage)
    ds = xr.Dataset({"elevation": xr.DataArray(np.zeros((4, 4), dtype="float32"), dims=["y", "x"])})
    session = repo.writable_session("main")
    ds.to_zarr(session.store, mode="w")
    session.commit("static write")

    rechunk_store(store_path, time_chunk=12)


def test_era5land_plugin_declares_rechunk_time() -> None:
    from climate_api.ingest.plugins.era5_land import Era5LandPlugin

    plugin = Era5LandPlugin(variable="t2m")
    assert plugin.rechunk_time == 12


# ---------------------------------------------------------------------------
# time_dim=False (static datasets)
# ---------------------------------------------------------------------------


class FakeStaticPlugin(FakePlugin):
    """FakePlugin variant whose probe returns time_dim=False (static dataset)."""

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        return GridSpec(shape=(4, 4), crs=4326, dtype=np.dtype("float32"), nodata=None, time_dim=False)

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        self.fetched.append(period_id)
        return xr.Dataset(
            {"elevation": xr.DataArray(np.zeros((4, 4), dtype="float32"), dims=["y", "x"])},
        )


def test_run_ingest_static_dataset_writes_once(tmp_path: Path) -> None:
    """time_dim=False: the orchestrator commits only one write (no append) and the
    store has no time dimension."""
    import icechunk
    import zarr

    plugin = FakeStaticPlugin(["2024-01", "2024-02", "2024-03"])
    store_path = tmp_path / "static.icechunk"

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-03",
            store_path=store_path,
            period_type="monthly",
        )
    )

    assert store_path.exists()
    # The store must exist and contain the static variable without a time axis.
    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path)))
    session = repo.readonly_session("main")
    g = zarr.open_group(session.store, mode="r")
    assert "elevation" in g
    assert "time" not in g


# ---------------------------------------------------------------------------
# apply_transforms
# ---------------------------------------------------------------------------


def test_run_ingest_apply_transforms_called_per_period(tmp_path: Path) -> None:
    """apply_transforms is invoked for every fetched period before writing."""
    plugin = FakePlugin(["2024-01", "2024-02"])
    store_path = tmp_path / "test.icechunk"
    transform_calls: list[str] = []

    def record_transform(ds: xr.Dataset) -> xr.Dataset:
        transform_calls.append(str(ds.time.values[0]))
        return ds

    asyncio.run(
        run_ingest(
            plugin=plugin,
            params={},
            bbox=[-180, -90, 180, 90],
            start="2024-01",
            end="2024-02",
            store_path=store_path,
            period_type="monthly",
            apply_transforms=record_transform,
        )
    )

    assert len(transform_calls) == 2
    committed = read_committed_period_ids(store_path, "monthly")
    assert committed == {"2024-01", "2024-02"}
