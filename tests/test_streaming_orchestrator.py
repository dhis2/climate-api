from __future__ import annotations

from pathlib import Path
from typing import Any

import icechunk
import numpy as np
import pytest
import xarray as xr

import climate_api.streaming.orchestrator as streaming_orchestrator
from climate_api.streaming.orchestrator import run_streaming_ingest_sync
from climate_api.streaming.protocol import GridSpec


class _FakePlugin:
    max_concurrency = 2
    commit_batch_size = 2

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        _ = bbox, params
        return GridSpec(shape=(1, 1), crs=4326, dtype=np.dtype("float32"))

    async def periods(self, start: str, end: str) -> list[str]:
        _ = start, end
        return ["2026-01-01", "2026-01-02", "2026-01-03"]

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = bbox, params
        value = float(period_id[-2:])
        return xr.Dataset(
            {"precip": (("time", "y", "x"), np.array([[[value]]], dtype=np.float32))},
            coords={"time": [np.datetime64(period_id, "D")], "y": [0.0], "x": [1.0]},
        )


class _FakeSession:
    def __init__(self, store: str) -> None:
        self.store = store
        self.messages: list[str] = []

    def commit(self, message: str) -> None:
        self.messages.append(message)


class _FakeRepo:
    def __init__(self, store: str) -> None:
        self.store = store
        self.sessions: list[_FakeSession] = []

    def writable_session(self, branch: str) -> _FakeSession:
        assert branch == "main"
        session = _FakeSession(self.store)
        self.sessions.append(session)
        return session


def _read_committed_periods_from_zarr(store_path: Path, period_type: str) -> set[str]:
    _ = period_type
    if not store_path.exists():
        return set()
    ds = xr.open_zarr(store_path, consolidated=None)
    try:
        if "time" not in ds.coords:
            return set()
        return {str(item)[:10] for item in ds["time"].values}
    finally:
        ds.close()


def test_orchestrator_uses_store_state_as_resume_truth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))
    progress_updates: list[tuple[int | None, int | None, str | None]] = []
    cursor_saves: list[dict[str, Any]] = []

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator,
        "read_committed_period_ids",
        lambda path, period_type: _read_committed_periods_from_zarr(path, period_type),
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: not path.exists())

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        on_progress=lambda done, total, message: progress_updates.append((done, total, message)),
        save_cursor=lambda cursor: cursor_saves.append(cursor),
        load_cursor=lambda: {"last_committed": "2026-01-02"},
    )

    assert result.periods_written == 3
    assert store_path.exists()
    first = xr.open_zarr(store_path, consolidated=None)
    try:
        assert first["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
    finally:
        first.close()
    import zarr

    root = zarr.open_group(store_path, mode="r")
    assert root.attrs["proj:code"] == "EPSG:4326"
    assert root.attrs["spatial:dimensions"] == ["y", "x"]
    assert root.attrs["spatial:shape"] == [1, 1]

    run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        on_progress=lambda done, total, message: progress_updates.append((done, total, message)),
        save_cursor=lambda cursor: cursor_saves.append(cursor),
        load_cursor=lambda: {"last_committed": "2026-01-02"},
    )

    second = xr.open_zarr(store_path, consolidated=None)
    try:
        assert second["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
    finally:
        second.close()

    assert any(update[2] == "Wrote 2026-01-03" for update in progress_updates)
    assert cursor_saves[-1] == {"last_committed": "2026-01-03"}


def test_orchestrator_refuses_destructive_first_write_when_existing_store_is_not_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "streaming-store.zarr"
    store_path.mkdir()
    repo = _FakeRepo(str(store_path))

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(streaming_orchestrator, "read_committed_period_ids", lambda path, period_type: set())
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: False)

    with pytest.raises(RuntimeError, match="committed periods could not be determined safely"):
        run_streaming_ingest_sync(
            plugin=_FakePlugin(),
            params={},
            bbox=[0.0, 0.0, 1.0, 1.0],
            start="2026-01-01",
            end="2026-01-03",
            store_path=store_path,
            period_type="daily",
        )


def test_orchestrator_normalizes_invalid_plugin_batching_and_concurrency(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _InvalidPlugin(_FakePlugin):
        max_concurrency = 0
        commit_batch_size = 0

    store_path = tmp_path / "streaming-store.zarr"
    repo = _FakeRepo(str(store_path))
    cursor_saves: list[dict[str, Any]] = []

    monkeypatch.setattr(streaming_orchestrator, "open_or_create_repo", lambda path: repo)
    monkeypatch.setattr(
        streaming_orchestrator,
        "read_committed_period_ids",
        lambda path, period_type: _read_committed_periods_from_zarr(path, period_type),
    )
    monkeypatch.setattr(streaming_orchestrator, "is_store_empty", lambda path: not path.exists())

    result = run_streaming_ingest_sync(
        plugin=_InvalidPlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        save_cursor=lambda cursor: cursor_saves.append(cursor),
    )

    assert result.periods_written == 3
    assert cursor_saves[-1] == {"last_committed": "2026-01-03"}


def test_orchestrator_writes_and_reads_through_real_icechunk_repo(tmp_path: Path) -> None:
    store_path = tmp_path / "streaming-store.icechunk"

    result = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
    )

    assert result.periods_written == 3
    assert store_path.exists()

    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store)
    try:
        assert ds["precip"].values[:, 0, 0].tolist() == [1.0, 2.0, 3.0]
        assert {str(item)[:10] for item in ds["time"].values} == {"2026-01-01", "2026-01-02", "2026-01-03"}
    finally:
        ds.close()

    rerun = run_streaming_ingest_sync(
        plugin=_FakePlugin(),
        params={},
        bbox=[0.0, 0.0, 1.0, 1.0],
        start="2026-01-01",
        end="2026-01-03",
        store_path=store_path,
        period_type="daily",
        load_cursor=lambda: {"last_committed": "2026-01-02"},
    )

    assert rerun.periods_written == 0
