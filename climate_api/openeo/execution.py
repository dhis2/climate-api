"""Process graph execution for openEO jobs.

Only a minimal set of processes is natively implemented here. Full
openeo-processes-dask integration is tracked in issue #155 / PR #372.
"""

from __future__ import annotations

import logging
from typing import Any

import xarray as xr
from fastapi import HTTPException, Request

from climate_api.data_accessor.services.accessor import open_icechunk_dataset, open_zarr_dataset
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import ArtifactFormat, PublicationStatus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# load_collection
# ---------------------------------------------------------------------------


def load_collection(
    collection_id: str,
    *,
    spatial_extent: dict[str, float] | None = None,
    temporal_extent: list[str | None] | None = None,
    bands: list[str] | None = None,
) -> xr.Dataset:
    """Open a published dataset as an xarray Dataset."""
    artifact = _get_published_artifact(collection_id)
    ds = _open_artifact(artifact)

    # Normalize time dimension name to openEO standard "t"
    rename_map = {d: "t" for d in ("time", "valid_time") if d in ds.dims}
    if rename_map:
        ds = ds.rename(rename_map)

    if temporal_extent is not None:
        start, end = temporal_extent[0], temporal_extent[1] if len(temporal_extent) > 1 else None
        start = _strip_tz(start)
        end = _strip_tz(end)
        if "t" in ds.dims:
            if start is not None:
                ds = ds.sel(t=slice(start, end))
            elif end is not None:
                ds = ds.sel(t=slice(None, end))

    if spatial_extent is not None:
        extent = {k.lower(): v for k, v in spatial_extent.items()}
        x_dim = next((d for d in ("x", "longitude") if d in ds.dims), None)
        y_dim = next((d for d in ("y", "latitude") if d in ds.dims), None)
        west, east = extent.get("west"), extent.get("east")
        south, north = extent.get("south"), extent.get("north")
        if x_dim and west is not None and east is not None:
            ds = ds.sel({x_dim: slice(west, east)})
        if y_dim and south is not None and north is not None:
            ds = ds.sel({y_dim: slice(south, north)})

    if bands is not None:
        available = [b for b in bands if b in ds]
        if available:
            ds = ds[available]

    return ds


def _get_published_artifact(collection_id: str) -> Any:
    eligible = _eligible_artifacts()
    artifact = eligible.get(collection_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")
    return artifact


def _eligible_artifacts() -> dict[str, Any]:
    result: dict[str, Any] = {}
    for dataset_id, artifacts in ingestion_services.group_datasets().items():
        latest = max(artifacts, key=lambda a: a.created_at)
        if latest.publication.status != PublicationStatus.PUBLISHED:
            continue
        if latest.format not in {ArtifactFormat.ZARR, ArtifactFormat.ICECHUNK}:
            continue
        result[dataset_id] = latest
    return result


def _open_artifact(artifact: Any) -> xr.Dataset:
    path = _artifact_store_path(artifact)
    if artifact.format == ArtifactFormat.ICECHUNK:
        return open_icechunk_dataset(path)
    return open_zarr_dataset(path)


def _artifact_store_path(artifact: Any) -> str:
    if artifact.path:
        return str(artifact.path)
    if artifact.asset_paths:
        return str(artifact.asset_paths[0])
    raise HTTPException(
        status_code=500,
        detail=f"Artifact '{artifact.artifact_id}' has no readable storage path",
    )


# ---------------------------------------------------------------------------
# Simple process graph runner
# ---------------------------------------------------------------------------


class ProcessGraphRunner:
    """Minimal process graph interpreter.

    Supports: load_collection, save_result, filter_temporal, filter_bbox,
    aggregate_temporal_period, mean, sum, min, max.

    Full openeo-processes-dask integration is pending Python 3.13 support.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, Any]] = {}
        self._cache: dict[str, Any] = {}

    def execute(self, process_graph: dict[str, Any]) -> Any:
        """Execute a process graph dict and return the result node's value."""
        self._nodes = process_graph
        self._cache = {}
        result_node = self._find_result_node()
        if result_node is None:
            raise HTTPException(status_code=422, detail="Process graph has no result node")
        return self._evaluate_node(result_node)

    def _find_result_node(self) -> str | None:
        for node_id, node in self._nodes.items():
            if node.get("result") is True:
                return node_id
        if self._nodes:
            return next(reversed(self._nodes))
        return None

    def _evaluate_node(self, node_id: str) -> Any:
        if node_id in self._cache:
            return self._cache[node_id]
        node = self._nodes.get(node_id)
        if node is None:
            raise HTTPException(status_code=422, detail=f"Node '{node_id}' not found in process graph")
        raw_process_id = node.get("process_id")
        if not isinstance(raw_process_id, str):
            raise HTTPException(status_code=422, detail=f"Node '{node_id}' is missing a string process_id")
        process_id: str = raw_process_id
        raw_args = node.get("arguments", {})
        args = self._resolve_args(raw_args)
        result = self._dispatch(process_id, args, node_id)
        self._cache[node_id] = result
        return result

    def _resolve_args(self, raw: Any) -> Any:
        if isinstance(raw, dict):
            if "from_node" in raw:
                return self._evaluate_node(raw["from_node"])
            if "from_parameter" in raw:
                return None
            return {k: self._resolve_args(v) for k, v in raw.items()}
        if isinstance(raw, list):
            return [self._resolve_args(item) for item in raw]
        return raw

    def _dispatch(self, process_id: str, args: dict[str, Any], node_id: str) -> Any:
        if process_id == "load_collection":
            return self._load_collection(args)
        if process_id == "save_result":
            return self._save_result(args)
        if process_id == "filter_temporal":
            return self._filter_temporal(args)
        if process_id == "filter_bbox":
            return self._filter_bbox(args)
        if process_id == "aggregate_temporal_period":
            return self._aggregate_temporal_period(args)
        if process_id in {"mean", "sum", "min", "max"}:
            return self._reducer(process_id, args)
        raise HTTPException(
            status_code=400,
            detail=(
                f"Process '{process_id}' is not yet implemented. "
                "Full openeo-processes-dask support is pending Python 3.13 compatibility."
            ),
        )

    def _load_collection(self, args: dict[str, Any]) -> xr.Dataset:
        collection_id = args.get("id")
        if not isinstance(collection_id, str):
            raise HTTPException(status_code=422, detail="load_collection requires 'id' (string)")
        return load_collection(
            collection_id,
            spatial_extent=args.get("spatial_extent"),
            temporal_extent=args.get("temporal_extent"),
            bands=args.get("bands"),
        )

    def _save_result(self, args: dict[str, Any]) -> Any:
        return args.get("data")

    def _filter_temporal(self, args: dict[str, Any]) -> Any:
        data = args.get("data")
        extent = args.get("extent", [None, None])
        if not isinstance(data, xr.Dataset):
            return data
        time_dim = next((d for d in ("t", "time") if d in data.dims), None)
        if time_dim is None:
            return data
        start = _strip_tz(extent[0] if len(extent) > 0 else None)
        end = _strip_tz(extent[1] if len(extent) > 1 else None)
        return data.sel({time_dim: slice(start, end)})

    def _filter_bbox(self, args: dict[str, Any]) -> Any:
        data = args.get("data")
        raw_extent = args.get("extent", {})
        if not isinstance(data, xr.Dataset):
            return data
        # Normalise keys to lowercase so both "west" and "West" work
        extent = {k.lower(): v for k, v in raw_extent.items()} if isinstance(raw_extent, dict) else {}
        x_dim = next((d for d in ("x", "longitude") if d in data.dims), None)
        y_dim = next((d for d in ("y", "latitude") if d in data.dims), None)
        if x_dim:
            west, east = extent.get("west"), extent.get("east")
            if west is not None and east is not None:
                data = data.sel({x_dim: slice(west, east)})
        if y_dim:
            south, north = extent.get("south"), extent.get("north")
            if south is not None and north is not None:
                coord = data[y_dim].values
                # Slice high→low for descending axes (e.g. ERA5 / WorldPop latitude)
                if len(coord) > 1 and coord[0] > coord[-1]:
                    data = data.sel({y_dim: slice(north, south)})
                else:
                    data = data.sel({y_dim: slice(south, north)})
        return data

    def _aggregate_temporal_period(self, args: dict[str, Any]) -> Any:
        data = args.get("data")
        period = args.get("period", "month")
        reducer_arg = args.get("reducer")
        if not isinstance(data, xr.Dataset):
            return data
        time_dim = next((d for d in ("t", "time") if d in data.dims), None)
        if time_dim is None:
            return data
        freq_map = {
            "day": "D",
            "week": "W",
            "dekad": "10D",
            "month": "MS",
            "season": "QS-DEC",
            "tropical-season": "QS-MAR",
            "year": "YS",
            "decade": "10YS",
        }
        freq = freq_map.get(period, "MS")
        reducer_name = _extract_reducer_name(reducer_arg)
        if reducer_name == "sum":
            return data.resample({time_dim: freq}).sum()
        if reducer_name in {"min", "minimum"}:
            return data.resample({time_dim: freq}).min()
        if reducer_name in {"max", "maximum"}:
            return data.resample({time_dim: freq}).max()
        return data.resample({time_dim: freq}).mean()

    def _reducer(self, process_id: str, args: dict[str, Any]) -> Any:
        data = args.get("data")
        if isinstance(data, xr.Dataset):
            if process_id == "sum":
                return data.sum()
            if process_id == "min":
                return data.min()
            if process_id == "max":
                return data.max()
            return data.mean()
        if isinstance(data, list):
            import statistics

            if process_id == "sum":
                return sum(float(v) for v in data)
            if process_id == "min":
                return min(float(v) for v in data)
            if process_id == "max":
                return max(float(v) for v in data)
            return statistics.mean(float(v) for v in data)
        return data


def _strip_tz(value: str | None) -> str | None:
    """Strip timezone suffix from an ISO datetime string so it matches naive xarray time coords."""
    if value is None:
        return None
    # Remove trailing Z or ±HH:MM offset
    for suffix in ("Z", "+00:00"):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _extract_reducer_name(reducer_arg: Any) -> str:
    """Extract the reducer process id from a callback argument."""
    if isinstance(reducer_arg, str):
        return reducer_arg.lower()
    if isinstance(reducer_arg, dict):
        pg = reducer_arg.get("process_graph", {})
        if isinstance(pg, dict):
            for node in pg.values():
                if isinstance(node, dict) and node.get("result"):
                    return str(node.get("process_id", "mean")).lower()
    return "mean"


def run_process_graph(
    process: dict[str, Any],
    request: Request | None = None,
    result_dir: str | None = None,
) -> Any:
    """Execute an openEO process graph and return the result dataset or value."""
    process_graph = process.get("process_graph")
    if not isinstance(process_graph, dict):
        raise HTTPException(status_code=422, detail="process.process_graph must be an object")
    runner = ProcessGraphRunner()
    return runner.execute(process_graph)
