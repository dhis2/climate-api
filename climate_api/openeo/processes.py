"""openEO /processes endpoint — wraps the openeo-processes-dask spec registry."""

from __future__ import annotations

import inspect
import logging
from typing import Any

from climate_api.data_registry.services import processes as process_registry

logger = logging.getLogger(__name__)

_BACKEND_PROCESSES: list[dict[str, Any]] = [
    {
        "id": "load_collection",
        "summary": "Load a collection",
        "description": (
            "Loads a collection from the current back-end by its id and returns it as a processable data cube."
        ),
        "parameters": [
            {"name": "id", "description": "Collection ID", "schema": {"type": "string"}},
            {
                "name": "spatial_extent",
                "description": "Bounding box filter",
                "optional": True,
                "default": None,
                "schema": [{"type": "object"}, {"type": "null"}],
            },
            {
                "name": "temporal_extent",
                "description": "Date range filter as [start, end]",
                "optional": True,
                "default": None,
                "schema": [{"type": "array"}, {"type": "null"}],
            },
            {
                "name": "bands",
                "description": "Variable names to load",
                "optional": True,
                "default": None,
                "schema": [{"type": "array", "items": {"type": "string"}}, {"type": "null"}],
            },
        ],
        "returns": {"description": "A data cube for further processing.", "schema": {"type": "object"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#load_collection"}],
    },
    {
        "id": "save_result",
        "summary": "Save processed data to storage",
        "description": "Saves processed data to the local user workspace.",
        "parameters": [
            {"name": "data", "description": "The data to save", "schema": {"type": "object"}},
            {
                "name": "format",
                "description": "Output format (e.g. Zarr, JSON, GeoParquet)",
                "schema": {"type": "string"},
            },
            {
                "name": "options",
                "description": "Format-specific output options",
                "optional": True,
                "default": {},
                "schema": {"type": "object"},
            },
        ],
        "returns": {"description": "false if saving was not successfully finished.", "schema": {"type": "boolean"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#save_result"}],
    },
]


def _load_standard_processes() -> list[dict[str, Any]]:
    """Return process descriptions for all openeo-processes-dask implementations.

    Derives the list from the same execution registry used for process graph
    execution, guaranteeing that listed processes are actually callable.
    """
    # Use the execution registry as the authoritative source of available processes.
    # This avoids duplicating the openeo-processes-dask import logic and ensures
    # the listed processes exactly match what run_process_graph can execute.
    from climate_api.openeo.execution import _build_process_registry

    try:
        registry = _build_process_registry()
        predefined: dict[str, Any] = registry.store.get("predefined", {})
    except Exception:
        logger.warning("Could not load process registry for listing", exc_info=True)
        return []

    # Backend-specific processes are listed separately via _BACKEND_PROCESSES;
    # skip them here to avoid duplicates.
    _backend_ids = {p["id"] for p in _BACKEND_PROCESSES}

    result = []
    for process_id, proc in sorted(predefined.items()):
        if process_id in _backend_ids:
            continue
        impl = proc.implementation
        doc = inspect.getdoc(impl) or ""
        result.append(
            {
                "id": process_id,
                "summary": doc.splitlines()[0] if doc else process_id,
                "description": doc,
                "parameters": [],
                "returns": {"description": "Result.", "schema": {}},
                "links": [{"rel": "about", "href": f"https://processes.openeo.org/#{process_id}"}],
            }
        )
    return result


def list_openeo_processes() -> list[dict[str, Any]]:
    """Return all openEO-compatible process descriptions.

    Merges openeo-processes-dask standard processes with backend-specific processes
    (load_collection, save_result) and any exposed native plugin processes.
    """
    standard = _load_standard_processes()
    standard_ids = {p.get("id") for p in standard}

    # Backend processes take precedence over any stub in the standard list
    result: list[dict[str, Any]] = list(_BACKEND_PROCESSES)
    backend_ids = {p["id"] for p in _BACKEND_PROCESSES}

    for p in standard:
        pid = p.get("id")
        if pid and pid not in backend_ids:
            result.append(p)
            standard_ids.add(pid)

    # Append exposed native plugin processes
    for process in process_registry.list_processes():
        if not process.get("expose"):
            continue
        pid = process.get("id")
        if pid in standard_ids or pid in backend_ids:
            continue
        result.append(_native_to_openeo(process))

    return result


def _native_to_openeo(process: dict[str, Any]) -> dict[str, Any]:
    """Convert a native process registry entry to openEO process format."""
    pid = process.get("id", "")
    raw_inputs = process.get("inputs") or {}
    parameters = []
    for name, spec in (raw_inputs.items() if isinstance(raw_inputs, dict) else []):
        if not isinstance(spec, dict):
            continue
        param: dict[str, Any] = {"name": name}
        if spec.get("description"):
            param["description"] = spec["description"]
        if spec.get("required") is not True:
            param["optional"] = True
            if "default" in spec:
                param["default"] = spec["default"]
        schema_type = spec.get("type")
        param["schema"] = {"type": schema_type} if schema_type else {}
        parameters.append(param)

    return {
        "id": pid,
        "summary": process.get("title", pid),
        "description": process.get("description", ""),
        "parameters": parameters,
        "returns": {"description": "Result", "schema": {}},
        "links": [{"rel": "self", "href": f"/processes/{pid}"}],
    }
