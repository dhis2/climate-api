"""openEO /processes endpoint — wraps the native process registry."""

from __future__ import annotations

from typing import Any

from climate_api.data_registry.services import processes as process_registry

# openEO standard processes that we declare support for.
# Parameters follow the openEO spec: https://processes.openeo.org/
_OPENEO_STANDARD_PROCESSES: list[dict[str, Any]] = [
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
        "description": "Saves processed data to the local user workspace. The data is stored in the format specified.",
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
    {
        "id": "aggregate_temporal_period",
        "summary": "Aggregate temporal dimension over calendar hierarchies",
        "description": "Computes a temporal aggregation based on calendar hierarchies such as years, months, or seasons.",  # noqa: E501
        "parameters": [
            {"name": "data", "description": "The source data cube", "schema": {"type": "object"}},
            {
                "name": "period",
                "description": "Calendar period (e.g. day, week, month, season, year)",
                "schema": {"type": "string"},
            },
            {"name": "reducer", "description": "Reducer process (e.g. mean, sum, min, max)", "schema": {}},
        ],
        "returns": {"description": "A data cube with dates as the dimension labels.", "schema": {"type": "object"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#aggregate_temporal_period"}],
    },
    {
        "id": "aggregate_spatial",
        "summary": "Aggregate spatial dimensions over geometries",
        "description": "Aggregates statistics for one or more geometries (e.g. zonal statistics).",
        "parameters": [
            {"name": "data", "description": "The source data cube", "schema": {"type": "object"}},
            {"name": "geometries", "description": "GeoJSON or vector cube", "schema": {}},
            {"name": "reducer", "description": "Reducer process", "schema": {}},
        ],
        "returns": {"description": "A vector data cube with the computed statistics.", "schema": {"type": "object"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#aggregate_spatial"}],
    },
    {
        "id": "filter_temporal",
        "summary": "Temporal filter for a temporal extent",
        "description": "Limits the data cube to the specified interval of dates and/or times.",
        "parameters": [
            {"name": "data", "description": "The source data cube", "schema": {"type": "object"}},
            {
                "name": "extent",
                "description": "Left-closed temporal interval [start, end]",
                "schema": {"type": "array"},
            },
        ],
        "returns": {"description": "A data cube restricted to the temporal extent.", "schema": {"type": "object"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#filter_temporal"}],
    },
    {
        "id": "filter_bbox",
        "summary": "Spatial filter using a bounding box",
        "description": "Limits the data cube to the specified bounding box.",
        "parameters": [
            {"name": "data", "description": "The source data cube", "schema": {"type": "object"}},
            {"name": "extent", "description": "Bounding box {west, south, east, north}", "schema": {"type": "object"}},
        ],
        "returns": {"description": "A data cube restricted to the bounding box.", "schema": {"type": "object"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#filter_bbox"}],
    },
    {
        "id": "mean",
        "summary": "Arithmetic mean",
        "description": "Computes the arithmetic mean of an array of numbers.",
        "parameters": [{"name": "data", "description": "An array of numbers", "schema": {"type": "array"}}],
        "returns": {"description": "The computed mean.", "schema": {"type": "number"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#mean"}],
    },
    {
        "id": "sum",
        "summary": "Compute the sum by adding up numbers",
        "description": "Adds up all elements in a sequential array of numbers.",
        "parameters": [{"name": "data", "description": "An array of numbers", "schema": {"type": "array"}}],
        "returns": {"description": "The computed sum of the numbers.", "schema": {"type": "number"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#sum"}],
    },
    {
        "id": "min",
        "summary": "Minimum value",
        "description": "Computes the smallest value of an array of numbers.",
        "parameters": [{"name": "data", "description": "An array of numbers", "schema": {"type": "array"}}],
        "returns": {"description": "The minimum value.", "schema": {"type": "number"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#min"}],
    },
    {
        "id": "max",
        "summary": "Maximum value",
        "description": "Computes the largest value of an array of numbers.",
        "parameters": [{"name": "data", "description": "An array of numbers", "schema": {"type": "array"}}],
        "returns": {"description": "The maximum value.", "schema": {"type": "number"}},
        "links": [{"rel": "about", "href": "https://processes.openeo.org/#max"}],
    },
]


def list_openeo_processes() -> list[dict[str, Any]]:
    """Return all openEO-compatible process descriptions.

    Merges standard openEO processes with native plugin processes from the registry.
    """
    standard_ids = {p["id"] for p in _OPENEO_STANDARD_PROCESSES}
    result = list(_OPENEO_STANDARD_PROCESSES)

    for process in process_registry.list_processes():
        if not process.get("expose"):
            continue
        pid = process.get("id")
        if pid in standard_ids:
            continue
        result.append(_native_to_openeo(process))

    return result


def _native_to_openeo(process: dict[str, Any]) -> dict[str, Any]:
    """Convert a native process registry entry to openEO process format."""
    pid = process.get("id", "")
    raw_inputs = process.get("inputs") or {}
    parameters = []
    for name, spec in raw_inputs.items() if isinstance(raw_inputs, dict) else []:
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
        if schema_type:
            param["schema"] = {"type": schema_type}
        else:
            param["schema"] = {}
        parameters.append(param)

    return {
        "id": pid,
        "summary": process.get("title", pid),
        "description": process.get("description", ""),
        "parameters": parameters,
        "returns": {"description": "Result", "schema": {}},
        "links": [{"rel": "self", "href": f"/processes/{pid}"}],
    }
