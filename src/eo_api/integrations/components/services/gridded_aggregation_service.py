"""Generic temporal/spatial aggregation stage services for gridded datasets."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.adapters.temporal.policy import evaluate_temporal_request
from eo_api.integrations.components.services.spatial_aggregate_service import aggregate_gridded_rows_by_features
from eo_api.integrations.components.services.temporal_aggregate_service import aggregate_gridded_time_rows_by_features


def run_temporal_aggregation_stage(
    params: dict[str, Any],
    *,
    source_resolution_default: str,
    supported_resolutions: set[str],
    supports_downsample: bool,
    cache_root: str | None = None,
    preferred_value_variable: str | None = None,
) -> dict[str, Any]:
    """Run temporal aggregation policy and optional gridded aggregation execution."""
    decision = evaluate_temporal_request(
        source_resolution=str(params.get("source_temporal_resolution", source_resolution_default)),
        requested_resolution=str(params.get("temporal_resolution", source_resolution_default)),
        supported_resolutions=supported_resolutions,
        supports_downsample=supports_downsample,
    )
    if decision["action"] == "exit":
        return {
            "files": params.get("files", []),
            "rows": params.get("rows", []),
            "_step_control": {"action": "exit", "reason": decision["reason"]},
        }

    can_aggregate = (
        bool(params.get("files"))
        and bool(params.get("valid_features"))
        and bool(params.get("start_date"))
        and bool(params.get("end_date"))
        and bool(cache_root)
    )
    if can_aggregate:
        result = aggregate_gridded_time_rows_by_features(
            files=[str(path) for path in params.get("files", [])],
            valid_features=list(params.get("valid_features", [])),
            start_date=str(params["start_date"]),
            end_date=str(params["end_date"]),
            spatial_reducer=str(params.get("spatial_reducer", "mean")),
            temporal_resolution=str(params.get("temporal_resolution", source_resolution_default)),
            temporal_reducer=str(params.get("temporal_reducer", "sum")),
            value_rounding=int(params.get("value_rounding", 3)),
            cache_root=str(cache_root),
            preferred_var=preferred_value_variable,
            stage=str(params.get("stage", "final")),
            flavor=str(params.get("flavor", "rnl")),
        )
    else:
        result = {
            "files": params.get("files", []),
            "rows": params.get("rows", []),
        }

    if decision["action"] == "pass_through":
        result["_step_control"] = {"action": "pass_through", "reason": decision["reason"]}
    return result


def run_spatial_aggregation_stage(params: dict[str, Any]) -> dict[str, Any]:
    """Run generic spatial stage: pass-through rows or aggregate raster files by features."""
    rows = params.get("rows")
    if isinstance(rows, list) and rows:
        return {
            "rows": rows,
            "_step_control": {
                "action": "pass_through",
                "reason": "Spatial aggregation already available from upstream stage.",
            },
        }

    files = [str(path) for path in params.get("files", [])]
    feature_collection = params.get("feature_collection")
    if files and isinstance(feature_collection, dict):
        result = aggregate_gridded_rows_by_features(
            files=files,
            feature_collection=feature_collection,
            start_year=int(params.get("start_year", 0) or 0),
            org_unit_id_property=str(params.get("org_unit_id_property", "id")),
            reducer=str(params.get("reducer", "sum")),
        )
        return {"rows": result["rows"], "summary": result["summary"]}

    return {
        "files": files,
        "rows": [] if rows is None else rows,
        "_step_control": {
            "action": "pass_through",
            "reason": "Spatial stage has no feature/raster pair to aggregate.",
        },
    }
