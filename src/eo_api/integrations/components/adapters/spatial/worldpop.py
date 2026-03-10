"""WorldPop spatial aggregation adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.spatial_aggregate_service import build_worldpop_rows


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Aggregate WorldPop raster files by features and return canonical rows."""
    result = build_worldpop_rows(
        files=[str(path) for path in params.get("files", [])],
        feature_collection=params["feature_collection"],
        start_year=int(params["start_year"]),
        org_unit_id_property=str(params.get("org_unit_id_property", "id")),
        reducer=str(params.get("reducer", "sum")),
    )
    return {"rows": result["rows"], "summary": result["summary"]}
