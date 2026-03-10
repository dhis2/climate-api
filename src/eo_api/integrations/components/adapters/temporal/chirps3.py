"""CHIRPS3 temporal aggregation adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.adapters.temporal.policy import evaluate_temporal_request
from eo_api.integrations.components.services.temporal_aggregate_service import aggregate_chirps_rows


def run(params: dict[str, Any], download_dir: str) -> dict[str, Any]:
    """Aggregate CHIRPS temporally/spatially and return rows."""
    decision = evaluate_temporal_request(
        source_resolution=str(params.get("source_temporal_resolution", "daily")),
        requested_resolution=str(params.get("temporal_resolution", "daily")),
        supported_resolutions={"daily", "weekly", "monthly"},
        supports_downsample=True,
    )
    if decision["action"] == "exit":
        return {
            "rows": [],
            "_step_control": {"action": "exit", "reason": decision["reason"]},
        }

    payload = dict(params)
    payload.pop("dataset", None)
    payload.pop("source_temporal_resolution", None)
    payload["cache_root"] = download_dir
    result = aggregate_chirps_rows(**payload)
    if decision["action"] == "pass_through":
        result["_step_control"] = {
            "action": "pass_through",
            "reason": decision["reason"],
        }
    return result
