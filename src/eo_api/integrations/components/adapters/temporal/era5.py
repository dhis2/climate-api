"""ERA5 temporal adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.adapters.temporal.policy import evaluate_temporal_request


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Apply temporal policy for ERA5 and return pass-through or exit."""
    decision = evaluate_temporal_request(
        source_resolution=str(params.get("source_temporal_resolution", "hourly")),
        requested_resolution=str(params.get("temporal_resolution", "hourly")),
        supported_resolutions={"hourly", "daily", "weekly", "monthly", "yearly", "annual"},
        supports_downsample=False,
    )
    if decision["action"] == "pass_through":
        return {
            "files": params.get("files", []),
            "_step_control": {"action": "pass_through", "reason": decision["reason"]},
        }
    return {
        "files": params.get("files", []),
        "_step_control": {
            "action": "exit",
            "reason": decision["reason"],
        },
    }
