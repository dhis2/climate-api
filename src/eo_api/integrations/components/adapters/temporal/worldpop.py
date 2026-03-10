"""WorldPop temporal adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.adapters.temporal.policy import evaluate_temporal_request


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Apply temporal policy for WorldPop source and requested resolution."""
    decision = evaluate_temporal_request(
        source_resolution=str(params.get("source_temporal_resolution", "yearly")),
        requested_resolution=str(params.get("temporal_resolution", "yearly")),
        supported_resolutions={"yearly", "annual"},
        supports_downsample=True,
    )
    if decision["action"] == "exit":
        return {
            "files": params.get("files", []),
            "_step_control": {
                "action": "exit",
                "reason": decision["reason"],
            },
        }
    return {
        "files": params.get("files", []),
        "_step_control": {
            "action": "pass_through",
            "reason": decision["reason"],
        },
    }
