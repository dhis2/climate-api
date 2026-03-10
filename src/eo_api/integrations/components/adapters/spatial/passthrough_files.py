"""Shared file passthrough for spatial component."""

from __future__ import annotations

from typing import Any


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Pass through files for datasets with deferred spatial aggregation."""
    return {
        "files": params.get("files", []),
        "rows": params.get("rows", []),
        "_step_control": {
            "action": "pass_through",
            "reason": "Spatial aggregation deferred to dataset-specific payload/analysis component.",
        },
    }
