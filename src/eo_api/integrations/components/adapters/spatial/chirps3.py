"""CHIRPS3 spatial adapter."""

from __future__ import annotations

from typing import Any


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Pass through because CHIRPS spatial reduction already happened upstream."""
    return {
        "rows": params.get("rows", []),
        "_step_control": {
            "action": "pass_through",
            "reason": "Spatial reduction already produced in CHIRPS aggregate component.",
        },
    }
