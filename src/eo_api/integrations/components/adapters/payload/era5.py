"""ERA5 payload adapter placeholder."""

from __future__ import annotations

from typing import Any


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Exit until ERA5 payload logic is implemented."""
    del params
    return {
        "_step_control": {
            "action": "exit",
            "reason": "ERA5 -> DHIS2 payload builder is not implemented in this scaffold.",
        }
    }
