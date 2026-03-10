"""WorldPop spatial aggregation adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.gridded_aggregation_service import run_spatial_aggregation_stage


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Run generic spatial stage for WorldPop inputs."""
    return run_spatial_aggregation_stage(params)
