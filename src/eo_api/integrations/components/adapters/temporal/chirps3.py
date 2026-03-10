"""CHIRPS3 temporal aggregation adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.gridded_aggregation_service import run_temporal_aggregation_stage


def run(params: dict[str, Any], download_dir: str) -> dict[str, Any]:
    """Run generic temporal stage with CHIRPS capability profile."""
    return run_temporal_aggregation_stage(
        params=params,
        source_resolution_default="daily",
        supported_resolutions={"daily", "weekly", "monthly"},
        supports_downsample=True,
        cache_root=download_dir,
        preferred_value_variable="precip",
    )
