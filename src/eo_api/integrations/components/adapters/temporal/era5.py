"""ERA5 temporal adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.gridded_aggregation_service import run_temporal_aggregation_stage


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Run generic temporal stage with ERA5 capability profile."""
    return run_temporal_aggregation_stage(
        params=params,
        source_resolution_default="hourly",
        supported_resolutions={"hourly", "daily", "weekly", "monthly", "yearly", "annual"},
        supports_downsample=False,
    )
