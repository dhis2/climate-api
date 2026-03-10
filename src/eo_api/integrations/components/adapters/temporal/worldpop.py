"""WorldPop temporal adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.gridded_aggregation_service import run_temporal_aggregation_stage


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Run generic temporal stage with WorldPop capability profile."""
    return run_temporal_aggregation_stage(
        params=params,
        source_resolution_default="yearly",
        supported_resolutions={"yearly", "annual"},
        supports_downsample=True,
    )
