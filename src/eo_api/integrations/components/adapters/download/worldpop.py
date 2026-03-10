"""WorldPop download/sync adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eo_api.integrations.components.services.worldpop_fetch_service import resolve_worldpop_files


def run(params: dict[str, Any], download_dir: Path) -> dict[str, Any]:
    """Resolve WorldPop files using fetch-service cache/download strategy."""
    resolution = resolve_worldpop_files(
        raster_files=params.get("raster_files"),
        country_code=params.get("country_code"),
        bbox=params.get("bbox"),
        start_year=int(params["start_year"]),
        end_year=int(params["end_year"]),
        output_format=str(params.get("output_format", "netcdf")),
        root_dir=download_dir / "worldpop_cache",
        dry_run=bool(params.get("dry_run", False)),
    )
    files = [str(path) for path in resolution["files"]]
    if not files:
        return {
            "dataset": "worldpop",
            "files": [],
            "_step_control": {
                "action": "exit",
                "reason": str(
                    resolution.get("reason") or "No local WorldPop raster files available for downstream processing."
                ),
            },
        }
    return {
        "dataset": "worldpop",
        "files": files,
        "sync": resolution["plan"],
        "strategy": {
            "used": resolution["strategy_used"],
            "cache_hit": bool(resolution["cache_hit"]),
            "download_attempted": bool(resolution["download_attempted"]),
            "not_implemented_reason": resolution.get("not_implemented_reason"),
        },
        "source_temporal_resolution": "yearly",
    }
