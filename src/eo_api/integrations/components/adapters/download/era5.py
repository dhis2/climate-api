"""ERA5 download adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eo_api.integrations.components.services.era5_land_fetch_service import resolve_era5_land_files


def run(params: dict[str, Any], download_dir: Path) -> dict[str, Any]:
    """Resolve ERA5 files using fetch-service strategy."""
    result = resolve_era5_land_files(
        start=params["start"],
        end=params["end"],
        bbox=params["bbox"],
        variables=params["variables"],
        download_root=download_dir,
        output_format=str(params.get("output_format", "netcdf")),
    )
    files = [str(path) for path in result["files"]]
    if not files:
        return {
            "dataset": "era5",
            "files": [],
            "_step_control": {
                "action": "exit",
                "reason": str(result.get("reason") or "No local ERA5 files available."),
            },
        }
    return {
        "dataset": "era5",
        "files": files,
        "summary": result["summary"],
        "strategy": {
            "used": result["strategy_used"],
            "cache_hit": bool(result["cache_hit"]),
            "download_attempted": bool(result["download_attempted"]),
            "not_implemented_reason": result.get("not_implemented_reason"),
        },
        "source_temporal_resolution": "hourly",
    }
