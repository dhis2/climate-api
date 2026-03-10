"""CHIRPS3 download adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eo_api.integrations.components.services.chirps3_fetch_service import resolve_chirps3_files


def run(params: dict[str, Any], download_dir: Path) -> dict[str, Any]:
    """Resolve CHIRPS3 files using fetch-service strategy."""
    result = resolve_chirps3_files(
        start=str(params["start"]),
        end=str(params["end"]),
        bbox=[float(value) for value in list(params["bbox"])],
        stage=str(params.get("stage", "final")),
        flavor=str(params.get("flavor", "rnl")),
        download_root=download_dir,
        output_format=str(params.get("output_format", "netcdf")),
    )
    files = [str(path) for path in result["files"]]
    if not files:
        return {
            "dataset": "chirps3",
            "files": [],
            "_step_control": {
                "action": "exit",
                "reason": str(result.get("reason") or "No local CHIRPS3 files available."),
            },
        }
    return {
        "dataset": "chirps3",
        "files": files,
        "cache": result.get("cache"),
        "strategy": {
            "used": result["strategy_used"],
            "cache_hit": bool(result["cache_hit"]),
            "download_attempted": bool(result["download_attempted"]),
            "not_implemented_reason": result.get("not_implemented_reason"),
        },
        "source_temporal_resolution": "daily",
    }
