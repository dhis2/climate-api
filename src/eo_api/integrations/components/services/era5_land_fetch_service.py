"""Reusable ERA5-Land download component."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly


def download_era5_land(
    *,
    start: str,
    end: str,
    bbox: list[float],
    variables: list[str],
    download_root: Path,
) -> dict[str, Any]:
    """Download ERA5-Land files to local storage and return metadata."""
    download_root.mkdir(parents=True, exist_ok=True)
    files = era5_land_hourly.download(
        start=start,
        end=end,
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]),
        dirname=str(download_root),
        prefix="era5",
        variables=variables,
    )
    file_paths = [str(path) for path in files]
    return {
        "files": file_paths,
        "summary": {
            "file_count": len(file_paths),
            "variables": variables,
            "start": start,
            "end": end,
        },
    }


def resolve_era5_land_files(
    *,
    start: str,
    end: str,
    bbox: list[float],
    variables: list[str],
    download_root: Path,
    output_format: str = "netcdf",
) -> dict[str, Any]:
    """Resolve ERA5 files using cache-aware download strategy."""
    if output_format != "netcdf":
        return {
            "files": [],
            "planned_files": [],
            "existing_files": [],
            "missing_files": [],
            "strategy_used": "cache-only",
            "cache_hit": False,
            "download_attempted": False,
            "implementation_status": "not_implemented",
            "not_implemented_reason": (
                f"ERA5-Land fetch output_format='{output_format}' is not implemented; supported: netcdf."
            ),
            "reason": f"Unsupported ERA5-Land output_format '{output_format}'.",
        }

    result = download_era5_land(
        start=start,
        end=end,
        bbox=bbox,
        variables=variables,
        download_root=download_root,
    )
    files = [str(path) for path in result["files"]]
    existing_files = [path for path in files if Path(path).exists()]
    missing_files = [path for path in files if not Path(path).exists()]
    return {
        "files": existing_files,
        "planned_files": files,
        "existing_files": existing_files,
        "missing_files": missing_files,
        "strategy_used": "cache-and-download",
        "cache_hit": bool(existing_files) and not bool(missing_files),
        "download_attempted": bool(missing_files) or bool(existing_files),
        "implementation_status": "ok",
        "not_implemented_reason": None,
        "reason": None if existing_files else "ERA5-Land download produced no local files.",
        "summary": result["summary"],
    }
