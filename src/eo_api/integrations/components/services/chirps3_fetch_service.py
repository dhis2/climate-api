"""Reusable CHIRPS3 download component."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from dhis2eo.data.chc.chirps3 import daily as chirps3_daily

from eo_api.utils.cache import bbox_token, monthly_periods, read_manifest, write_manifest


def download_chirps3(
    *,
    start: str,
    end: str,
    bbox: list[float],
    stage: str,
    flavor: str,
    download_root: Path,
) -> dict[str, Any]:
    """Download CHIRPS3 files with scope-aware cache semantics."""
    root_dir = download_root / "chirps3_cache"
    scope_key = f"{stage}_{flavor}_{bbox_token(bbox)}"
    download_dir = root_dir / scope_key
    download_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"chirps3_{scope_key}"
    manifest_path = download_dir / "manifest.json"

    requested_months = monthly_periods(start, end)
    expected_files = [download_dir / f"{prefix}_{month}.nc" for month in requested_months]
    existing_before = {str(path) for path in expected_files if path.exists()}

    files = [
        str(path)
        for path in chirps3_daily.download(
            start=start,
            end=end,
            bbox=(bbox[0], bbox[1], bbox[2], bbox[3]),
            dirname=str(download_dir),
            prefix=prefix,
            stage=stage,
            flavor=flavor,
        )
    ]
    downloaded_now = [path for path in files if path not in existing_before]
    cache_hit = len(downloaded_now) == 0

    manifest = read_manifest(manifest_path) or {}
    manifest.update(
        {
            "dataset": "chirps3",
            "scope_key": scope_key,
            "bbox": [float(v) for v in bbox],
            "stage": stage,
            "flavor": flavor,
            "last_start": start,
            "last_end": end,
        }
    )
    write_manifest(manifest_path, manifest)

    return {
        "files": files,
        "cache": {
            "hit": cache_hit,
            "key": scope_key,
            "downloaded_delta_count": len(downloaded_now),
            "reused_count": len(files) - len(downloaded_now),
            "dir": str(download_dir),
        },
    }


def resolve_chirps3_files(
    *,
    start: str,
    end: str,
    bbox: Sequence[float],
    stage: str,
    flavor: str,
    download_root: Path,
    output_format: str = "netcdf",
) -> dict[str, Any]:
    """Resolve CHIRPS3 files using cache-aware download strategy."""
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
                f"CHIRPS3 fetch output_format='{output_format}' is not implemented; supported: netcdf."
            ),
            "reason": f"Unsupported CHIRPS3 output_format '{output_format}'.",
        }

    result = download_chirps3(
        start=start,
        end=end,
        bbox=[float(value) for value in bbox],
        stage=stage,
        flavor=flavor,
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
        "cache_hit": bool(result.get("cache", {}).get("hit")),
        "download_attempted": bool(result.get("cache", {}).get("downloaded_delta_count", 0)),
        "implementation_status": "ok",
        "not_implemented_reason": None,
        "reason": None if existing_files else "CHIRPS3 download produced no local files.",
        "cache": result.get("cache"),
    }
