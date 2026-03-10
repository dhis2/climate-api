"""WorldPop fetch/sync service."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from dhis2eo.data.worldpop.pop_total import yearly as pop_total_yearly

from eo_api.utils.cache import bbox_token, write_manifest

WORLDPOP_DATASET_ID = "worldpop-global-2-total-population-1km"
WORLDPOP_MIN_YEAR = 2015
WORLDPOP_MAX_YEAR = 2030


def _scope_key(country_code: str | None, bbox: Sequence[float] | None) -> str:
    """Build a stable scope key from country or bbox input."""
    if country_code:
        return f"iso_{country_code.upper()}"
    if bbox is None:
        raise ValueError("Expected bbox when country_code is not set")
    return f"bbox_{bbox_token(bbox)}"


def build_sync_plan(
    *,
    country_code: str | None,
    bbox: Sequence[float] | None,
    start_year: int,
    end_year: int,
    output_format: str,
    root_dir: Path,
) -> dict[str, Any]:
    """Build a deterministic sync plan for WorldPop data artifacts."""
    scope_key = _scope_key(country_code, bbox)
    years = list(range(start_year, end_year + 1))
    suffix = ".nc" if output_format == "netcdf" else (".tif" if output_format == "geotiff" else ".zarr")
    target_dir = root_dir / scope_key / output_format
    planned_files = [target_dir / f"{WORLDPOP_DATASET_ID}_{scope_key}_{year}{suffix}" for year in years]
    existing_files = [path for path in planned_files if path.exists()]
    missing_files = [path for path in planned_files if not path.exists()]
    manifest_path = root_dir / "manifests" / f"{scope_key}_{start_year}_{end_year}_{output_format}.json"

    return {
        "dataset_id": WORLDPOP_DATASET_ID,
        "scope_key": scope_key,
        "years": years,
        "output_format": output_format,
        "target_dir": str(target_dir),
        "planned_files": [str(path) for path in planned_files],
        "existing_files": [str(path) for path in existing_files],
        "missing_files": [str(path) for path in missing_files],
        "manifest_path": str(manifest_path),
    }


def _download_worldpop_yearly(
    *,
    start_year: int,
    end_year: int,
    country_code: str,
    dirname: str,
    prefix: str,
) -> list[str]:
    """Download WorldPop yearly files using dhis2eo."""
    files = pop_total_yearly.download(
        start=f"{start_year:04d}",
        end=f"{end_year:04d}",
        country_code=country_code,
        dirname=dirname,
        prefix=prefix,
        version="global2",
        overwrite=False,
    )
    return [str(path) for path in files]


def sync_worldpop(
    *,
    country_code: str | None,
    bbox: Sequence[float] | None,
    start_year: int,
    end_year: int,
    output_format: str,
    root_dir: Path,
    dry_run: bool,
) -> dict[str, Any]:
    """Create/update sync manifest for a WorldPop request scope."""
    plan = build_sync_plan(
        country_code=country_code,
        bbox=bbox,
        start_year=start_year,
        end_year=end_year,
        output_format=output_format,
        root_dir=root_dir,
    )

    target_dir = Path(plan["target_dir"])
    manifest_path = Path(plan["manifest_path"])
    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "netcdf":
            if country_code is None:
                raise ValueError("Real WorldPop netcdf sync currently requires country_code scope")
            downloaded_files = _download_worldpop_yearly(
                start_year=start_year,
                end_year=end_year,
                country_code=country_code.upper(),
                dirname=str(target_dir),
                prefix=f"{WORLDPOP_DATASET_ID}_{plan['scope_key']}",
            )
            plan["planned_files"] = downloaded_files
            plan["existing_files"] = [path for path in downloaded_files if Path(path).exists()]
            plan["missing_files"] = [path for path in downloaded_files if not Path(path).exists()]
            implementation_status = "country_netcdf_download"
        else:
            implementation_status = "planning_only"

        write_manifest(
            manifest_path,
            {
                "dataset_id": plan["dataset_id"],
                "scope_key": plan["scope_key"],
                "years": plan["years"],
                "output_format": plan["output_format"],
                "planned_files": plan["planned_files"],
                "existing_files": plan["existing_files"],
                "missing_files": plan["missing_files"],
                "implementation_status": implementation_status,
            },
        )
        plan["implementation_status"] = implementation_status
    else:
        plan["implementation_status"] = "planning_only"

    return plan


def resolve_worldpop_files(
    *,
    raster_files: Sequence[str] | None,
    country_code: str | None,
    bbox: Sequence[float] | None,
    start_year: int,
    end_year: int,
    output_format: str,
    root_dir: Path,
    dry_run: bool,
) -> dict[str, Any]:
    """Resolve file availability using cache-first + download strategy."""
    if raster_files:
        files = [str(path) for path in raster_files]
        return {
            "files": files,
            "plan": {"mode": "provided-raster-files", "planned_files": files},
            "strategy_used": "provided-raster-files",
            "cache_hit": all(Path(path).exists() for path in files),
            "download_attempted": False,
            "not_implemented_reason": None,
            "reason": None if files else "No raster_files were provided.",
        }

    initial_plan = build_sync_plan(
        country_code=country_code,
        bbox=bbox,
        start_year=start_year,
        end_year=end_year,
        output_format=output_format,
        root_dir=root_dir,
    )
    initial_existing = [path for path in initial_plan["planned_files"] if Path(path).exists()]
    initial_missing = [path for path in initial_plan["planned_files"] if not Path(path).exists()]

    can_download_netcdf = output_format == "netcdf" and country_code is not None
    download_attempted = False
    not_implemented_reason: str | None = None
    reason: str | None

    if dry_run:
        plan = sync_worldpop(
            country_code=country_code,
            bbox=bbox,
            start_year=start_year,
            end_year=end_year,
            output_format=output_format,
            root_dir=root_dir,
            dry_run=True,
        )
        files = initial_existing
        strategy_used = "dry-run-plan-only"
        reason = "Dry run mode does not fetch missing files."
    elif can_download_netcdf:
        download_attempted = bool(initial_missing)
        plan = sync_worldpop(
            country_code=country_code,
            bbox=bbox,
            start_year=start_year,
            end_year=end_year,
            output_format=output_format,
            root_dir=root_dir,
            dry_run=False,
        )
        files = [path for path in plan["planned_files"] if Path(path).exists()]
        strategy_used = "cache-only" if not download_attempted else "cache-and-download"
        reason = None if files else "WorldPop netcdf download attempted but no files were produced."
    else:
        plan = sync_worldpop(
            country_code=country_code,
            bbox=bbox,
            start_year=start_year,
            end_year=end_year,
            output_format=output_format,
            root_dir=root_dir,
            dry_run=False,
        )
        files = initial_existing
        strategy_used = "cache-only"
        if initial_missing:
            not_implemented_reason = (
                f"Download strategy for output_format='{output_format}' is not implemented yet in fetch service."
            )
        reason = not_implemented_reason or (None if files else "No cached files were found for requested scope.")

    return {
        "files": files,
        "plan": plan,
        "strategy_used": strategy_used,
        "cache_hit": bool(initial_existing) and not bool(initial_missing),
        "download_attempted": download_attempted,
        "not_implemented_reason": not_implemented_reason,
        "reason": reason,
    }
