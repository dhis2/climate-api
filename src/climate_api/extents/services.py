"""Extent registry backed by CLIMATE_API_CONFIG."""

from typing import Any

from fastapi import HTTPException

from climate_api import config as api_config


def get_extent() -> dict[str, Any] | None:
    """Return the configured extent for this Climate API instance, or None if not set."""
    extent = api_config.get_config().get("extent")
    if extent is None:
        return None
    if not isinstance(extent, dict):
        raise ValueError("extent in CLIMATE_API_CONFIG must be a mapping")
    if not isinstance(extent.get("id"), str) or not extent["id"]:
        raise ValueError("extent.id in CLIMATE_API_CONFIG must be a non-empty string")
    if not isinstance(extent.get("bbox"), list) or len(extent["bbox"]) != 4:
        raise ValueError("extent.bbox in CLIMATE_API_CONFIG must be a list of four numbers [xmin, ymin, xmax, ymax]")
    return extent


def get_extent_or_404(extent_id: str) -> dict[str, Any]:
    """Return the configured extent if its id matches, or raise 404."""
    extent = get_extent()
    if extent is not None and extent.get("id") == extent_id:
        return extent
    raise HTTPException(status_code=404, detail=f"Extent '{extent_id}' not found")
