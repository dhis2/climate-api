"""Extent registry backed by CLIMATE_API_CONFIG."""

from typing import Any

from fastapi import HTTPException

from climate_api import config as api_config


def list_extents() -> list[dict[str, Any]]:
    """Return configured extents for this Climate API instance.

    Reads the single extent defined under the ``extent:`` key in
    CLIMATE_API_CONFIG. Returns an empty list when no config is set.
    """
    extent = api_config.get_config().get("extent")
    if extent is None:
        return []
    return [extent] if isinstance(extent, dict) else []


def get_extent_or_404(extent_id: str) -> dict[str, Any]:
    """Return one configured extent or raise 404."""
    for extent in list_extents():
        if extent.get("id") == extent_id:
            return extent
    raise HTTPException(status_code=404, detail=f"Extent '{extent_id}' not found")
