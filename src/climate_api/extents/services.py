"""Extent registry backed by CLIMATE_API_CONFIG."""

from typing import Any

from fastapi import HTTPException

from climate_api import config as api_config


def get_extent() -> dict[str, Any] | None:
    """Return the configured extent for this Climate API instance, or None if not set."""
    extent = api_config.get_config().get("extent")
    return extent if isinstance(extent, dict) else None


def get_extent_or_404(extent_id: str) -> dict[str, Any]:
    """Return the configured extent if its id matches, or raise 404."""
    extent = get_extent()
    if extent is not None and extent.get("id") == extent_id:
        return extent
    raise HTTPException(status_code=404, detail=f"Extent '{extent_id}' not found")
