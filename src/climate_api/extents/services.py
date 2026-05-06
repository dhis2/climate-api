"""Extent registry backed by CLIMATE_API_CONFIG or a legacy extents.yaml."""

from pathlib import Path
from typing import Any

import yaml
from fastapi import HTTPException

from climate_api import config as api_config

# Legacy fallback path — used when CLIMATE_API_CONFIG is not set.
_LEGACY_EXTENTS_PATH = Path(__file__).parent.parent.parent.parent / "data" / "extents.yaml"


def list_extents() -> list[dict[str, Any]]:
    """Return configured extents for this Climate API instance.

    When CLIMATE_API_CONFIG is set, returns the single extent defined under
    the ``extent:`` key. Otherwise falls back to the legacy ``data/extents.yaml``
    list for backward compatibility.
    """
    extent = api_config.get_config().get("extent")
    if extent is not None:
        return [extent] if isinstance(extent, dict) else []

    if not _LEGACY_EXTENTS_PATH.exists():
        return []
    payload = yaml.safe_load(_LEGACY_EXTENTS_PATH.read_text(encoding="utf-8")) or {}
    extents = payload.get("extents", [])
    if not isinstance(extents, list):
        raise ValueError(f"Expected 'extents' list in {_LEGACY_EXTENTS_PATH}")
    return [e for e in extents if isinstance(e, dict)]


def get_extent_or_404(extent_id: str) -> dict[str, Any]:
    """Return one configured extent or raise 404."""
    for extent in list_extents():
        if extent.get("id") == extent_id:
            return extent
    raise HTTPException(status_code=404, detail=f"Extent '{extent_id}' not found")
