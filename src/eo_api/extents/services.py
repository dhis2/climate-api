"""Extent registry backed by YAML config files."""

from pathlib import Path
from typing import Any

import yaml
from fastapi import HTTPException

SCRIPT_DIR = Path(__file__).parent.resolve()
EXTENTS_PATH = SCRIPT_DIR.parent.parent.parent / "data" / "extents.yaml"


def list_extents() -> list[dict[str, Any]]:
    """Return configured extents for this EO API instance."""
    if not EXTENTS_PATH.exists():
        return []
    payload = yaml.safe_load(EXTENTS_PATH.read_text(encoding="utf-8")) or {}
    extents = payload.get("extents", [])
    if not isinstance(extents, list):
        raise ValueError(f"Expected 'extents' list in {EXTENTS_PATH}")
    return [extent for extent in extents if isinstance(extent, dict)]


def get_extent_or_404(extent_id: str) -> dict[str, Any]:
    """Return one configured extent or raise 404."""
    for extent in list_extents():
        if extent.get("id") == extent_id:
            return extent
    raise HTTPException(status_code=404, detail=f"Extent '{extent_id}' not found")
