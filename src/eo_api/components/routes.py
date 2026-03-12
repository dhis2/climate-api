"""Component discovery and execution endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from ..data_manager.services.constants import BBOX
from . import services
from .schemas import ComponentCatalogResponse


router = APIRouter()


@router.get("/components", response_model=ComponentCatalogResponse)
def list_components() -> ComponentCatalogResponse:
    """List all discoverable reusable components."""
    return ComponentCatalogResponse(components=services.component_catalog())
