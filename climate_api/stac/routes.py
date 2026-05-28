"""Routes for the STAC catalogue.

The canonical collection data is now served at /collections (openEO + STAC
unified endpoint). This module serves a thin STAC landing page that STAC
Browser and other conformant clients can use to discover the catalog.
"""

import os

from fastapi import APIRouter, Request

from climate_api.stac.schemas import StacCatalogResponse

router = APIRouter()


def _abs_url(request: Request, path: str) -> str:
    base_url = os.getenv("CLIMATE_API_BASE_URL")
    if base_url:
        return f"{base_url.rstrip('/')}{path}"
    return f"{str(request.base_url).rstrip('/')}{path}"


def _build_stac_landing(request: Request) -> dict[str, object]:
    """Minimal STAC catalog root that links to /collections."""
    self_href = _abs_url(request, "/stac")
    return {
        "stac_version": "1.1.0",
        "type": "Catalog",
        "id": "climate-api",
        "title": "DHIS2 Open Climate Service",
        "description": "Published climate datasets. Collections are served at /collections.",
        "links": [
            {"rel": "self", "href": self_href, "type": "application/json"},
            {"rel": "root", "href": self_href, "type": "application/json"},
            {
                "rel": "data",
                "href": _abs_url(request, "/collections"),
                "type": "application/json",
                "title": "Collections",
            },
        ],
    }


@router.get("", response_model=StacCatalogResponse)
def get_stac_landing(request: Request) -> dict[str, object]:
    """Return the STAC catalog landing page."""
    return _build_stac_landing(request)


@router.get("/catalog.json", response_model=StacCatalogResponse)
def get_stac_catalog_json(request: Request) -> dict[str, object]:
    """Return the STAC catalog landing page (catalog.json alias)."""
    return _build_stac_landing(request)
