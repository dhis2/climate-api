"""openEO /collections endpoint — unified openEO + STAC response."""

from __future__ import annotations

import os
from typing import Any

from fastapi import HTTPException, Request

from climate_api.stac import services as stac_services


def _rewrite_collection_links(collection: dict[str, Any], request: Request) -> dict[str, Any]:
    """Replace /stac/collections links with /collections links."""
    base_url = _abs_base(request)
    links = collection.get("links", [])
    rewritten: list[dict[str, Any]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        href = link.get("href", "")
        if isinstance(href, str):
            href = href.replace(f"{base_url}/stac/collections", f"{base_url}/collections")
            href = href.replace("/stac/catalog.json", "/stac")
        rewritten.append({**link, "href": href})
    return {**collection, "links": rewritten}


def list_collections(request: Request) -> dict[str, Any]:
    """Return the openEO /collections response (openEO + STAC compatible)."""
    eligible = stac_services._eligible_artifacts_by_dataset()
    collections = []
    for dataset_id in eligible:
        try:
            col = stac_services.build_collection(dataset_id, request)
            col = _rewrite_collection_links(col, request)
            collections.append(col)
        except HTTPException:
            continue

    base_url = _abs_base(request)
    return {
        "collections": collections,
        "links": [
            {"rel": "self", "href": f"{base_url}/collections", "type": "application/json"},
            {"rel": "root", "href": f"{base_url}/", "type": "application/json"},
        ],
    }


def get_collection(dataset_id: str, request: Request) -> dict[str, Any]:
    """Return one openEO/STAC collection."""
    collection = stac_services.build_collection(dataset_id, request)
    return _rewrite_collection_links(collection, request)


def _abs_base(request: Request) -> str:
    base_url = os.getenv("CLIMATE_API_BASE_URL")
    if base_url:
        return base_url.rstrip("/")
    return str(request.base_url).rstrip("/")
