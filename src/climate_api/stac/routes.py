"""Routes for the STAC catalogue."""

from fastapi import APIRouter, Request

from climate_api.stac import services
from climate_api.stac.schemas import StacCatalogResponse, StacCollectionResponse

router = APIRouter()


@router.get("", response_model=StacCatalogResponse)
def get_catalog(request: Request) -> dict[str, object]:
    """Return the STAC catalog root document."""
    return services.build_catalog(request)


@router.get("/catalog.json", response_model=StacCatalogResponse)
def get_catalog_json(request: Request) -> dict[str, object]:
    """Return the STAC catalog root document."""
    return services.build_catalog(request)


@router.get("/collections/{dataset_id}", response_model=StacCollectionResponse)
def get_collection(dataset_id: str, request: Request) -> dict[str, object]:
    """Return one STAC collection for a published Zarr-backed managed dataset."""
    return services.build_collection(dataset_id, request)
