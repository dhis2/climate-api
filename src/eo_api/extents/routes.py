"""FastAPI routes for configured extent discovery."""

from fastapi import APIRouter

from eo_api.extents import services
from eo_api.extents.schemas import ExtentListResponse, ExtentRecord

router = APIRouter()


@router.get("", response_model=ExtentListResponse)
def list_extents() -> ExtentListResponse:
    """List configured extents for this EO API instance."""
    items = [_build_extent_record(extent) for extent in services.list_extents()]
    return ExtentListResponse(items=items)


@router.get("/{extent_id}", response_model=ExtentRecord)
def get_extent(extent_id: str) -> ExtentRecord:
    """Get one configured extent for this EO API instance."""
    return _build_extent_record(services.get_extent_or_404(extent_id))


def _build_extent_record(extent: dict[str, object]) -> ExtentRecord:
    bbox = extent.get("bbox")
    if not (isinstance(bbox, list) and len(bbox) == 4 and all(isinstance(value, int | float) for value in bbox)):
        raise ValueError(f"Invalid bbox in extent config for '{extent.get('id')}'")
    name = extent.get("name")
    description = extent.get("description")
    return ExtentRecord(
        extent_id=str(extent["id"]),
        name=name if isinstance(name, str) else None,
        description=description if isinstance(description, str) else None,
        bbox=(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])),
    )
