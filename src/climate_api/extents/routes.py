"""FastAPI routes for configured extent discovery."""

from fastapi import APIRouter, HTTPException

from climate_api.extents import services
from climate_api.extents.schemas import ExtentRecord

router = APIRouter()


@router.get("", response_model=ExtentRecord)
def get_extent() -> ExtentRecord:
    """Return the configured extent for this Climate API instance."""
    extent = services.get_extent()
    if extent is None:
        raise HTTPException(status_code=404, detail="No extent configured")
    return _build_extent_record(extent)


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
