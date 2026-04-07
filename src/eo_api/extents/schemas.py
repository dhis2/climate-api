"""Pydantic schemas for extent discovery APIs."""

from pydantic import BaseModel, Field


class ExtentRecord(BaseModel):
    """Configured spatial extent for this EO API instance."""

    extent_id: str = Field(description="Stable identifier for the configured extent.")
    name: str | None = Field(default=None, description="Human-readable name of the extent.")
    description: str | None = Field(default=None, description="Optional descriptive text for the extent.")
    bbox: tuple[float, float, float, float] = Field(
        description="Configured bounding box for the extent as xmin, ymin, xmax, ymax."
    )


class ExtentListResponse(BaseModel):
    """Envelope response for configured extents."""

    kind: str = Field(
        default="ExtentList",
        description="Self-describing envelope type for this collection response.",
        examples=["ExtentList"],
    )
    items: list[ExtentRecord] = Field(
        default_factory=list,
        description="Configured extents available in this EO API instance.",
        examples=[
            [
                {
                    "extent_id": "sle",
                    "name": "Sierra Leone",
                    "description": "National extent for Sierra Leone.",
                    "bbox": [-13.5, 6.9, -10.1, 10.0],
                }
            ]
        ],
    )
