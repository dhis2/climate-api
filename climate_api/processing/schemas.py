"""Pydantic schemas for native process discovery endpoints."""

from pydantic import BaseModel, Field


class ProcessLink(BaseModel):
    """Hypermedia link for a process resource."""

    href: str
    rel: str
    title: str | None = None


class ProcessField(BaseModel):
    """Optional input/output field metadata for a process."""

    type: str | None = None
    required: bool | None = None
    description: str | None = None
    enum: list[str] | None = None


class ProcessSummary(BaseModel):
    """Public summary view of one registered process."""

    id: str
    title: str
    description: str | None = None
    version: str | None = None
    keywords: list[str] = Field(default_factory=list)
    jobControlOptions: list[str] = Field(default_factory=list)
    links: list[ProcessLink] = Field(default_factory=list)


class ProcessDetail(ProcessSummary):
    """Full public view of one registered process."""

    inputs: dict[str, ProcessField] = Field(default_factory=dict)
    outputs: dict[str, ProcessField] = Field(default_factory=dict)


class ProcessListResponse(BaseModel):
    """Envelope response for registered processes."""

    processes: list[ProcessSummary] = Field(default_factory=list)
    links: list[ProcessLink] = Field(default_factory=list)
