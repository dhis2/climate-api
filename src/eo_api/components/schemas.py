"""Schemas for generic components."""

from __future__ import annotations

from typing import Any
from enum import StrEnum

from pydantic import BaseModel, Field, conlist


class ComponentDefinition(BaseModel):
    """Component metadata for discovery."""

    name: str
    version: str = "v1"
    description: str
    input_schema: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)


class ComponentCatalogResponse(BaseModel):
    """List of discoverable components."""

    components: list[ComponentDefinition]


# below is shared across components, maybe move elsewhere...

class PeriodType(StrEnum):
    """Supported temporal period types."""

    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"
