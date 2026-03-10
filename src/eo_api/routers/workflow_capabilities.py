"""Discoverability endpoints for generic workflow capabilities."""

from __future__ import annotations

from fastapi import APIRouter

from eo_api.integrations.orchestration.capabilities import build_generic_workflow_capabilities_document

router = APIRouter(tags=["Workflow Capabilities"])


@router.get(
    "/ogcapi/processes/generic-dhis2-workflow/capabilities",
    summary="Generic workflow capabilities",
)
def generic_workflow_capabilities() -> dict:
    """Return dataset/service discoverability document for generic workflow."""
    return build_generic_workflow_capabilities_document()
