"""Template workflow specs (scaffold) for CHIRPS and WorldPop."""

from __future__ import annotations

from eo_api.integrations.orchestration.definitions import load_workflow_definition
from eo_api.integrations.orchestration.spec import WorkflowSpec


def chirps3_dhis2_template() -> WorkflowSpec:
    """Return scaffold CHIRPS3 workflow template from YAML definition."""
    return load_workflow_definition("chirps3-dhis2-template")


def worldpop_dhis2_template() -> WorkflowSpec:
    """Return scaffold WorldPop workflow template from YAML definition."""
    return load_workflow_definition("worldpop-dhis2-template")
