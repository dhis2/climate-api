"""CHIRPS3 DHIS2 payload adapter."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.dhis2_datavalues_service import build_data_value_set


def run(params: dict[str, Any]) -> dict[str, Any]:
    """Build DHIS2 payload for CHIRPS rows."""
    result = build_data_value_set(
        rows=params["rows"],
        data_element=params["data_element"],
        category_option_combo=params.get("category_option_combo"),
        attribute_option_combo=params.get("attribute_option_combo"),
        data_set=params.get("data_set"),
    )
    return {"dataValueSet": result["dataValueSet"], "table": result["table"]}
