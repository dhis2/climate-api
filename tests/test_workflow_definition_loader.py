from eo_api.integrations.orchestration.definitions import available_workflow_definitions, load_workflow_definition


def test_workflow_definition_loader_lists_templates() -> None:
    ids = available_workflow_definitions()
    assert "chirps3-dhis2-template" in ids
    assert "worldpop-dhis2-template" in ids


def test_workflow_definition_loader_loads_chirps_template() -> None:
    spec = load_workflow_definition("chirps3-dhis2-template")
    assert spec.workflow_id == "chirps3-dhis2-template"
    assert [node.id for node in spec.nodes] == [
        "features",
        "download",
        "temporal_aggregation",
        "spatial_aggregation",
        "dhis2_payload_builder",
    ]
