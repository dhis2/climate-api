import pytest

from eo_api.integrations.orchestration.executor import execute_workflow_spec
from eo_api.integrations.orchestration.registry import (
    ComponentDescriptor,
    ComponentRegistry,
    build_default_component_registry,
)
from eo_api.integrations.orchestration.spec import WorkflowNodeSpec, WorkflowSpec
from eo_api.integrations.orchestration.templates import chirps3_dhis2_template, worldpop_dhis2_template


def test_workflow_spec_rejects_duplicate_node_ids() -> None:
    with pytest.raises(ValueError, match="Duplicate workflow node ids"):
        WorkflowSpec(
            workflow_id="dup",
            nodes=[
                WorkflowNodeSpec(id="a", component="x"),
                WorkflowNodeSpec(id="a", component="y"),
            ],
        )


def test_default_registry_contains_core_components() -> None:
    registry = build_default_component_registry()
    ids = registry.list_ids()
    assert "workflow.features" in ids
    assert "workflow.download" in ids
    assert "workflow.temporal_aggregation" in ids
    assert "workflow.spatial_aggregation" in ids
    assert "workflow.dhis2_payload_builder" in ids


def test_execute_workflow_spec_resolves_refs() -> None:
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="mock.a",
            description="a",
            fn=lambda params, context: {"value": params["x"] + context["base"]},
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="mock.b",
            description="b",
            fn=lambda params, context: {"value": params["from_a"] * 2 + context["base"]},
        )
    )

    spec = WorkflowSpec(
        workflow_id="demo",
        nodes=[
            WorkflowNodeSpec(id="a", component="mock.a", params={"x": "{{input.seed}}"}),
            WorkflowNodeSpec(id="b", component="mock.b", params={"from_a": "{{a.value}}"}),
        ],
    )
    result = execute_workflow_spec(spec=spec, registry=registry, context={"base": 3, "input": {"seed": 4}})

    assert result["status"] == "completed"
    assert result["outputs"]["a"]["value"] == 7
    assert result["outputs"]["b"]["value"] == 17
    assert len(result["workflowTrace"]) == 2


def test_templates_present_for_chirps_and_worldpop() -> None:
    chirps = chirps3_dhis2_template()
    worldpop = worldpop_dhis2_template()
    assert chirps.workflow_id == "chirps3-dhis2-template"
    assert worldpop.workflow_id == "worldpop-dhis2-template"
    assert [node.id for node in chirps.nodes] == [
        "features",
        "download",
        "temporal_aggregation",
        "spatial_aggregation",
        "dhis2_payload_builder",
    ]
    assert [node.component for node in chirps.nodes] == [
        "workflow.features",
        "workflow.download",
        "workflow.temporal_aggregation",
        "workflow.spatial_aggregation",
        "workflow.dhis2_payload_builder",
    ]
    assert [node.id for node in worldpop.nodes] == [
        "features",
        "download",
        "temporal_aggregation",
        "spatial_aggregation",
        "dhis2_payload_builder",
    ]


def test_execute_workflow_spec_supports_step_passthrough() -> None:
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="mock.pass",
            description="pass",
            fn=lambda params, context: {
                "value": params["x"],
                "_step_control": {"action": "pass_through", "reason": "already aggregated"},
            },
        )
    )

    spec = WorkflowSpec(workflow_id="pass", nodes=[WorkflowNodeSpec(id="p", component="mock.pass", params={"x": 5})])
    result = execute_workflow_spec(spec=spec, registry=registry, context={})
    assert result["status"] == "completed"
    assert result["outputs"]["p"]["value"] == 5
    assert result["workflowTrace"][0]["status"] == "passed_through"
    assert result["workflowTrace"][0]["action"] == "pass_through"


def test_execute_workflow_spec_supports_step_exit() -> None:
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="mock.exit",
            description="exit",
            fn=lambda params, context: {
                "partial": True,
                "_step_control": {"action": "exit", "reason": "cannot disaggregate yearly to monthly"},
            },
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="mock.never",
            description="never",
            fn=lambda params, context: {"ran": True},
        )
    )

    spec = WorkflowSpec(
        workflow_id="exit",
        nodes=[
            WorkflowNodeSpec(id="a", component="mock.exit"),
            WorkflowNodeSpec(id="b", component="mock.never"),
        ],
    )
    result = execute_workflow_spec(spec=spec, registry=registry, context={})
    assert result["status"] == "exited"
    assert "b" not in result["outputs"]
    assert result["exit"]["step"] == "a"
    assert "cannot disaggregate" in result["exit"]["reason"]
