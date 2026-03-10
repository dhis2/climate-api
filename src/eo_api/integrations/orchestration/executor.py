"""Scaffold executor for component-based workflow specs."""

from __future__ import annotations

import re
from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.orchestration.registry import ComponentRegistry
from eo_api.integrations.orchestration.runtime import run_planned_component_with_trace, should_exit_workflow
from eo_api.integrations.orchestration.spec import WorkflowSpec

_REF_PATTERN = re.compile(r"^\{\{([a-zA-Z0-9_.-]+)\}\}$")


def _lookup_ref(path: str, namespace: dict[str, Any]) -> Any:
    cursor: Any = namespace
    for part in path.split("."):
        if isinstance(cursor, dict) and part in cursor:
            cursor = cursor[part]
        else:
            raise ProcessorExecuteError(f"Workflow reference '{path}' could not be resolved")
    return cursor


def _resolve_param_value(value: Any, namespace: dict[str, Any]) -> Any:
    if isinstance(value, str):
        match = _REF_PATTERN.match(value)
        if match:
            return _lookup_ref(match.group(1), namespace)
        return value
    if isinstance(value, list):
        return [_resolve_param_value(item, namespace) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_param_value(item, namespace) for key, item in value.items()}
    return value


def execute_workflow_spec(
    *,
    spec: WorkflowSpec,
    registry: ComponentRegistry,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute workflow nodes sequentially and return outputs with trace."""
    run_context = dict(context or {})
    outputs: dict[str, Any] = {}
    trace: list[dict[str, Any]] = []

    for node in spec.nodes:
        namespace = {**run_context, **outputs}
        resolved_params = _resolve_param_value(node.params, namespace)
        descriptor = registry.get(node.component)
        output = run_planned_component_with_trace(
            trace,
            step_name=node.id,
            fn=descriptor.fn,
            params=resolved_params,
            context=run_context,
        )
        outputs[node.id] = output
        exit_requested, reason = should_exit_workflow(output)
        if exit_requested:
            result: dict[str, Any] = {
                "workflowId": spec.workflow_id,
                "status": "exited",
                "outputs": outputs,
                "workflowTrace": trace,
                "exit": {"step": node.id},
            }
            if reason:
                result["exit"]["reason"] = reason
            return result

    return {
        "workflowId": spec.workflow_id,
        "status": "completed",
        "outputs": outputs,
        "workflowTrace": trace,
    }
