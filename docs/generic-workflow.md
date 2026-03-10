# Generic Workflow (Dataset-agnostic)

The `generic-dhis2-workflow` process executes a canonical component chain:

1. `workflow.features`
2. `workflow.download`
3. `workflow.temporal_aggregation`
4. `workflow.spatial_aggregation`
5. `workflow.dhis2_payload_builder`

Workflow templates are defined in YAML under:

- `src/eo_api/integrations/workflow_definitions/chirps3-dhis2-template.yaml`
- `src/eo_api/integrations/workflow_definitions/worldpop-dhis2-template.yaml`
- Dataset/service capability catalog:
  - `src/eo_api/integrations/workflow_definitions/dataset_capabilities.yaml`

Workflow orchestration loading/runtime lives under:

- `src/eo_api/integrations/orchestration/definitions.py`
- `src/eo_api/integrations/orchestration/templates.py`
- `src/eo_api/integrations/orchestration/registry.py`
- `src/eo_api/integrations/orchestration/executor.py`
- `src/eo_api/integrations/orchestration/runtime.py`

Discoverability endpoint:

- `GET /ogcapi/processes/generic-dhis2-workflow/capabilities`

## How To Plug A New Dataset

1. Add typed dataset input model in:
   - `src/eo_api/routers/ogcapi/plugins/processes/schemas.py`
   - Extend `GenericDhis2WorkflowInput` discriminated union (`dataset_type`).

2. Extend component behavior in:
   - `src/eo_api/integrations/components/`
   - Add dataset branch logic for `workflow.download`, `workflow.temporal_aggregation`, `workflow.spatial_aggregation`, and `workflow.dhis2_payload_builder`.

3. Register provider/integration capability metadata in:
   - `src/eo_api/integrations/workflow_definitions/dataset_capabilities.yaml`
   - Keep `provider_capabilities` and `integration_capabilities` separate.

4. Add YAML workflow definition:
   - `src/eo_api/integrations/workflow_definitions/<dataset>-dhis2-template.yaml`
   - Reuse canonical step IDs unless there is a strong reason to diverge.

5. Add template entrypoint function (optional convenience):
   - `src/eo_api/integrations/orchestration/templates.py`

6. Wire generic process branching:
   - `src/eo_api/routers/ogcapi/plugins/processes/generic_dhis2_workflow.py`
   - Map the new typed input branch to workflow template + input normalization.

7. Add tests:
   - Unit tests for model validation, component branching, and generic process execution path.

## Design Rules

- Keep reference workflows (`chirps3-dhis2-workflow`, `worldpop-dhis2-workflow`) unchanged as gold standards.
- Keep cross-cutting concerns in runtime helpers only.
- Use step decisions (`executed`, `pass_through`, `exit`) for capability-aware flow.
- Keep service contracts domain-native and adapter outputs workflow-native.

## Related architecture walkthrough

- `docs/generic-workflow-components-and-orchestration.md`
