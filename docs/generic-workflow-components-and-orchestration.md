# Generic DHIS2 Workflow: Components and Orchestration

This document explains the architecture behind `generic-dhis2-workflow` from two angles:

1. `components`: domain work units
2. `orchestration`: pipeline assembly and execution

## High-level execution path

`generic-dhis2-workflow` runs a canonical chain:

1. `workflow.features`
2. `workflow.download`
3. `workflow.temporal_aggregation`
4. `workflow.spatial_aggregation`
5. `workflow.dhis2_payload_builder`

The chain is declared in YAML workflow definitions and executed by orchestration runtime.

## Components Angle

Component modules live in:

- `src/eo_api/integrations/components/`

Each component follows a three-layer convention:

1. Wrapper (`*_component.py`): orchestration-facing function signature `fn(params, context) -> dict`
2. Adapter (`adapters/<stage>/<dataset>.py`): dataset/stage translation layer from service contract to workflow stage contract
3. Service (`services/*_service.py`): domain/integration logic (download APIs, aggregation, DHIS2 payload formatting)

Step-control semantics (`executed`, `pass_through`, `exit`) are emitted by adapters/services and interpreted in orchestration runtime.

Adapter rationale:

1. Components are generic and stable for all datasets.
2. Adapters isolate dataset churn from components.
3. Adapters translate service-native results into workflow-native outputs and `_step_control`.

### Naming convention

- Wrapper modules:
  - `feature_component.py`
  - `download_component.py`
  - `temporal_aggregation_component.py`
  - `spatial_aggregation_component.py`
  - `dhis2_payload_builder_component.py`
- Domain service modules:
  - `*_service.py`
- Dataset adapter modules:
  - `adapters/<stage>/<dataset>.py`

### 1) Features component

- Wrapper:
  - `src/eo_api/integrations/components/feature_component.py`
- Service:
  - `src/eo_api/integrations/components/services/feature_resolver_service.py`
- External integration adapter used by service:
  - `src/eo_api/integrations/dhis2_adapter.py`

Responsibilities:

1. Resolve feature scope from inline GeoJSON or DHIS2 selectors.
2. Normalize to canonical feature rows:
   - `{"orgUnit": ..., "geometry": ...}`
3. Compute/resolve `effective_bbox`.
4. Emit a `feature_collection` for downstream consumers (e.g. WorldPop payload builder).

Why both wrapper and service:

1. `feature_component.py` is orchestration-facing contract wrapper.
2. `feature_resolver_service.py` contains domain-specific feature logic and DHIS2 resolution.
3. This keeps orchestration concerns and domain concerns separated.

### 2) Download component

- Entrypoint:
  - `src/eo_api/integrations/components/download_component.py`
- Dataset adapters:
  - `.../components/adapters/download/chirps3.py`
  - `.../components/adapters/download/worldpop.py`
  - `.../components/adapters/download/era5.py`
- Services:
  - `.../components/services/chirps3_fetch_service.py`
  - `.../components/services/worldpop_fetch_service.py`
  - `.../components/services/era5_land_fetch_service.py`

Responsibilities:

1. Acquire local files for the requested dataset.
2. Emit canonical outputs like `files`, plus dataset-specific metadata.
3. Decide `pass_through` (e.g. provided raster files) or `exit` if download/sync cannot provide artifacts.

### 3) Temporal aggregation component

- Entrypoint:
  - `src/eo_api/integrations/components/temporal_aggregation_component.py`
- Dataset adapters:
  - `.../components/adapters/temporal/chirps3.py`
  - `.../components/adapters/temporal/worldpop.py`
  - `.../components/adapters/temporal/era5.py`
- Services:
  - `.../components/services/temporal_aggregate_service.py`

Responsibilities:

1. Apply temporal aggregation where meaningful.
2. `pass_through` if source granularity already matches request.
3. `exit` when requested temporal transform is unsupported (e.g. WorldPop yearly -> monthly disaggregation).

### 4) Spatial aggregation component

- Entrypoint:
  - `src/eo_api/integrations/components/spatial_aggregation_component.py`
- Dataset adapters:
  - `.../components/adapters/spatial/chirps3.py`
  - `.../components/adapters/spatial/worldpop.py`
  - `.../components/adapters/spatial/passthrough_files.py`

Responsibilities:

1. Apply/confirm spatial reduction stage.
2. Often pass-through where spatial work is already done upstream or deferred to payload stage.

### 5) DHIS2 payload builder component

- Entrypoint:
  - `src/eo_api/integrations/components/dhis2_payload_builder_component.py`
- Dataset adapters:
  - `.../components/adapters/payload/chirps3.py`
  - `.../components/adapters/payload/worldpop.py`
  - `.../components/adapters/payload/era5.py`
- Services:
  - `.../components/services/dhis2_datavalues_service.py`

Responsibilities:

1. Convert dataset outputs to canonical DHIS2 payload shape:
   - `dataValueSet`
   - `table` (inspection)
2. Emit `exit` where payload logic is not implemented for dataset.

## Capability Discoverability

Capabilities are published from:

- `src/eo_api/integrations/workflow_definitions/dataset_capabilities.yaml`

And exposed through:

- `GET /ogcapi/processes/generic-dhis2-workflow/capabilities`

Capability model is intentionally split:

1. `provider_capabilities`: what the external dataset ecosystem offers.
2. `integration_capabilities`: what current `dhis2eo/eo-api` path implements.

This avoids conflating roadmap/provider potential with current runtime support.

## Orchestration Angle

Orchestration modules live in:

- `src/eo_api/integrations/orchestration/`

### `spec.py`

Defines workflow graph schema:

1. `WorkflowSpec`
2. `WorkflowNodeSpec`

Validates structure (e.g. duplicate node IDs).

### `definitions.py`

Loads YAML workflow definitions from:

- `src/eo_api/integrations/workflow_definitions/*.yaml`

Validates loaded YAML into `WorkflowSpec`.

### `templates.py`

Named template entrypoints:

1. `chirps3_dhis2_template()`
2. `worldpop_dhis2_template()`

They return validated specs loaded from YAML.

### `registry.py`

Maps component IDs (`workflow.features`, etc.) to actual callables.

This is the central assembly point for component availability.

### `executor.py`

Executes workflow specs:

1. resolves `{{...}}` references
2. executes each node using runtime helpers
3. aggregates outputs and trace
4. supports early workflow exit semantics

### `runtime.py`

Cross-cutting execution concerns:

1. consistent logging/timing
2. error wrapping
3. trace entry creation
4. step-control interpretation:
   - `executed`
   - `pass_through`
   - `exit`

## Why this split is intentional

1. Components own domain behavior.
2. Orchestration owns composition and execution lifecycle.
3. Adding a new dataset is mostly adapter work, not orchestration rewrites.
4. Generic process remains stable while capabilities evolve per dataset.
