# Generic DHIS2 Workflow: Executive Brief (One Page)

## What We Built

We established a single, generic workflow architecture for gridded datasets (currently CHIRPS3 and WorldPop, with ERA5 path aligned for extension).

Canonical chain:

1. `features`
2. `download`
3. `temporal_aggregation`
4. `spatial_aggregation`
5. `dhis2_payload_builder`

## Why This Matters

1. One orchestration model across datasets instead of one workflow per dataset.
2. Clear separation of concerns:
   - orchestration: sequence + execution control
   - components: stage-level contracts
   - adapters: dataset-specific behavior
   - services: domain operations
3. Faster onboarding for new datasets with lower regression risk.

## Key Design Principles

1. Stage purity: each component does one job.
2. Capability-driven behavior: pass/aggregate/exit decisions come from source vs requested frequency, not dataset name.
3. Canonical intermediate contract: standardized rows (`orgUnit`, `period`, `value`) before DHIS2 payload formatting.
4. Traceability: each step reports status and timing (`completed`, `passed_through`, `exit`).

## Current Status

1. Architecture and naming have been cleaned and aligned.
2. WorldPop fetch is fetch-only (`worldpop_fetch_service`); aggregation is handled in aggregation services/components.
3. Aggregation concerns were split into:
   - `temporal_aggregate_service`
   - `spatial_aggregate_service`
4. Generic workflow validation is green:
   - lint/type checks pass
   - targeted workflow/component tests pass
5. Discoverability is in place:
   - `GET /ogcapi/processes/generic-dhis2-workflow/capabilities`
   - capability split: `provider_capabilities` vs `integration_capabilities`

## Strategic Outcome

We now have a generic workflow engine rather than dataset-specific orchestration pipelines. This gives us a scalable base to support climate, population, and disease-related gridded products behind one process endpoint and one architectural pattern.

## Next Step (Recommended)

Continue expanding dataset capability registration (YAML-based) for:

1. source temporal resolution
2. supported temporal targets
3. provider vs integration output-format support
4. reducer support
5. adapter bindings per stage

This will make new dataset onboarding configuration-first rather than code-first.
