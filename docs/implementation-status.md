# Implementation Status: Dataset Publication Slice

## Purpose

This note captures the current implementation state of the branch after the API consolidation around ingestions, datasets, extents, raw Zarr access, STAC discovery, and pygeoapi publication.

It is intended to answer:

1. what the branch now exposes
2. what is intentionally internal
3. how the current pieces fit together
4. what remains to be refined

## Branch Direction

The branch now centers on one narrow vertical slice:

1. define dataset templates in the Climate API registry
2. define configured extents for the Climate API instance
3. ingest data into a managed dataset for one dataset template plus one extent
4. publish that managed dataset through `pygeoapi` under `/ogcapi`
5. expose native metadata under `/datasets`, STAC discovery under `/stac`, and raw Zarr access under `/zarr`
6. sync existing managed datasets forward through `/sync`

The public surface is intentionally small:

- `/ingestions`
- `/extents`
- `/datasets`
- `/stac/...`
- `/zarr/{dataset_id}`
- `/sync/{dataset_id}`
- `/ogcapi/...`

## Main Code References

- [src/climate_api/main.py](../src/climate_api/main.py)
  - app assembly and router mounting
- [src/climate_api/ingestions/routes.py](../src/climate_api/ingestions/routes.py)
  - ingestion, dataset, zarr, and sync routes
- [src/climate_api/ingestions/services.py](../src/climate_api/ingestions/services.py)
  - internal artifact persistence, dataset grouping, sync service wiring, Zarr browsing
- [src/climate_api/ingestions/sync_engine.py](../src/climate_api/ingestions/sync_engine.py)
  - sync planning and execution engine
- [src/climate_api/ingestions/schemas.py](../src/climate_api/ingestions/schemas.py)
  - public ingestion, dataset, and sync contracts
- [src/climate_api/providers/availability.py](../src/climate_api/providers/availability.py)
  - provider-specific sync availability policies
- [src/climate_api/extents/routes.py](../src/climate_api/extents/routes.py)
  - extent discovery endpoints
- [src/climate_api/extents/services.py](../src/climate_api/extents/services.py)
  - YAML-backed extent registry
- [src/climate_api/publications/services.py](../src/climate_api/publications/services.py)
  - pygeoapi publication and stable managed dataset id logic
- [data/extents.yaml](../data/extents.yaml)
  - configured extents for the Climate API instance

## What Was Achieved

### 1. Public ingestion contract now uses `extent_id`

`POST /ingestions` now takes:

- `dataset_id`
- `start`
- `end`
- `extent_id`
- `overwrite`
- `prefer_zarr`
- `publish`

Raw `bbox` and `country_code` are no longer part of the public ingestion payload.

The route resolves `extent_id` inside Climate API and then calls the downloader with concrete spatial inputs.

### 2. Public ingestion responses now return datasets, not artifacts

`POST /ingestions`, `GET /ingestions`, and `GET /ingestions/{ingestion_id}` now define the operational ingestion surface.

`POST /ingestions` and `GET /ingestions/{ingestion_id}` return:

- `ingestion_id`
- `status`
- `dataset`

The `dataset` field uses the public dataset summary model from `/datasets`, not the full dataset detail view with version history.

Internal artifact records still exist, but they no longer define the public response story.

`GET /ingestions` lists ingestion run records for admin and operational use. `/datasets` remains the canonical managed-data surface for consumers.

### 3. Extents are now a first-class read-only part of the native API

The branch exposes:

- `GET /extents`
- `GET /extents/{extent_id}`

Extents are configured in YAML and currently include:

- `extent_id`
- `name`
- `description`
- `bbox`

This keeps spatial configuration explicit without turning it into a runtime write API.

### 4. `/datasets` is now the native managed-data catalog

`GET /datasets` returns a public dataset catalog envelope:

- `kind`
- `items`

Each dataset item includes:

- public dataset id
- source dataset template id
- dataset metadata from the registry
- current extent
- last updated timestamp
- public links
- publication status

The public dataset response no longer exposes internal artifact ids, artifact counts, filesystem paths, or downloader implementation details.

### 5. Raw Zarr access is now canonical under `/zarr/{dataset_id}`

The raw data surface is:

- `GET /zarr/{dataset_id}`
- `GET /zarr/{dataset_id}/{relative_path}`

The public Zarr listing response now avoids leaking internal artifact ids and raw filesystem roots. It returns:

- `kind`
- `dataset_id`
- `path`
- `entries`

Entry links point back into the canonical `/zarr/{dataset_id}/...` namespace.

### 6. STAC is now the public discovery surface for published Zarr datasets

The branch exposes a dedicated STAC surface under:

- `/stac`
- `/stac/catalog.json`
- `/stac/collections/{dataset_id}`

Published Zarr-backed managed datasets appear there as one STAC Collection per dataset. The `zarr` asset points to the canonical native `/zarr/{dataset_id}` route.

`xstac` derives Datacube metadata from the opened Zarr-backed dataset, while the Climate API service layer remains responsible for publication filtering, link construction, and Zarr asset metadata.

Current STAC details:

- multiscale datasets can expose `/zarr/{dataset_id}/0` as the canonical asset href when declared in template metadata
- temporal extents are normalized to RFC 3339 in both STAC and Datacube temporal extent fields
- STAC collection `license` currently defaults to `various`
- spatial `step` values are rounded for readability while preserving axis direction
- an opt-in live interoperability smoke test exists at `tests/integration/test_stac_interop.py`

### 7. pygeoapi remains the OGC query and coverage surface

Published datasets are exposed through:

- `/ogcapi/collections`
- `/ogcapi/collections/{dataset_id}`
- `/ogcapi/collections/{dataset_id}/coverage`

From the native FastAPI side, dataset responses include publication state and links to the OGC collection, but the collection resource itself is only public under `/ogcapi`.

### 8. Internal artifacts still exist as a storage/provenance model

The branch still persists internal artifact records in `data/artifacts/records.json`.

Those internal records retain:

- exact request scope
- stored format
- creation time
- publication mapping
- deduplication and sync history inputs

This internal model remains necessary for provenance and sync behavior, but it is no longer a public API concept.

The current JSON-backed store is still an interim persistence layer. Record mutations now use file locking to avoid lost updates during concurrent writes, but the long-term direction should be a proper transactional store.

### 9. `/sync` is now a testable managed dataset update path

The sync API now exposes:

- `GET /sync/{dataset_id}/plan?end={period}`
- `POST /sync/{dataset_id}`

The plan endpoint returns a dry-run `SyncDetail` without downloading or writing data. The post endpoint executes the same plan through the existing artifact creation path when work is required.

Implemented sync behavior:

- temporal datasets can append missing periods
- release datasets rematerialize when a newer requested release exists
- static datasets return `not_syncable`
- provider availability policies clamp unsafe future targets before execution
- append V1 downloads only the missing range, then rebuilds the canonical artifact from local cache
- Zarr materialization clips cached upstream data to the requested artifact scope
- artifact reuse ignores records whose stored coverage does not match the requested scope
- newly materialized artifacts are rejected when realized temporal coverage does not match the requested scope

## How The Current Flow Works

### Ingestion

1. client submits `dataset_id`, `start`, optional `end`, and optional `extent_id`
2. Climate API resolves the dataset template from the registry
3. Climate API resolves `extent_id` to a concrete bbox or other configured spatial input
4. Climate API checks for an existing matching internal artifact
5. if needed, Climate API downloads the source data
6. Climate API prefers Zarr materialization and falls back to NetCDF when needed
7. Climate API computes realized coverage metadata
8. Climate API stores an internal artifact record
9. if `publish=true`, Climate API publishes the dataset through pygeoapi
10. the route returns the public managed dataset view

### Dataset publication

1. publication derives a stable managed dataset id
2. pygeoapi resources are regenerated from published internal artifacts
3. STAC collection documents are derived dynamically from the same published artifact state
4. the mounted pygeoapi sub-application is refreshed in process
5. the dataset becomes available immediately under `/stac/collections/{dataset_id}` and `/ogcapi/collections/{dataset_id}`

### Raw data access

1. `/datasets/{dataset_id}` exposes native metadata and version summary
2. `/stac/collections/{dataset_id}` exposes standards-friendly discovery metadata for direct Zarr-opening clients
3. `/zarr/{dataset_id}` exposes the raw Zarr store layout when the latest version is Zarr-backed
4. `/ogcapi/collections/{dataset_id}/coverage` exposes standards-facing coverage access

### Sync

1. `GET /sync/{dataset_id}/plan` resolves the latest local artifact and source template
2. `sync_engine.plan_sync(...)` computes the action, target, and delta range
3. provider availability metadata clamps unsupported future targets
4. `POST /sync/{dataset_id}` returns `up_to_date` or `not_syncable` without writes when applicable
5. otherwise, sync calls the existing artifact creation path
6. the new version is optionally published under the same stable managed dataset id

## Current Public Surface

### Native FastAPI

- `POST /ingestions`
- `GET /ingestions`
- `GET /ingestions/{ingestion_id}`
- `GET /extents`
- `GET /extents/{extent_id}`
- `GET /datasets`
- `GET /datasets/{dataset_id}`
- `GET /datasets/{dataset_id}/download`
- `GET /zarr/{dataset_id}`
- `GET /zarr/{dataset_id}/{relative_path}`
- `POST /sync/{dataset_id}`
- `GET /sync/{dataset_id}/plan`

### Standards-facing

- `GET /stac`
- `GET /stac/catalog.json`
- `GET /stac/collections/{dataset_id}`
- `GET /ogcapi/collections`
- `GET /ogcapi/collections/{dataset_id}`
- `GET /ogcapi/collections/{dataset_id}/coverage`

## What Is Still Deferred

1. a final decision on how much version history to expose publicly
2. richer extent configuration shapes beyond `id + bbox + optional metadata`
3. any runtime write API for extents
4. multi-version publication resolution behind one dataset id
5. true in-place Zarr append, if storage semantics require it later
6. upstream `dhis2eo` improvements so provider download boundaries can respect partial months directly

## Short Summary

The branch now presents a much cleaner product story:

1. run ingestions through `/ingestions` as an execution and admin surface
2. return datasets, not artifacts
3. discover managed data under `/datasets`
4. discover published Zarr-backed datasets under `/stac/catalog.json`
5. access raw Zarr under `/zarr/{dataset_id}`
6. sync managed datasets through `/sync/{dataset_id}`
7. use `/ogcapi` for standards-facing query and coverage access

Internal artifacts still exist, but only as a storage and provenance model.
