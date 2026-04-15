# Implementation Status: Dataset Publication Slice

## Purpose

This note captures the current implementation state of the branch after the API consolidation around ingestions, datasets, extents, raw Zarr access, and pygeoapi publication.

It is intended to answer:

1. what the branch now exposes
2. what is intentionally internal
3. how the current pieces fit together
4. what remains to be refined

## Branch Direction

The branch now centers on one narrow vertical slice:

1. define dataset templates in the EO API registry
2. define configured extents for the EO API instance
3. ingest data into a managed dataset for one dataset template plus one extent
4. publish that managed dataset through `pygeoapi` under `/ogcapi`
5. expose native metadata under `/datasets` and raw Zarr access under `/zarr`

The public surface is intentionally small:

- `/ingestions`
- `/extents`
- `/datasets`
- `/zarr/{dataset_id}`
- `/sync/{dataset_id}`
- `/ogcapi/...`

## Main Code References

- [src/eo_api/main.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/main.py)
  - app assembly and router mounting
- [src/eo_api/ingestions/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/routes.py)
  - ingestion, dataset, zarr, and sync routes
- [src/eo_api/ingestions/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/services.py)
  - internal artifact persistence, dataset grouping, sync behavior, Zarr browsing
- [src/eo_api/ingestions/schemas.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/schemas.py)
  - public ingestion, dataset, and sync contracts
- [src/eo_api/extents/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/extents/routes.py)
  - extent discovery endpoints
- [src/eo_api/extents/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/extents/services.py)
  - YAML-backed extent registry
- [src/eo_api/publications/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/publications/services.py)
  - pygeoapi publication and stable managed dataset id logic
- [data/extents.yaml](/home/abyot/coding/EO/eo-api-pygeoapi-publication/data/extents.yaml)
  - configured extents for the EO API instance

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

The route resolves `extent_id` inside EO API and then calls the downloader with concrete spatial inputs.

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

### 6. pygeoapi remains the only public collection surface

The branch no longer maintains a native `/collections` API.

Published datasets are exposed through:

- `/ogcapi/collections`
- `/ogcapi/collections/{dataset_id}`
- `/ogcapi/collections/{dataset_id}/coverage`

From the native FastAPI side, dataset responses include publication state and links to the OGC collection, but the collection resource itself is only public under `/ogcapi`.

### 7. Internal artifacts still exist as a storage/provenance model

The branch still persists internal artifact records in `data/artifacts/records.json`.

Those internal records retain:

- exact request scope
- stored format
- creation time
- publication mapping
- deduplication and sync history inputs

This internal model remains necessary for provenance and sync behavior, but it is no longer a public API concept.

The current JSON-backed store is still an interim persistence layer. Record mutations now use file locking to avoid lost updates during concurrent writes, but the long-term direction should be a proper transactional store.

## How The Current Flow Works

### Ingestion

1. client submits `dataset_id`, `start`, optional `end`, and optional `extent_id`
2. EO API resolves the dataset template from the registry
3. EO API resolves `extent_id` to a concrete bbox or other configured spatial input
4. EO API checks for an existing matching internal artifact
5. if needed, EO API downloads the source data
6. EO API prefers Zarr materialization and falls back to NetCDF when needed
7. EO API computes realized coverage metadata
8. EO API stores an internal artifact record
9. if `publish=true`, EO API publishes the dataset through pygeoapi
10. the route returns the public managed dataset view

### Dataset publication

1. publication derives a stable managed dataset id
2. pygeoapi resources are regenerated from published internal artifacts
3. the mounted pygeoapi sub-application is refreshed in process
4. the dataset becomes available immediately under `/ogcapi/collections/{dataset_id}`

### Raw data access

1. `/datasets/{dataset_id}` exposes native metadata and version summary
2. `/zarr/{dataset_id}` exposes the raw Zarr store layout when the latest version is Zarr-backed
3. `/ogcapi/collections/{dataset_id}/coverage` exposes standards-facing coverage access

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

### Standards-facing

- `GET /ogcapi/collections`
- `GET /ogcapi/collections/{dataset_id}`
- `GET /ogcapi/collections/{dataset_id}/coverage`

## What Is Still Deferred

1. direct provider-specific `/sync` availability discovery beyond template-metadata clamping
2. a final decision on how much version history to expose publicly
3. richer extent configuration shapes beyond `id + bbox + optional metadata`
4. any runtime write API for extents
5. multi-version publication resolution behind one dataset id

Recent refinement:

- `/sync/{dataset_id}` now rematerializes a new managed dataset version from the dataset's original request start through the requested sync end period, instead of creating a latest-only delta slice
- `/sync/{dataset_id}` now delegates to `sync_engine.plan_sync(...)` and `sync_engine.run_sync(...)`, keyed by dataset-template `sync_kind`
- sync responses now include `message` and structured `sync_detail`
- sync now re-resolves `extent_id` configuration so extent-backed `country_code` inputs continue to work during sync

## Short Summary

The branch now presents a much cleaner product story:

1. run ingestions through `/ingestions` as an execution and admin surface
2. return datasets, not artifacts
3. discover managed data under `/datasets`
4. access raw Zarr under `/zarr/{dataset_id}`
5. browse published collections only under `/ogcapi`

Internal artifacts still exist, but only as a storage and provenance model.
