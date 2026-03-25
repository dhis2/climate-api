# Simple Pygeoapi Publication Plan

## Purpose

This branch resets the architecture around one narrow vertical slice:

1. download EO gridded data
2. persist it as a local artifact, preferring Zarr and falling back to NetCDF
3. publish that artifact as a pygeoapi collection
4. expose simple product-oriented access through FastAPI

The goal is to prove end-to-end ingestion and publication without reintroducing the generic workflow/component/publication platform from the parked branch.

## Decision

For this branch:

1. FastAPI is the main application entry point
2. pygeoapi is the primary publication engine
3. published data should exist as pygeoapi collections
4. FastAPI may expose convenience access to the same data, but should not build a second publication system

This is intentionally simpler than the parked `fastapi-generic-workflow` branch.

## Target Architecture

### FastAPI owns

1. ingestion requests
2. artifact creation
3. artifact metadata
4. lightweight publication registration
5. simple artifact access routes

### pygeoapi owns

1. collection registration target
2. collection metadata and standards-facing browse surface
3. provider-backed access to published EO resources

### Shared principle

One stored artifact may be reachable through:

1. FastAPI convenience routes for product and operational access
2. pygeoapi collection routes for OGC-facing access

## Scope

### Phase 1

1. one download endpoint
2. one artifact record model
3. one publication flow into pygeoapi collections
4. one FastAPI metadata/access surface over the same artifact

### Out of scope for now

1. generic workflow engine
2. generic component catalog
3. schedule contracts
4. native OGC process/job abstraction beyond what pygeoapi already provides
5. multi-surface publication policy machinery
6. broad registry or capability matrix abstractions

## Proposed API Shape

### FastAPI routes

1. `POST /downloads`
   - accepts dataset id and query parameters
   - may accept `bbox` and/or `country_code` depending on dataset needs
   - downloads source data
   - stores artifact metadata
   - optionally builds Zarr

2. `GET /downloads/{download_id}`
   - returns status and resulting artifact metadata

3. `GET /artifacts`
   - lists stored artifacts

4. `GET /artifacts/{artifact_id}`
   - returns artifact metadata

5. `GET /artifacts/{artifact_id}/download`
   - downloads the stored file when a single file is directly downloadable

6. `POST /artifacts/{artifact_id}/publish`
   - registers artifact as a pygeoapi collection

7. `GET /artifacts/{artifact_id}/publication`
   - returns publication status, collection id, and links

### OGC / pygeoapi routes

1. `/ogcapi/collections/{collection_id}`
2. `/ogcapi/collections/{collection_id}/coverage`
3. other provider-backed routes as supported by the chosen pygeoapi provider

## Data Model

Keep the model thin.

### Artifact record

Suggested fields:

1. `artifact_id`
2. `dataset_id`
3. `format`
   - `zarr` or `netcdf`
4. `path`
5. `variables`
6. `bbox`
7. `time_start`
8. `time_end`
9. `created_at`
10. `publication`
    - `status`
    - `collection_id`

This can start as a JSON-backed local store.

## Publication Flow

1. FastAPI creates the artifact
2. FastAPI derives minimal collection metadata from the artifact
3. FastAPI writes or updates pygeoapi resource configuration
4. pygeoapi exposes the collection
5. FastAPI stores the mapping between artifact id and collection id

The pygeoapi collection definition should be treated as the real published representation for this branch.

## Format Policy

1. Prefer Zarr for gridded EO data where chunked access is useful and tooling is stable
2. Fall back to NetCDF when the download path or provider support makes Zarr conversion impractical
3. Store the actual artifact format explicitly in metadata instead of pretending everything is Zarr

## FastAPI Access Role

FastAPI should remain an access point, but only in a thin way:

1. metadata lookup
2. direct artifact download where applicable
3. preview or lightweight inspection routes if needed
4. links to pygeoapi collection routes

FastAPI should not duplicate pygeoapi's collection and coverage semantics.

## Implementation Order

1. introduce artifact metadata storage
2. replace dataset-specific download route shape with a generic `POST /downloads`
3. add artifact lookup routes
4. add pygeoapi mount/config generation for published artifacts
5. add publish endpoint and collection mapping
6. document one end-to-end example using a single dataset

## Request Scope Rules

For gridded EO downloads, request scope should be explicit and simple:

1. prefer request-supplied `bbox` for raster datasets that support extent-based extraction
2. allow request-supplied `country_code` for country-scoped datasets such as WorldPop
3. allow `DOWNLOAD_BBOX` in `.env` as a bootstrap default when request scope is omitted
4. only fall back to DHIS2-derived bounds when neither request nor env scope is available

## Practical Guidance

When choosing between simplicity and abstraction on this branch:

1. prefer explicit code over generic orchestration
2. prefer one narrow happy path over a flexible platform surface
3. prefer pygeoapi-backed publication over native publication machinery
4. add abstractions only after two concrete datasets need them
