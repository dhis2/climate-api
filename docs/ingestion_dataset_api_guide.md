# Climate API Ingestion and Dataset Guide

This guide describes the current native FastAPI surface for Climate API and how it relates to the standards-facing `pygeoapi` publication layer.

The current public story is:

- run and inspect ingestion operations with `/ingestions`
- discover configured extents with `/extents`
- discover managed datasets with `/datasets`
- access raw Zarr data with `/zarr/{dataset_id}`
- access standards-facing publication with `/ogcapi/...`

Internal artifacts still exist as a storage and provenance model, but they are not part of the public API contract.

Operational note:

- `/ingestions` is the execution and admin-facing surface for ingestion runs
- `/datasets` is the canonical managed-data surface for consumers

## Main Public Endpoints

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
- `GET /ogcapi/collections`
- `GET /ogcapi/collections/{dataset_id}`
- `GET /ogcapi/collections/{dataset_id}/coverage`

## 1. Discover configured extents

Configured extents are setup-time Climate API configuration. They are read-only at runtime and are identified by `extent_id`.

Example:

```bash
curl -s http://127.0.0.1:8000/extents | jq
```

Example response:

```json
{
  "kind": "ExtentList",
  "items": [
    {
      "extent_id": "sle",
      "name": "Sierra Leone",
      "description": "National extent for Sierra Leone.",
      "bbox": [-13.5, 6.9, -10.1, 10.0]
    }
  ]
}
```

What this means:

- `extent_id` is the public Climate API handle for a configured spatial extent
- `bbox` is the resolved spatial extent exposed publicly
- provider-specific hints may exist internally in extent config, but they are not part of the public extent response

## 2. Ingest a dataset

The public ingestion contract now takes:

- `dataset_id`
- `start`
- optional `end`
- optional `extent_id`
- `overwrite`
- `prefer_zarr`
- `publish`

Raw `bbox` and `country_code` are no longer part of the public ingestion payload.

### Example: CHIRPS3

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "extent_id": "sle",
    "overwrite": false,
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

### Example: WorldPop

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "worldpop_population_yearly",
    "start": "2020",
    "end": "2020",
    "extent_id": "sle",
    "overwrite": false,
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Example response:

```json
{
  "ingestion_id": "a7e06c93-ba78-4c74-b772-160927fdb463",
  "status": "completed",
  "dataset": {
    "dataset_id": "chirps3_precipitation_daily_sle",
    "source_dataset_id": "chirps3_precipitation_daily",
    "dataset_name": "Total precipitation (CHIRPS3)",
    "short_name": "Total precipitation",
    "variable": "precip",
    "period_type": "daily",
    "units": "mm",
    "resolution": "5 km x 5 km",
    "source": "CHIRPS v3",
    "source_url": "https://www.chc.ucsb.edu/data/chirps3",
    "extent": {
      "spatial": {
        "xmin": -13.52499751932919,
        "ymin": 6.92499920912087,
        "xmax": -10.124997468665242,
        "ymax": 10.02499925531447
      },
      "temporal": {
        "start": "2024-01-01",
        "end": "2024-01-31"
      }
    },
    "last_updated": "2026-04-01T09:03:28.691120Z",
    "links": [
      {
        "href": "/datasets/chirps3_precipitation_daily_sle",
        "rel": "self",
        "title": "Dataset detail"
      },
      {
        "href": "/zarr/chirps3_precipitation_daily_sle",
        "rel": "zarr",
        "title": "Zarr store"
      },
      {
        "href": "/ogcapi/collections/chirps3_precipitation_daily_sle",
        "rel": "ogc-collection",
        "title": "OGC collection"
      }
    ],
    "publication": {
      "status": "published",
      "published_at": "2026-04-01T09:03:28.692230Z"
    }
  }
}
```

What this means:

- `ingestion_id` is the handle for the ingestion event lookup route
- `status = "completed"` means this branch still treats ingestion synchronously
- `/ingestions` is an operational/admin surface, not the main managed-data catalog
- `dataset` is a public managed dataset summary, not an internal artifact record
- `extent` is realized data coverage, not just the configured bbox
- `links` point to the native dataset metadata, native Zarr access, and standards-facing OGC collection

## 3. List ingestion runs

`GET /ingestions` returns ingestion run records for operational and admin use.

Example:

```bash
curl -s http://127.0.0.1:8000/ingestions | jq
```

What this means:

- this route is for execution lookup and operational visibility
- it is not intended to replace `/datasets` as the primary data discovery surface
- items are ordered from most recent ingestion to oldest
## 4. Ingestion failure behavior

Ingestion should fail gracefully with a structured API error, not a raw 500 stack trace.

Current behavior:

- invalid or missing spatial/config inputs return `400`
- dataset/provider execution failures return `502`

Example cases:

- a provider requires a country code and the resolved extent config does not provide one
- a dataset requires a bbox and no bbox can be resolved
- the upstream provider fails at download time

Example error response:

```json
{
  "detail": "Upstream dataset download failed: provider timeout"
}
```

## 5. Discover managed datasets

`GET /datasets` is the native managed-data catalog and the main consumer-facing data surface.

Example:

```bash
curl -s http://127.0.0.1:8000/datasets | jq
```

Example response:

```json
{
  "kind": "DatasetList",
  "items": [
    {
      "dataset_id": "chirps3_precipitation_daily_sle",
      "source_dataset_id": "chirps3_precipitation_daily",
      "dataset_name": "Total precipitation (CHIRPS3)",
      "short_name": "Total precipitation",
      "variable": "precip",
      "period_type": "daily",
      "units": "mm",
      "resolution": "5 km x 5 km",
      "source": "CHIRPS v3",
      "source_url": "https://www.chc.ucsb.edu/data/chirps3",
      "extent": {
        "spatial": {
          "xmin": -13.52499751932919,
          "ymin": 6.92499920912087,
          "xmax": -10.124997468665242,
          "ymax": 10.02499925531447
        },
        "temporal": {
          "start": "2024-01-01",
          "end": "2024-01-31"
        }
      },
      "last_updated": "2026-04-01T09:03:28.691120Z",
      "links": [
        {
          "href": "/datasets/chirps3_precipitation_daily_sle",
          "rel": "self",
          "title": "Dataset detail"
        },
        {
          "href": "/zarr/chirps3_precipitation_daily_sle",
          "rel": "zarr",
          "title": "Zarr store"
        },
        {
          "href": "/ogcapi/collections/chirps3_precipitation_daily_sle",
          "rel": "ogc-collection",
          "title": "OGC collection"
        }
      ],
      "publication": {
        "status": "published",
        "published_at": "2026-04-01T09:03:28.692230Z"
      }
    }
  ]
}
```

What this means:

- `/datasets` is the public native catalog of managed datasets
- `items` is wrapped in a `kind` envelope for consistency and self-description
- dataset items contain public metadata and access links only
- internal artifact ids, filesystem paths, and downloader implementation details are intentionally omitted

## 6. Get dataset detail

`GET /datasets/{dataset_id}` returns the full managed dataset detail view.

Example:

```bash
curl -s http://127.0.0.1:8000/datasets/chirps3_precipitation_daily_sle | jq
```

What this adds beyond the list response:

- full dataset metadata
- publication summary
- slim `versions` history derived from internal records

The detailed dataset response is where version history belongs. The ingestion response stays as a summary.

## 7. Access raw Zarr data

If the latest managed dataset version is Zarr-backed, the canonical native raw-data route is `/zarr/{dataset_id}`.

Examples:

```bash
curl -s http://127.0.0.1:8000/zarr/chirps3_precipitation_daily_sle | jq
curl -s http://127.0.0.1:8000/zarr/chirps3_precipitation_daily_sle/zarr.json | jq
```

The listing response exposes:

- `kind`
- `dataset_id`
- `format`
- `path`
- `entries`

What this means:

- `/zarr/{dataset_id}` is for raw native data access
- dataset metadata remains under `/datasets`
- entry links stay inside the canonical `/zarr/{dataset_id}/...` namespace
- internal artifact ids and local filesystem roots are not exposed

## 8. Access published OGC collections

Published datasets are exposed only through `/ogcapi`.

Examples:

```bash
curl -s "http://127.0.0.1:8000/ogcapi/collections?f=json" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily_sle?f=json" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily_sle/coverage?f=json" | jq
```

What this means:

- `/ogcapi` is the only public collection surface
- native FastAPI no longer exposes `/collections`
- dataset responses include links to `/ogcapi/collections/{dataset_id}`, but the collection resource itself lives only under `pygeoapi`

## 9. `/sync`

`/sync` advances an existing managed dataset from its latest local coverage toward a requested upstream period.

Available operations:

- `GET /sync/{dataset_id}/plan?end={period}` returns the planned sync action without downloading or writing data
- `POST /sync/{dataset_id}` executes the plan when a new version is needed

Implemented behavior:

- `temporal` datasets compare the next missing period with the requested or metadata-clamped latest period
- `release` datasets compare the current materialized release with the requested or metadata-clamped latest release
- `static` datasets return `not_syncable`
- preserve stable managed dataset identity
- use template-level `sync_execution`
- `append` execution downloads only the missing period range, then rebuilds the canonical artifact from the local cache
- `rematerialize` execution downloads the full original request range through the requested end period
- return the updated dataset view plus structured `sync_detail`

Current limitations:

- append execution is a delta-download plus canonical rebuild, not in-place Zarr mutation
- upstream availability can use a provider-specific `sync_availability.latest_available_function`; otherwise it falls back to template metadata such as lag days

Example dry-run plan:

```bash
curl -s "http://127.0.0.1:8000/sync/chirps3_precipitation_daily_sle/plan?end=2024-02-10" | jq
```

Example execution:

```bash
curl -s -X POST "http://127.0.0.1:8000/sync/chirps3_precipitation_daily_sle" \
  -H "Content-Type: application/json" \
  -d '{"end":"2024-02-10","prefer_zarr":true,"publish":true}' | jq
```

## Manual Test Sequence

These commands assume the API is running on `http://127.0.0.1:8000` and that
`jq` is available.

### 1. Confirm configured extents

```bash
curl -s "http://127.0.0.1:8000/extents" | jq
```

Use an extent that has enough spatial metadata for the selected dataset. The
examples below use `sle`.

### 2. Create an initial CHIRPS3 managed dataset

```bash
curl -s -X POST "http://127.0.0.1:8000/ingestions" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "extent_id": "sle",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Expected:

- `status` is `completed`
- `dataset.dataset_id` is `chirps3_precipitation_daily_sle`
- `dataset.extent.temporal.end` is `2024-01-31`

### 3. Inspect the managed dataset and publication

```bash
curl -s "http://127.0.0.1:8000/datasets/chirps3_precipitation_daily_sle" | jq
curl -s "http://127.0.0.1:8000/zarr/chirps3_precipitation_daily_sle" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily_sle?f=json" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily_sle/coverage?f=json" | jq
```

### 4. Dry-run a CHIRPS3 append sync

```bash
curl -s "http://127.0.0.1:8000/sync/chirps3_precipitation_daily_sle/plan?end=2024-02-10" | jq
```

Expected planning response:

- `sync_kind` is `temporal`
- `action` is `append`
- `reason` is `new_periods_available_for_append`
- `message` explains that existing data is present and which missing period range will be downloaded
- `current_start` is `2024-01-01`
- `current_end` is `2024-01-31`
- `target_end` is `2024-02-10`
- `target_end_source` is `request`
- `delta_start` is `2024-02-01`
- `delta_end` is `2024-02-10`

`append` here means Climate API downloads only the missing period range and then
rebuilds the canonical artifact from local cache. It is not in-place Zarr mutation.

Where these timestamps come from:

- `current_start` and `current_end` come from the latest stored artifact coverage
- `target_end` comes from the explicit `end` query parameter, or defaults to today in the dataset-native period format when omitted
- `target_end_source` tells you whether `target_end` came from `request`, `default_today`, or `current_coverage`
- `delta_start` is the first period after `current_end`
- `delta_end` is the resolved target period after any availability clamping

If `end` is omitted, the planner defaults to the current date. For example, calling
`/sync/chirps3_precipitation_daily_sle/plan` on `2026-04-20` after ingesting
through `2024-01-31` plans an append from `2024-02-01` through `2026-04-20`.
In that response, `target_end_source` will be `default_today`. For controlled
tests, always pass an explicit `end`.

### 5. Execute the CHIRPS3 sync

```bash
curl -s -X POST "http://127.0.0.1:8000/sync/chirps3_precipitation_daily_sle" \
  -H "Content-Type: application/json" \
  -d '{
    "end": "2024-02-10",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Expected:

- `status` is `completed`
- `sync_detail.action` is `append`
- `sync_detail.current_end` was `2024-01-31`
- `sync_detail.delta_start` is `2024-02-01`
- `sync_detail.delta_end` is `2024-02-10`
- `sync_detail.target_end` is `2024-02-10`
- the returned `dataset.dataset_id` is still `chirps3_precipitation_daily_sle`
- the returned dataset has a newer version in `versions`

### 6. Confirm no-op behavior

Run the same plan again with the same end:

```bash
curl -s "http://127.0.0.1:8000/sync/chirps3_precipitation_daily_sle/plan?end=2024-02-10" | jq
```

Expected:

- `action` is `no_op`
- `reason` is `no_new_period`
- `message` explains that the requested target is already covered locally

### 7. Test release-style sync with WorldPop

Create an initial WorldPop managed dataset:

```bash
curl -s -X POST "http://127.0.0.1:8000/ingestions" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "worldpop_population_yearly",
    "extent_id": "sle",
    "start": "2020",
    "end": "2020",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Plan a later release:

```bash
curl -s "http://127.0.0.1:8000/sync/worldpop_population_yearly_sle/plan?end=2021" | jq
```

Expected:

- `sync_kind` is `release`
- `action` is `rematerialize`
- `reason` is `new_release_available`
- `target_end` is `2021`

Execute the release sync:

```bash
curl -s -X POST "http://127.0.0.1:8000/sync/worldpop_population_yearly_sle" \
  -H "Content-Type: application/json" \
  -d '{
    "end": "2021",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Expected:

- `status` is `completed`
- `sync_detail.action` is `rematerialize`
- the managed dataset id remains `worldpop_population_yearly_sle`

## Summary

The current branch is no longer an artifact-first API.

The public contract is now:

- ingest with `/ingestions`
- discover extents with `/extents`
- discover managed datasets with `/datasets`
- access raw native data with `/zarr/{dataset_id}`
- access standards-facing publication with `/ogcapi`

Artifacts remain internal because Climate API still needs storage and provenance records behind ingestion and publication, but those internals are no longer exposed as first-class public resources.
