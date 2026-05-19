# Architecture

This document explains how the Climate API is structured, why it is structured that way, and what the consequences are of each design decision. It is written for developers who will maintain or extend the platform over time.

---

## Core concepts

The platform has four first-class concepts. Understanding the distinction between them is the foundation for understanding everything else.

### Dataset template

A **template** is a YAML blueprint that describes a data source. Built-ins live in `climate_api/data/datasets/` inside the package (loaded via `importlib.resources`). Custom templates live in `{plugins_dir}/datasets/` where `plugins_dir` is set in `climate-api.yaml`. It has no state — it describes what _could_ be ingested, not what _has been_ ingested.

A template defines:

- the dataset identifier and display metadata
- the variable name, units, and period type
- how to ingest the data (`ingestion.plugin`)
- what transforms to apply (`transforms`)
- what sync strategy to use (`sync.kind`, `sync.execution`)

Templates are config, not code. If a template needs custom logic, the logic goes into a Python function referenced by dotted path from the YAML.

### Artifact

An **artifact** is the internal record of a completed data ingestion. It is the persistence layer — not a public API concept. Each ingestion produces exactly one artifact, which records:

- what dataset template it came from
- the exact spatial extent and time range that was materialized
- where the data lives on disk (path to the zarr store or netCDF files)
- when it was created
- whether it has been published

Multiple artifacts can exist for the same dataset template if data was ingested at different times (they form the version history). The most recent artifact for a given `dataset_id` is what the public API serves.

Artifacts are stored in `{data_dir}/artifacts/records.json`, where `data_dir` is the path configured in `climate-api.yaml`. This is an internal implementation detail — consumers should never depend on artifact IDs or artifact paths directly.

### Managed dataset

A **managed dataset** is the public-facing view of the most recent artifact for a given template. It is what `/datasets`, `/zarr`, `/stac`, and `/ogcapi` expose. When an operator ingests or syncs a dataset, the managed dataset view updates to reflect the new artifact — the public ID stays stable.

The relationship is: one template → many artifacts over time → one managed dataset (the latest).

### Extent

The **extent** is the spatial bounding box configured for this Climate API instance. It is set once in `climate-api.yaml` and does not change at runtime. Every ingestion is automatically scoped to this extent — operators do not specify it per-request.

This is a deliberate design constraint: each instance serves one place. A Sierra Leone instance serves Sierra Leone. Multi-country coverage requires multiple instances.

---

## Data lifecycle

All datasets are ingested through the plugin path (`ingestion.plugin`):

**Plugin path** (`ingestion.plugin`) — streams data directly into an Icechunk store:

```
Template (YAML)
    │
    │  POST /ingestions  (or  POST /sync)
    ▼
Orchestrator
    │  probe() → fix chunk shape, write GeoZarr attributes
    │  periods() → compare against committed store state
    │  for each pending period:
    │    fetch_period() → xr.Dataset (in source CRS)
    │    to_zarr(icechunk_store, append_dim="time")
    │    commit every commit_batch_size periods
    │  rechunk in-place (if rechunk_time is set)
    │  expire intermediate snapshots
    │  register ArtifactFormat.ICECHUNK artifact record
    │
    │  publish=true
    ▼
Managed dataset (public API)  — same endpoints as above
```

All ingest writes go directly to an Icechunk store — no intermediate files on disk. A crash leaves the store at the last committed period; restart resumes from there. The store is readable and serveable from the first committed period.

---

## Sync kinds

The `sync.kind` field in a template determines how a managed dataset is kept current.

| `sync.kind` | On each sync                            | Use when                                                          |
| ----------- | --------------------------------------- | ----------------------------------------------------------------- |
| `temporal`  | Append new time steps, or rematerialize | Historical record that grows over time (CHIRPS, ERA5-Land)        |
| `release`   | Rematerialize if a newer release exists | Versioned releases where each year/version is discrete (WorldPop) |
| `static`    | Never synced                            | One-time fixed dataset with no updates                            |

### The sync execution modes

Within `sync.kind: temporal`, two execution modes control what happens when new data is available:

- `append` — downloads only the missing time range and appends it to the existing artifact
- `rematerialize` — discards the existing artifact and rebuilds it from scratch

`append` is efficient for large historical datasets (avoid re-downloading years of data on each sync). `rematerialize` is appropriate when old data may change retroactively (e.g. reanalysis products that are corrected after the fact).

### Availability clamping

Providers publish data on a delay. The `sync.availability` block in a template tells the sync engine how far back from today data is reliably available:

```yaml
sync:
  kind: temporal
  execution: append
  availability:
    latest_available_function: climate_api.providers.availability.lagged_latest_available
    lag_hours: 120
```

Before executing a sync, the engine calls the availability function to clamp the target end date. This prevents requesting data that has not yet been published, which would leave temporal gaps.

---

## The plugin contract

The platform has four extension points. Each one has a narrow contract — the framework handles everything else automatically.

### Ingestion plugin

```python
class MyPlugin:
    max_concurrency: int = 1    # parallel fetch limit
    commit_batch_size: int = 1  # periods per Icechunk commit

    async def probe(self, bbox: list[float], **params) -> GridSpec:
        """Metadata-only probe — no data transfer."""
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        """Return the ordered list of period IDs available from start to end."""
        ...

    async def fetch_period(self, period_id: str, bbox: list[float], **params) -> xr.Dataset:
        """Fetch one period. Return a Dataset with a 'time' dimension in source CRS."""
        ...
```

The orchestrator calls `probe()` once, `periods()` once, then drives a bounded-concurrency fetch loop — writing each period directly to an Icechunk store and committing every `commit_batch_size` periods. Plugins never touch zarr or Icechunk directly.

See [Extensibility — Ingestion plugins](extensibility.md#ingestion-plugins) for the full protocol and `GridSpec` reference.

### Transform function

```python
def my_transform(ds: xr.Dataset, dataset: dict) -> xr.Dataset:
    # Receive the dataset after download, return a modified dataset.
    # Modify ds[dataset["variable"]] values and variable attributes.
    # Do not modify dataset-level ds.attrs — the framework manages those.
```

Transforms are applied in order after each period is fetched, before the data is written to the Icechunk store. They receive the full xarray Dataset and the template dict. They return a modified Dataset. They do not write to disk.

### Process execution function

```python
def execute(*, source_dataset_id: str, **kwargs) -> dict:
    # Run a named operation (e.g. temporal resampling).
    # Return a JSON-serialisable result dict.
```

Processes are named operations triggered via `POST /processes/{id}/execution`. They are broader than single-dataset transforms — they can read one managed dataset and produce another (e.g. daily → monthly aggregation).

---

## The transform pipeline

Transforms are applied at a consistent point in the ingestion lifecycle:

1. ingestion function writes raw NetCDF files to disk
2. framework reads and normalises the data into an xarray Dataset
3. `_run_transforms(ds, dataset)` applies each declared transform in order
4. result is reprojected to instance CRS
5. zarr store is written with auto-computed chunking
6. framework writes GeoZarr root attributes
7. framework computes coverage from the zarr

Transforms see post-download, pre-reproject data. They should only modify data values and variable-level attributes. The framework writes dataset-level attributes (GeoZarr) after the transform pipeline completes.

---

## GeoZarr root attributes

Every zarr artifact must have GeoZarr root attributes for map rendering to work correctly. These are written into `zarr.json` at the store root:

- `spatial:bbox` — `[xmin, ymin, xmax, ymax]` in the native CRS
- `proj:code` — the CRS EPSG code (e.g. `EPSG:32633` for UTM, `EPSG:4326` for WGS84)
- `zarr_conventions` — GeoZarr convention declaration

The map viewer reads `spatial:bbox` and `proj:code` to determine where to position tiles on the map.

**The framework writes these attributes — plugins do not.** They are written in `build_dataset_zarr` after transforms and reprojection, using the actual coordinate bounds of the final written data and the instance CRS.

---

## CRS handling

The instance CRS is configured in `climate-api.yaml`:

```yaml
extent:
  name: Norway
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

Downloaded data is reprojected from the source CRS (`source_crs` in the template, default `EPSG:4326`) to the instance CRS during ingestion. The stored zarr is always in the instance CRS.

If no `crs` is set in the config, data is stored in `EPSG:4326` (WGS84). This is the correct default for instances that do not need a metric CRS.

---

## Artifact deduplication and version history

When a new ingestion request arrives, the framework checks whether an existing artifact already covers the requested scope:

- same `dataset_id`
- same bbox (from the configured extent)
- overlapping time range

If a match exists and `overwrite=false`, the existing artifact is returned without re-downloading. If `overwrite=true`, the existing artifact is replaced.

The artifact store keeps the full history of records for sync deduplication and provenance. Old artifacts are not deleted automatically. For long-running instances, `records.json` grows over time. The long-term direction is a proper transactional store, but for the current scale (tens of artifacts per instance) a JSON file is adequate.

---

## What the framework guarantees

Plugin code (ingestion functions, transforms, processes) can rely on the following being handled automatically by the framework:

| Concern                                               | Where handled                               |
| ----------------------------------------------------- | ------------------------------------------- |
| Coordinate name normalisation (`lat` → `y`, etc.)     | `build_dataset_zarr`                        |
| Reprojection to instance CRS                          | `reproject_to_instance_crs`                 |
| Zarr chunking (auto-sized from `extents.temporal.resolution`) | `_compute_time_space_chunks`         |
| Multiscale pyramid generation (when dims > 2048×2048) | `build_dataset_zarr`                        |
| GeoZarr root attributes (`spatial:bbox`, `proj:code`) | `build_dataset_zarr`                        |
| Artifact coverage computation                         | `_coverage_from_dataset`                    |
| Artifact record persistence                           | `_store_artifact`                           |
| pygeoapi publication                                  | `publish_artifact_record` if `publish=true` |
| STAC collection generation                            | Dynamic from artifact record                |

Plugin code only needs to produce data files. Everything else is the framework's responsibility.

---

## Consequences of design choices

### Single extent per instance

Each instance is configured for one place. This keeps the data model simple (no per-artifact extent tags) and the zarr stores small (country-scale downloads rather than global). The trade-off is that a national ministry with sub-national data needs either runs multiple instances or configures a single instance at national extent.

### Temporal gaps are not allowed

The sync engine validates that new data connects to the end of the existing artifact before appending. If a gap exists, the sync fails rather than silently producing a dataset with a hole. This is a deliberate constraint: downstream consumers (DHIS2, CHAP) depend on continuous time series and should not receive data with silent gaps.

### The append execution mode

For **legacy ZARR datasets** (downloader-based, no `ingestion.plugin`), `append` downloads only the missing time range and rebuilds the full zarr from all cached files. The local cache (NetCDF files in `data/downloads/`) is the source of truth; the zarr is a derived view. If the cache is deleted, a rematerialize is required to recover.

For **plugin-path** datasets, `append` compares the pending period list against the already-committed time coordinates in the Icechunk store and fetches only the missing periods. The Icechunk store itself is the source of truth — no separate download cache. A crash leaves the store at the last committed period; restart resumes from there without any additional recovery logic.

### Transforms run after download, before reproject

Transforms see raw downloaded values in the source CRS and source units. The order is: download → transform → reproject → write zarr.
