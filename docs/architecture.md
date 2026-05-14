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
- how to download or derive the data (`ingestion.function`)
- what transforms to apply (`transforms`)
- what sync strategy to use (`sync.kind`, `sync.execution`)

Templates are config, not code. If a template needs custom logic, the logic goes into a Python function referenced by dotted path from the YAML.

### Artifact

An **artifact** is the internal record of a completed data ingestion. It is the persistence layer — not a public API concept. Each ingestion produces exactly one artifact, which records:

- what dataset template it came from
- the exact spatial extent and time range that was materialized
- where the data lives on disk (path to the zarr store or netCDF files), or the URL of the remote store
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

For downloaded data (the common case), the lifecycle is:

```
Template (YAML)
    │
    │  POST /ingestions
    ▼
Ingestion
    │  download data
    │  apply transforms
    │  reproject to instance CRS
    │  write GeoZarr store
    │  compute coverage (spatial + temporal extent of actual data)
    ▼
Artifact (internal record)
    │
    │  publish=true
    ▼
Managed dataset (public API)
    ├── /datasets/{id}         — native metadata
    ├── /zarr/{id}             — raw zarr store access
    ├── /stac/collections/{id} — STAC discovery
    └── /ogcapi/collections/{id} — OGC API access
```

For derived data (forecast datasets), a derivation function replaces the download step. The rest of the lifecycle is identical — see [Derived datasets](#derived-datasets) below.

The ingestion function is called identically by both `POST /ingestions` and `POST /sync` — the framework invokes it the same way regardless of the trigger. A correctly written ingestion function works for both without any changes.

The framework is responsible for everything from "write zarr" onward. A download or derivation function only needs to produce data at a given path. The framework then:

1. reads and normalises the coordinate names
2. applies transforms (unit conversion, etc.)
3. reprojects to the instance CRS (downloaded data only)
4. builds the zarr store with auto-computed chunking
5. writes GeoZarr root attributes (`spatial:bbox`, `proj:code`) so map clients can position tiles
6. computes artifact coverage (spatial bounds + time range) from the written data
7. stores the artifact record
8. publishes the managed dataset through pygeoapi if `publish=true`

This division means that download and derivation functions do not need to know about zarr conventions, STAC, OGC, or pygeoapi. They write data; the framework handles everything else.

---

## Sync kinds

The `sync.kind` field in a template determines how a managed dataset is kept current.

| `sync.kind` | Data lives                      | On each sync                            | Use when                                                                              |
| ----------- | ------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------- |
| `temporal`  | Local zarr                      | Append new time steps, or rematerialize | Historical record that grows over time (CHIRPS, ERA5-Land)                            |
| `release`   | Local zarr                      | Rematerialize if a newer release exists | Versioned releases where each year/version is discrete (WorldPop)                     |
| `static`    | Local zarr                      | Never synced                            | One-time fixed dataset with no updates                                                |
| `derived`   | Local zarr, rebuilt from remote | Always rematerialize from remote store  | Forecast or model data that requires transformation before it is usable (GEFS)        |
| `remote`    | Remote store, no local copy     | Re-register latest coverage             | Datasets where direct store access is acceptable and no local transformation is needed |

### When to use `derived` vs `remote`

`remote` is the simplest: the Climate API registers the remote store URL as the artifact path and proxies requests to it directly. Nothing is downloaded or transformed. The trade-off is that the raw store is served as-is — if it has five dimensions (`init_time`, `lead_time`, `ensemble_member`, `latitude`, `longitude`), clients see five dimensions.

`derived` performs a transformation step before the data is accessible. The derivation function reads the remote store, collapses it to a usable shape (e.g. ensemble mean, lead_time → calendar dates, daily resample), and writes a local zarr. The local zarr is what clients see. The trade-off is that every sync rewrites the entire local artifact — there is no incremental append.

**Rule of thumb:** use `remote` only when the remote store is already in a shape that map clients and downstream tools can consume directly. Use `derived` when the raw store requires a transformation step to be useful.

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

The platform has five extension points. Each one has a narrow contract — the framework handles everything else automatically.

### Download function (`sync.kind: temporal`, `release`, `static`)

```python
def download(
    *,
    start: str,       # ISO 8601 date or datetime
    end: str,
    dirname: Path,    # write output files here
    prefix: str,      # use as filename prefix, e.g. f"{prefix}_{year}.nc"
    overwrite: bool,
    bbox: list[float],  # optional — only if the source needs a spatial filter
    **kwargs,           # default_params from the YAML template
) -> None:
    # Write one or more NetCDF files to dirname.
```

The function writes NetCDF files. The framework reads them, normalises coordinate names, applies transforms, reprojects to the instance CRS, builds the zarr, writes GeoZarr attributes, computes coverage, and registers the artifact.

The download function is called identically by `POST /ingestions` and `POST /sync`. The caller makes no difference to the function — it always receives the same parameters.

**Reusing ingestion logic across templates**: multiple YAML templates can reference the same Python function and differentiate via `default_params`. This is the intended pattern for sources that have the same fetching logic but expose different variables:

```yaml
# era5land_temperature_hourly.yaml
ingestion:
  function: dhis2eo.data.era5_land.download
  default_params:
    variable: 2m_temperature

# era5land_precipitation_hourly.yaml
ingestion:
  function: dhis2eo.data.era5_land.download
  default_params:
    variable: total_precipitation
```

No framework changes are needed to support a new variable from the same source.

### Derivation function (`sync.kind: derived`)

```python
def derive_dataset(
    *,
    store_config: dict,   # the template's store block
    output_path: Path,    # write a consolidated zarr here
    extent: dict | None,  # instance spatial extent — use to subset the data
    variable: str,
    period_type: str,
) -> None:
    # Read from remote store, transform, write zarr to output_path.
    # Output must have dimensions (time, latitude, longitude) in ascending order.
    # Do not write GeoZarr root attributes — the framework does this.
```

The function writes a local zarr. The framework then applies any declared transforms, writes GeoZarr root attributes using the actual coordinate bounds of the final data, computes coverage, and registers the artifact.

**Important:** the derivation function must not write GeoZarr root attributes itself. The framework writes them after transforms run, so they always reflect the final data. If the function writes them pre-transform, they will be overwritten.

### Transform function

```python
def my_transform(ds: xr.Dataset, dataset: dict) -> xr.Dataset:
    # Receive the dataset after download or derivation, return a modified dataset.
    # Modify ds[dataset["variable"]] values and variable attributes.
    # Do not modify dataset-level ds.attrs — the framework manages those.
```

Transforms are applied in order after the download or derivation function returns. They receive the full xarray Dataset and the template dict. They return a modified Dataset. They do not write to disk.

### Process execution function

```python
def execute(*, source_dataset_id: str, **kwargs) -> dict:
    # Run a named operation (e.g. temporal resampling).
    # Return a JSON-serialisable result dict.
```

Processes are named operations triggered via `POST /processes/{id}/execution`. They are broader than single-dataset transforms — they can read one managed dataset and produce another (e.g. daily → monthly aggregation).

---

## The transform pipeline

Transforms are applied at a consistent point in the ingestion lifecycle, regardless of sync kind:

1. download or derivation function writes raw data to disk
2. framework loads the data into an xarray Dataset
3. `_run_transforms(ds, dataset)` applies each declared transform in order
4. result is written to the zarr store
5. framework writes GeoZarr root attributes
6. framework computes coverage from the zarr

For downloaded data, step 4 also includes reprojection to the instance CRS. For derived data, no reprojection is applied — the derivation function is responsible for spatial subsetting.

Transforms see post-function, pre-GeoZarr data. They should only modify data values and variable-level attributes. The framework manages dataset-level `.attrs`.

The transform pipeline is identical for downloaded and derived data. A transform written for a regular dataset works unchanged on a derived dataset.

---

## Derived datasets

`sync.kind: derived` is for datasets sourced from a remote zarr store that requires a non-trivial transformation before it is useful. The canonical example is NOAA GEFS, whose raw store has five dimensions (`init_time`, `lead_time`, `ensemble_member`, `latitude`, `longitude`) that need to be collapsed into a simple `time × latitude × longitude` zarr.

The derivation function is called with the same contract each sync. The full pipeline on each sync is:

1. derivation function opens remote store, transforms, writes local zarr
2. framework applies declared transforms (e.g. unit conversion)
3. framework writes GeoZarr root attributes from the actual coordinate bounds
4. framework computes coverage and upserts the artifact record

The artifact is always fully rematerialized — there is no incremental append for derived datasets.

### `store.crs`

For derived datasets, the `store.crs` field in the template sets the CRS for the artifact:

```yaml
store:
  kind: remote_zarr
  store_url: "s3://..."
  crs: "EPSG:4326"   # optional; defaults to EPSG:4326
```

Most public remote stores (dynamical.org, ARCO-ERA5) are in `EPSG:4326`. Set `store.crs` only for stores in projected coordinate systems. The value is written as `proj:code` in the GeoZarr root attributes and STAC metadata.

---

## GeoZarr root attributes

Every zarr artifact must have GeoZarr root attributes for map rendering to work correctly. These are written into `zarr.json` at the store root:

- `spatial:bbox` — `[xmin, ymin, xmax, ymax]` in the native CRS
- `proj:code` — the CRS EPSG code (e.g. `EPSG:32633` for UTM, `EPSG:4326` for WGS84)
- `zarr_conventions` — GeoZarr convention declaration

The map viewer reads `spatial:bbox` and `proj:code` to determine where to position tiles on the map.

**The framework writes these attributes — plugins do not.** For downloaded datasets, they are written in `build_dataset_zarr` after transforms and reprojection. For derived datasets, they are written in `_derive_artifact` after transforms run, using the actual coordinate bounds of the final written data and the CRS from `store.crs`.

---

## CRS handling

The instance CRS is configured in `climate-api.yaml`:

```yaml
extent:
  name: Norway
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

**Downloaded datasets** are reprojected from their source CRS (`source_crs` in the template, default `EPSG:4326`) to the instance CRS during ingestion. The stored zarr is in the instance CRS.

**Derived datasets** are never reprojected. They are stored in their native CRS, declared via `store.crs` (default `EPSG:4326`). The derivation function is responsible for spatial subsetting using the `extent` parameter.

If no `crs` is set in the config, downloaded data is stored in `EPSG:4326` (WGS84). This is the correct default for instances that do not need a metric CRS.

---

## Artifact deduplication and version history

When a new ingestion request arrives, the framework checks whether an existing artifact already covers the requested scope:

- same `dataset_id`
- same bbox (from the configured extent)
- overlapping time range

If a match exists and `overwrite=false`, the existing artifact is returned without re-downloading. If `overwrite=true`, the existing artifact is replaced.

For derived and remote datasets, the framework always upserts: it replaces the existing record while preserving the `artifact_id`, so publication state (pygeoapi config, STAC entries) is maintained without a re-publish step.

The artifact store keeps the full history of records for sync deduplication and provenance. Old artifacts are not deleted automatically. For long-running instances, `records.json` grows over time. The long-term direction is a proper transactional store, but for the current scale (tens of artifacts per instance) a JSON file is adequate.

---

## What the framework guarantees

Plugin code (download functions, derivation functions, transforms, processes) can rely on the following being handled automatically:

| Concern                                               | Where handled                                                     |
| ----------------------------------------------------- | ----------------------------------------------------------------- |
| Coordinate name normalisation (`lat` → `y`, etc.)     | `build_dataset_zarr` (downloaded data)                            |
| Reprojection to instance CRS                          | `reproject_to_instance_crs` (downloaded data only)                |
| Zarr chunking (auto-sized from `extents.temporal.resolution`) | `_compute_time_space_chunks`                              |
| Multiscale pyramid generation (when dims > 2048×2048) | `build_dataset_zarr`                                              |
| GeoZarr root attributes (`spatial:bbox`, `proj:code`) | `build_dataset_zarr` (downloaded), `_derive_artifact` (derived)   |
| Artifact coverage computation                         | `_coverage_from_dataset`                                          |
| Artifact record persistence                           | `_store_artifact` / `_upsert_derived_record`                      |
| pygeoapi publication                                  | `publish_artifact_record` if `publish=true`                       |
| STAC collection generation                            | Dynamic from artifact record                                      |

Plugin code only needs to produce data. Everything else is the framework's responsibility.

---

## Consequences of design choices

### Single extent per instance

Each instance is configured for one place. This keeps the data model simple (no per-artifact extent tags) and the zarr stores small (country-scale downloads rather than global). The trade-off is that a national ministry with sub-national data needs either runs multiple instances or configures a single instance at national extent.

### Temporal gaps are not allowed

The sync engine validates that new data connects to the end of the existing artifact before appending. If a gap exists, the sync fails rather than silently producing a dataset with a hole. This is a deliberate constraint: downstream consumers (DHIS2, CHAP) depend on continuous time series and should not receive data with silent gaps.

### The append execution mode avoids re-downloading history

`append` downloads only the missing range and rebuilds the full zarr from all cached files. This means the local cache (NetCDF files in `data/downloads/`) is the source of truth for the full time series; the zarr is a derived view. If the cache is deleted, a rematerialize is required to recover.

### Derived datasets always rematerialize

`sync.kind: derived` rewrites the full local zarr on every sync. For a 35-day GEFS forecast at 0.25° resolution over a country extent, this takes seconds. For a global high-resolution dataset covering years of history, rematerialization would be impractical — use `temporal` with `append` instead.

### Transforms run after the function writes

Transforms see the exact output of the download or derivation function — raw values before any unit conversion. The order is: function → transform → write zarr → GeoZarr attrs. This means:

- if the function writes values in kg m⁻² s⁻¹, a `flux_to_mm_per_day` transform converts them
- if the function already converts units, adding a unit conversion transform would double-convert
- the function and the template's `transforms` list must not both handle the same conversion

For GEFS: the raw store is in kg m⁻² s⁻¹. The derivation function writes raw values. The `flux_to_mm_per_day` transform in the template converts to mm/day. Neither the function nor the template handles both steps.

### GeoZarr attrs are always written by the framework, never by plugins

Writing GeoZarr attrs is a framework concern because they must reflect the data *after* transforms run — the bbox and CRS of the final written zarr, not the intermediate state during derivation. If a plugin writes them during derivation and transforms later change the extent or units, the attrs would be stale. The framework writes them last, guaranteeing they are always consistent with the actual stored data.
