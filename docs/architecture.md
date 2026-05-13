# Architecture

This document explains how the Climate API is structured, why it is structured that way, and what the consequences are of each design decision. It is written for developers who will maintain or extend the platform over time.

---

## Core concepts

The platform has four first-class concepts. Understanding the distinction between them is the foundation for understanding everything else.

### Template

A **template** is a YAML blueprint that describes a data source. It lives in `data/datasets/` (built-ins) or in a `plugins_dir/datasets/` directory (custom). It has no state — it describes what *could* be ingested, not what *has been* ingested.

A template defines:
- the dataset identifier and display metadata
- the variable name, units, and period type
- how to download or derive the data (`ingestion.function`)
- what transforms to apply (`transforms`)
- what sync strategy to use (`sync.kind`, `sync.execution`)

Templates are config, not code. If a template needs custom logic, the logic goes into a Python function referenced by dotted path from the YAML.

### Artifact

An **artifact** is the internal record of a completed ingestion. It is the persistence layer — not a public API concept. Each ingestion produces exactly one artifact, which records:
- what dataset template it came from
- the exact spatial extent and time range that was materialized
- where the data lives on disk (path to the zarr store or netCDF files)
- when it was created
- whether it has been published

Multiple artifacts can exist for the same dataset template if data was ingested at different times (they form the version history). The most recent artifact for a given `dataset_id` is what the public API serves.

Artifacts are stored in `data/artifacts/records.json`. This is an internal implementation detail — consumers should never depend on artifact IDs or artifact paths directly.

### Managed dataset

A **managed dataset** is the public-facing view of the most recent artifact for a given template. It is what `/datasets`, `/zarr`, `/stac`, and `/ogcapi` expose. When an operator ingests or syncs a dataset, the managed dataset view updates to reflect the new artifact — the public ID stays stable.

The relationship is: one template → many artifacts over time → one managed dataset (the latest).

### Extent

The **extent** is the spatial bounding box configured for this Climate API instance. It is set once in `climate-api.yaml` and does not change at runtime. Every ingestion is automatically scoped to this extent — operators do not specify it per-request.

This is a deliberate design constraint: each instance serves one place. A Norway instance serves Norway. A Sierra Leone instance serves Sierra Leone. Multi-country coverage requires multiple instances.

---

## Data lifecycle

```
Template (YAML)
    │
    │  POST /ingestions
    ▼
Ingestion
    │  download or derive data
    │  apply transforms
    │  write GeoZarr store
    │  compute coverage (spatial + temporal extent of actual data)
    ▼
Artifact (internal record)
    │
    │  publish=true
    ▼
Managed dataset (public API)
    ├── /datasets/{id}       — native metadata
    ├── /zarr/{id}           — raw zarr store access
    ├── /stac/collections/{id} — STAC discovery
    └── /ogcapi/collections/{id} — OGC API access
```

The framework is responsible for everything from "write zarr" onward. A download function or derivation function only needs to produce a zarr file at `output_path`. The framework then:

1. applies transforms (unit conversion, etc.)
2. writes GeoZarr root attributes (`spatial:bbox`, `proj:code`) so map clients can position tiles
3. computes artifact coverage (spatial bounds + time range) from the written data
4. stores the artifact record
5. publishes the managed dataset through pygeoapi if `publish=true`

This division means that download functions and derivation functions do not need to know about zarr conventions, STAC, OGC, or pygeoapi. They write data; the framework handles everything else.

---

## Sync kinds

The `sync.kind` field in a template determines how a managed dataset is kept current. Choosing the wrong kind is the most common source of confusion — the table below is the primary guide.

| `sync.kind` | Data lives | On each sync | Use when |
|-------------|-----------|--------------|----------|
| `temporal` | Local zarr | Append new time steps (or rematerialize) | Historical record that grows over time (CHIRPS, ERA5-Land) |
| `release` | Local zarr | Rematerialize if a newer release exists | Versioned releases (WorldPop yearly) |
| `static` | Local zarr | Never synced | One-time fixed dataset |
| `derived` | Local zarr, rebuilt from remote | Always rematerialise from remote store | Forecast data that needs transformation before it is usable (GEFS) |
| `remote` | Remote store, no local copy | Re-register latest coverage | Datasets where direct store access is acceptable and transformation is not needed |

### When to use `derived` vs `remote`

`remote` is the simplest: the Climate API registers the remote store URL as the artifact path and proxies requests to it directly. Nothing is downloaded or transformed. The trade-off is that the raw store is served as-is — if it has five dimensions (`init_time`, `lead_time`, `ensemble_member`, `latitude`, `longitude`), the client sees five dimensions.

`derived` performs a transformation step before the data is accessible. The derivation function reads the remote store, collapses it to a usable shape (e.g. ensemble mean, lead_time → calendar dates, daily resample), and writes a local zarr. The local zarr is what clients see. The trade-off is that a sync rewrites the entire artifact — there is no incremental append.

**Rule of thumb:** use `remote` only if the remote store is already in a shape that map clients and downstream tools can consume directly. Use `derived` when the raw store requires transformation to be useful.

### The sync execution modes

Within `sync.kind: temporal`, two execution modes exist:

- `append` — downloads only the missing time range and appends it to the existing artifact
- `rematerialize` — discards the existing artifact and rebuilds it from scratch

`append` is efficient for large historical datasets (avoid re-downloading years of data on each sync). `rematerialize` is correct for forecasts and any dataset where old data may change retroactively. `derived` always rematerialises.

---

## The plugin contract

The platform has four extension points. Each one has a narrow contract — the framework handles everything else automatically.

### Download function (`sync.kind: temporal`, `release`, `static`)

```python
def download(
    *,
    start: str,
    end: str,
    dirname: Path,
    prefix: str,
    overwrite: bool,
    bbox: list[float],    # optional — only if needed
    **kwargs,             # default_params from YAML
) -> None:
    # Write NetCDF files to dirname using prefix as filename base.
```

The function writes one or more NetCDF files. The framework reads them, applies transforms, reprojects to the instance CRS, builds the zarr, writes GeoZarr attributes, computes coverage, and registers the artifact.

### Derivation function (`sync.kind: derived`)

```python
def derive_dataset(
    *,
    store_config: dict,   # the template's store block
    output_path: Path,    # write a consolidated zarr here
    extent: dict | None,  # instance spatial extent
    variable: str,
    period_type: str,
) -> None:
    # Read from remote store, transform, write zarr to output_path.
    # Output must have dimensions (time, latitude, longitude) in ascending order.
    # Do not write GeoZarr root attributes — the framework does this.
```

The framework calls the derivation function, then applies any declared transforms, writes GeoZarr root attributes, computes coverage, and registers the artifact.

**Important:** the derivation function must not write GeoZarr root attributes (`spatial:bbox`, `proj:code`) itself. These are written by the framework after transforms run, using the actual coordinate bounds of the final written data. If the function writes them, the framework will overwrite them anyway; if they are wrong they will confuse map clients.

### Transform function

```python
def my_transform(ds: xr.Dataset, dataset: dict) -> xr.Dataset:
    # Modify ds[dataset["variable"]] and return the modified dataset.
    # Preserve ds.attrs — the framework relies on them.
```

Transforms are applied in order after the download or derivation function returns. They receive the full xarray Dataset and the template dict. They return a modified Dataset. They should not write to disk — the framework handles writing.

### Process execution function

```python
def execute(*, source_dataset_id: str, **kwargs) -> dict:
    # Run a derived computation (e.g. temporal resampling).
    # Return a JSON-serialisable dict.
```

Processes are named operations triggered via `POST /processes/{id}/execution`. They are broader than single-dataset transforms — they can read one dataset and produce another (e.g. daily → monthly aggregation).

---

## The transform pipeline

Transforms are applied in a consistent location in the lifecycle:

1. download function / derivation function writes raw data
2. `_run_transforms(ds, dataset)` applies each transform in the declared order
3. result is written to the zarr store
4. framework writes GeoZarr root attributes
5. framework computes coverage from the zarr

This means transforms always see the raw output of the function, not the GeoZarr-attributed zarr. Transforms should only modify data values and variable attributes. They must not modify dataset-level `.attrs` — the framework manages those.

The transform pipeline runs identically for downloaded data and derived data. A transform written for a regular dataset works unchanged on a derived dataset.

---

## GeoZarr root attributes

Every zarr artifact must have GeoZarr root attributes for map rendering to work correctly. These are stored in the `zarr.json` root metadata:

- `spatial:bbox` — `[xmin, ymin, xmax, ymax]` in the native CRS
- `proj:code` — the CRS EPSG code (e.g. `EPSG:4326`)
- `zarr_conventions` — GeoZarr convention declaration

The map viewer reads `spatial:bbox` and `proj:code` to determine where to position tiles on the map. Without them, the viewer falls back to the instance CRS and null bounds, which produces a white or misaligned map.

**The framework writes these attributes — plugins do not.** For downloaded datasets, they are written in `build_dataset_zarr`. For derived datasets, they are written in `_derive_artifact` after transforms are applied, using the actual coordinate bounds of the final data and the CRS declared in `store.crs` (default: `EPSG:4326`).

---

## CRS handling

The instance CRS is configured in `climate-api.yaml`:

```yaml
extent:
  name: Norway
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633   # optional; defaults to EPSG:4326
```

**Downloaded datasets** are reprojected from their source CRS (`source_crs` in the template, default `EPSG:4326`) to the instance CRS during ingestion. The stored zarr is in the instance CRS.

**Derived datasets** are never reprojected. They are stored in their native CRS, declared via `store.crs` in the template (default `EPSG:4326`). If the remote store is in a projected CRS, set `store.crs` accordingly so STAC metadata reflects the correct projection.

**Remote datasets** follow the same rule as derived: no reprojection, CRS from `store.crs`.

---

## Artifact deduplication and version history

When a new ingestion request arrives, the framework checks whether an existing artifact already covers the requested scope:

- same `dataset_id`
- same `bbox` (from the configured extent)
- overlapping time range

If a match exists and `overwrite=false`, the existing artifact is returned without re-downloading. If `overwrite=true`, the existing artifact is replaced.

For derived and remote datasets, the framework always upserts: it replaces the existing record while preserving the `artifact_id`, so publication state (pygeoapi config, STAC entries) is maintained without a re-publish step.

---

## What the framework guarantees

Plugin code (download functions, derivation functions, transforms) can rely on the following being handled automatically:

| Concern | Where handled |
|---------|---------------|
| Writing GeoZarr root attributes | `build_dataset_zarr` (download), `_derive_artifact` (derived) |
| Computing artifact coverage | `_coverage_from_dataset` called after all writes |
| Reprojecting to instance CRS | `reproject_to_instance_crs` (downloaded data only) |
| Multiscale pyramid generation | `_needs_pyramid` check in `build_dataset_zarr` |
| Zarr chunking | Auto-computed from `period_type` in `_compute_chunk_sizes` |
| Artifact record persistence | `_upsert_derived_record` / `_upsert_remote_zarr_record` |
| pygeoapi publication | `publish_artifact_record` if `publish=true` |
| STAC collection generation | Dynamic from artifact record |

Plugin code only needs to produce data at the right path. Everything else is the framework's responsibility.

---

## Consequences of design choices

### Consequence: one extent per instance

Each instance is configured for one place. This keeps the data model simple (no multi-extent artifact records) and the zarr stores small (no global downloads). The trade-off is that a national ministry with sub-national data needs runs multiple instances or a single instance at national extent.

### Consequence: artifacts are append-only in version terms

The artifact store keeps historical records (for sync history and deduplication). Old artifacts are not deleted automatically. For long-running instances, `records.json` will grow. The long-term direction is a proper transactional store, but for the current scale (dozens of artifacts per instance) a JSON file is adequate.

### Consequence: derived datasets always rematerialise

`sync.kind: derived` rewrites the full zarr on every sync. For a 35-day GEFS forecast at 0.25° resolution over a country, this is fast (seconds). For a global 1 km dataset with years of history, rematerialisation is impractical — use `temporal` with `append` instead.

### Consequence: transforms run after the function writes

Transforms see the exact output of the download or derivation function, not raw upstream data. This means:
- unit conversion transforms work on whatever unit the function wrote
- if the function already converts units, a unit conversion transform would double-convert
- the function and the template's `transforms` list must not both handle the same conversion

For GEFS: the raw store is in kg m⁻² s⁻¹. The derivation function (`gefs.py`) writes the raw values. The `flux_to_mm_per_day` transform in the template converts them. Neither the function nor the template does both steps.

### Consequence: the plugin contract is stable but the framework internals are not

The public plugin contract — function signatures, YAML field names, dotted-path resolution — is intended to be stable. Framework internals (`_derive_artifact`, `_run_transforms`, `build_dataset_zarr`) are not public API and may change. Plugins should depend only on the documented contracts, not on importing private framework functions.
