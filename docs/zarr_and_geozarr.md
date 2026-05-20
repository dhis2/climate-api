# Zarr and GeoZarr

This document explains why the Climate API uses Zarr as its primary storage format, how Zarr stores are structured and served, and how GeoZarr root attributes enable map rendering.

---

## What is Zarr?

[Zarr](https://zarr.dev) is an open storage format for chunked, compressed N-dimensional arrays. A Zarr store is a directory tree: array metadata lives in `zarr.json` files, and the data itself is split into independent chunk files. Each chunk is compressed independently and can be read in a single HTTP request.

Zarr is designed to work natively in cloud object stores as well as on local disk — the directory layout is the same in both cases. The [Zarr v3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) is the current standard.

---

## What is GeoZarr?

[GeoZarr](https://github.com/zarr-developers/geozarr-spec) is a draft convention that adds spatial context to Zarr stores. A plain Zarr array has no concept of geography — it is just numbers in a grid. GeoZarr defines a small set of root attributes (`spatial:bbox`, `proj:code`, `zarr_conventions`) that tell a client where the grid is located on Earth and in which coordinate reference system.

---

## Why Zarr

Climate datasets are large, multi-dimensional arrays: a daily precipitation dataset covering a country at 5 km resolution for 10 years has roughly 3 600 time steps and hundreds of thousands of spatial pixels. Serving this efficiently from a REST API requires a format that supports:

- **Chunk-level random access** — a client requesting one time step should not have to read the entire file. Zarr stores data in independent, addressable chunks; a request for a single date reads only the relevant chunk.
- **HTTP-native serving** — each chunk is a separate file on disk. A standard `GET /zarr/{dataset_id}/{chunk_path}` serves it with a regular `FileResponse`. No specialised server software is needed.
- **Cloud compatibility** — the same directory layout works on local disk and cloud storage without code changes.
- **Multiscale pyramids** — GeoZarr defines a multiscales convention that allows a store to contain multiple resolution levels. Map clients request only the level that matches their current zoom, avoiding full-resolution downloads.

---

## ARCO: Analysis-Ready, Cloud-Optimized

The stores produced by the Climate API are an instance of the **ARCO** pattern — a term from the climate science community describing datasets that are simultaneously ready for analysis and optimised for cloud access.

The two halves of the term map directly onto the choices described in this document:

**Analysis-ready** means a consumer can open the data and start computing without preprocessing:

- Dimension names are normalised to `(time, x, y)` regardless of the source convention.
- All datasets in an instance share a single coordinate reference system.
- Units are standardised by the transform pipeline (e.g. Kelvin → Celsius).

**Cloud-optimized** means the data can be accessed efficiently over HTTP without downloading the whole file. The Zarr and GeoZarr formats provide all the necessary properties — chunk-level access, HTTP-native serving, multiscale pyramids, and cloud compatibility.

The Climate API targets the same access pattern at country scale for arbitrary source datasets.

---

## Store layout on disk

Each managed dataset has exactly one Icechunk repository on disk, stored under `{data_dir}/downloads/{dataset_id}.icechunk`. The zarr content inside the repository is either:

- **Flat** — a single-resolution store with dimensions `(time, x, y)`
- **Pyramid** — a multi-resolution store with levels `0/`, `1/`, `2/`, … where `0/` is the full resolution

The flat vs. pyramid decision is made at ingest time based on spatial size (see [Multiscale pyramids](#multiscale-pyramids) below).

---

## Icechunk — versioned Zarr storage

[Icechunk](https://icechunk.io) is a transactional storage layer that sits between the application and the underlying Zarr v3 data. It exposes a standard Zarr store interface to writers and readers, but adds **MVCC (multi-version concurrency control)**: every write is committed as an immutable snapshot, and readers always see a consistent view of the data regardless of concurrent writes.

### Why Icechunk

Plain Zarr on disk is a directory of independent chunk files — there is no transaction boundary. If an ingest is interrupted mid-write, some chunks for a new time step may be written and others not, leaving the store in an inconsistent state with no way to roll back.

The Climate API ingests one period at a time, committing each as a separate Icechunk snapshot. This gives three concrete properties:

- **Safe resume** — if a job is cancelled or the server restarts, the next run reads the list of committed snapshots and skips periods that are already present. No partial writes are ever visible to readers.
- **Snapshot isolation** — a read session opened at the start of a request sees a consistent snapshot even if a concurrent ingest is writing new periods. Readers are never blocked by writers.
- **Prunable history** — intermediate per-period snapshots accumulate during ingest. After the rechunk and pyramid passes complete, `expire_snapshots()` prunes all but the latest, keeping disk usage proportional to data size rather than ingest history.

### Snapshot lifecycle

A typical WorldPop ingest produces snapshots roughly like this:

```
snapshot 1:  write period 2015
snapshot 2:  write period 2016
...
snapshot 16: write period 2030
snapshot 17: rechunk: time=1
snapshot 18: pyramid: 6 levels
→ expire_snapshots() prunes snapshots 1–17
snapshot 18: (the only surviving snapshot — full pyramid, correctly chunked)
```

### Serving from Icechunk

Zarr keys are read directly from the Icechunk session store rather than from files on disk. The HTTP surface is identical — the same `/zarr/{dataset_id}/` routes — but the backend resolves each key through the Icechunk MVCC layer instead of a `FileResponse`.

---

## Chunk sizing

Chunks are sized to match expected access patterns. The goal is that reading one time step for the full spatial extent fits in one round-trip, and that full time series for a small area also fits in one round-trip.

Time chunk sizes are derived from the dataset's `extents.temporal.resolution` field, an ISO 8601 duration (e.g. `P1D`, `PT1H`, `P1M`). When present and valid, the duration is converted to approximate hours and mapped to a natural analysis window:

| Duration tier       | Approximate hours | Target window | Example                     |
| ------------------- | ----------------- | ------------- | --------------------------- |
| Sub-daily           | < 24 h            | ~1 week       | `PT1H` (hourly) → 168 steps |
| Daily to sub-weekly | 24 h – 168 h      | ~1 month      | `P1D` (daily) → 30 steps    |
| Weekly and coarser  | ≥ 168 h           | ~1 year       | `P1M` (monthly) → 12 steps  |

This calculation is fully data-driven: any dataset — including custom or plugin datasets — only needs to declare `extents.temporal.resolution` and the correct chunk size is computed automatically. If the field is absent or not a valid ISO 8601 duration, a warning is logged and the time chunk falls back to the dataset's `period_type`.

Spatial chunks are capped at 512 × 512 pixels — a pragmatic compromise between tile rendering (which benefits from smaller chunks) and analysis workloads (which benefit from larger ones). For small extents where the full spatial dimension is smaller than 512 pixels, the entire dimension fits in one chunk.

Dimension names are normalised to `(time, x, y)` before writing, regardless of the source naming convention (`lat`/`lon`, `latitude`/`longitude`, etc.).

---

## Multiscale pyramids

For large spatial extents, a flat zarr would require a map viewer to download the entire spatial extent at full resolution on every tile request. The platform builds a multiscale pyramid when the spatial dimensions exceed **2048 × 2048 pixels**.

Pyramid levels are computed as:

```
levels = ceil(log2(max_dim / 512))   # clamped to [2, 8]
```

Where 512 is the target tile size in pixels. Each level halves the resolution in both spatial dimensions using mean downsampling. Level `0/` is always the full resolution.

Both flat and pyramid stores are written in **Zarr v3** format.

---

## GeoZarr root attributes

A plain Zarr store has no concept of spatial coordinates. A map viewer opening it has no way to know where to position tiles on a map. GeoZarr addresses this by writing a small set of attributes into `zarr.json` at the store root:

| Attribute          | Example value             | Purpose                        |
| ------------------ | ------------------------- | ------------------------------ |
| `spatial:bbox`     | `[3.0, 57.0, 32.0, 72.5]` | Bounding box in the native CRS |
| `proj:code`        | `EPSG:4326`               | CRS of the stored coordinates  |
| `zarr_conventions` | `[{...}]`                 | Convention declarations        |

These attributes are computed from the actual coordinate bounds of the written data and the CRS declared by the plugin in `GridSpec.crs`. They are written by the framework after the first period is committed. This guarantees they always reflect the final stored data.

`zarr_conventions` for a flat store contains the base GeoZarr convention declaration. For pyramid stores it also includes a multiscales entry that declares the level structure.

---

## CRS handling

The instance CRS is configured in `climate-api.yaml`:

```yaml
extent:
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

Datasets are stored in whatever CRS the plugin returns. The plugin declares this via `GridSpec.crs` in its `probe()` response, and the framework writes `proj:code` from that value. No automatic reprojection occurs — if CRS conversion is needed, declare `reproject_to_instance_crs` as an explicit transform in the dataset template. The stored `spatial:bbox` is in the plugin's native CRS.

STAC metadata also stores the WGS84 bounding box alongside the native bbox, so catalogue clients that expect geographic coordinates always get one regardless of the instance CRS.

---

## How Zarr stores are served

The `/zarr/{dataset_id}/` endpoint serves Zarr keys from the Icechunk repository. The ZarrLayer client issues one HTTP request per key it needs:

```
GET /zarr/{dataset_id}/zarr.json          → root metadata (JSON)
GET /zarr/{dataset_id}/precip/c/0/0/0     → chunk at time=0, x=0, y=0
GET /zarr/{dataset_id}/time/c/0           → time coordinate chunk
```

Each request opens a readonly Icechunk session pinned to the latest committed snapshot, resolves the zarr key through the MVCC layer, and returns the raw bytes. Metadata files (`zarr.json`) are returned as `application/json`; chunk data as `application/octet-stream`; directory paths as a JSON listing.

The HTTP surface is identical to serving files from disk — ZarrLayer and other zarr clients require no changes — but correctness and consistency are guaranteed by Icechunk's snapshot model rather than filesystem state.

---

## Fill values and NaN handling

When writing float data to Zarr, missing data is stored as IEEE `NaN`. The map viewer uses the zarr `fill_value` attribute (which defaults to `NaN` for float arrays) to render missing pixels as transparent.
