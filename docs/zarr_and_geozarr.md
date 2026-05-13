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
- **xarray integration** — `xr.open_zarr()` opens a Zarr store lazily as an xarray dataset, making it trivial to subset, resample, and transform data in the API layer.

---

## Store layout on disk

Each managed dataset has exactly one Zarr store on disk, stored under `{data_dir}/downloads/{dataset_id}.zarr`. The store is either:

- **Flat** — a single-resolution Zarr store with dimensions `(time, x, y)`
- **Pyramid** — a multi-resolution Zarr store with levels `0/`, `1/`, `2/`, … where `0/` is the full resolution

The flat vs. pyramid decision is made at build time based on spatial size (see [Multiscale pyramids](#multiscale-pyramids) below).

---

## Chunk sizing

Chunks are sized to match expected access patterns. The goal is that reading one time step for the full spatial extent fits in one round-trip, and that full time series for a small area also fits in one round-trip.

Time chunk sizes are derived from the dataset's `extents.temporal.resolution` field, which every dataset template declares as an ISO 8601 duration (e.g. `P1D`, `PT1H`, `P1M`). The duration is converted to approximate hours and mapped to a natural analysis window:

| Duration tier       | Approximate hours | Target window | Example                      |
| ------------------- | ----------------- | ------------- | ---------------------------- |
| Sub-daily           | < 24 h            | ~1 week       | `PT1H` (hourly) → 168 steps  |
| Daily to sub-weekly | 24 h – 168 h      | ~1 month      | `P1D` (daily) → 30 steps     |
| Weekly and coarser  | ≥ 168 h           | ~1 year       | `P1M` (monthly) → 12 steps   |

This calculation is fully data-driven: any dataset — including custom or plugin datasets — only needs to declare `extents.temporal.resolution` and the correct chunk size is computed automatically. No hardcoded lookup by period name is needed.

Spatial chunks are capped at 512 × 512 pixels — a pragmatic compromise between tile rendering (which benefits from smaller chunks) and analysis workloads (which benefit from larger ones). For small extents where the full spatial dimension is smaller than 512 pixels, the entire dimension fits in one chunk.

Dimension names are normalised to `(time, x, y)` before writing, regardless of the source naming convention (`lat`/`lon`, `latitude`/`longitude`, etc.).

---

## Multiscale pyramids

For large spatial extents, a flat zarr would require the map viewer to download the entire spatial extent at full resolution on every tile request. The platform builds a multiscale pyramid when the spatial dimensions exceed **2048 × 2048 pixels**.

Pyramid levels are computed as:

```
levels = ceil(log2(max_dim / 512))   # clamped to [2, 8]
```

Where 512 is the target tile size in pixels. Each level halves the resolution in both spatial dimensions using mean downsampling. Level `0/` is always the full resolution.

Both flat and pyramid stores are written in **Zarr v3** format. Pyramids use [topozarr](https://github.com/carbonplan/topozarr); flat stores pass `zarr_format=3` to `to_zarr`. Both include consolidated metadata — embedded in `zarr.json` under `consolidated_metadata` — so clients can open the store with a single metadata read.

One implementation detail: ZarrLayer (the map client) looks for the `time` coordinate at the root of the store. For pyramid stores, the `time` coordinate from level `0/` is copied to the store root so clients can discover the time axis without knowing the pyramid structure.

---

## GeoZarr root attributes

A plain Zarr store has no concept of spatial coordinates. A map viewer opening it has no way to know where to position tiles on a map — whether `x=0` means longitude 0°, easting 300000 m, or something else.

GeoZarr addresses this by writing a small set of attributes into `zarr.json` at the store root:

| Attribute          | Example value                         | Purpose                        |
| ------------------ | ------------------------------------- | ------------------------------ |
| `spatial:bbox`     | `[270000, 6450000, 1120000, 7950000]` | Bounding box in the native CRS |
| `proj:code`        | `EPSG:32633`                          | CRS of the stored coordinates  |
| `zarr_conventions` | `[{...}]`                             | Convention declarations        |

These attributes are computed from the actual coordinate bounds of the written data and the instance CRS. They are always written by the framework after transforms and reprojection have run — never by download functions. This guarantees they always reflect the final stored data.

`zarr_conventions` for a flat store contains the base GeoZarr convention declaration. For pyramid stores it also includes a multiscales entry that declares the level structure.

**Without these attributes**, the map viewer falls back to null bounds and the instance CRS, producing a white or misaligned map.

---

## CRS handling

The instance CRS is configured in `climate-api.yaml`:

```yaml
extent:
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

Datasets are always stored in the instance CRS. During ingestion, data is reprojected from its source CRS (declared as `source_crs` in the template, default `EPSG:4326`) to the instance CRS. The stored `spatial:bbox` is therefore in the instance CRS — UTM eastings and northings for a UTM instance, degrees for a WGS84 instance.

STAC metadata also stores the WGS84 bounding box alongside the native bbox, so catalogue clients that expect geographic coordinates always get one regardless of the instance CRS.

---

## How Zarr stores are served

The `/zarr/{dataset_id}/` endpoint serves individual files from the Zarr store directory using FastAPI's `FileResponse`. The ZarrLayer client issues one HTTP request per chunk file it needs.

```
GET /zarr/{dataset_id}/zarr.json          → root metadata (JSON)
GET /zarr/{dataset_id}/precip/c/0/0/0     → chunk at time=0, x=0, y=0
GET /zarr/{dataset_id}/time/c/0           → time coordinate chunk
```

Metadata files (`zarr.json`) are returned as `application/json`. All other files — chunk data — are returned as `application/octet-stream`. Directory paths return a JSON listing of their contents.

This design means the zarr store is served by ordinary file serving — there is no zarr-specific server middleware.

---

## How the map viewer reads Zarr

The built-in map viewer at `/map` uses [ZarrLayer](https://github.com/carbonplan/zarr-layer) to render zarr tiles on a Leaflet map.

On dataset selection the viewer:

1. Fetches the STAC collection for the dataset (`/stac/collections/{dataset_id}`), which includes `proj:code`, `cube:dimensions`, and the zarr store href
2. Reads `proj:code` to determine if reprojection is needed for rendering
3. Reads `cube:dimensions` to discover the time axis and build the time slider
4. Initialises ZarrLayer with the zarr store URL, the variable name, the colour map, and the CRS

ZarrLayer then requests chunk files directly from `/zarr/{dataset_id}/` as tiles are needed, handling all chunked reads internally.

For non-WGS84 instances (e.g. UTM), the viewer fetches a proj4 string from `epsg.io` and passes it to ZarrLayer so tiles can be reprojected to the Leaflet display CRS on the fly.

---

## Fill values and NaN handling

When writing float data to Zarr, NetCDF sentinel fill values (e.g. `−999.99`) are removed from the encoding before the zarr write. This ensures missing pixels are stored as IEEE `NaN` in the zarr chunks. ZarrLayer uses the zarr `fill_value` attribute (which defaults to `NaN` for float arrays) to render missing pixels as transparent — no separate `fillValue` configuration is needed in the viewer.
