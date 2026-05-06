# Custom Dataset Guide

This guide explains how to add a new dataset source to your Climate API instance — for example a national meteorological service, a regional satellite product, or a custom model output.

The built-in dataset templates (CHIRPS3, ERA5-Land, WorldPop) ship as package data. Custom datasets are layered on top by pointing `datasets_dir` in your `climate-api.yaml` at a directory containing your own YAML template files.

## Overview

Adding a custom dataset involves two things:

1. **A download function** — a Python function that accepts a bounding box and time range and returns an `xarray.Dataset` with `time`, `latitude`, and `longitude` coordinates.
2. **A dataset template YAML** — a file that describes the dataset and tells the API which download function to call.

## Step 1: Write the download function

The download function must be importable as a dotted Python path and accept a standard set of keyword arguments:

```python
# mypackage/sources/enacts.py
import xarray as xr

def download(
    *,
    bbox: tuple[float, float, float, float],  # xmin, ymin, xmax, ymax
    start: str,                               # ISO 8601 date or datetime
    end: str,
    **kwargs: object,
) -> xr.Dataset:
    """Download ENACTS rainfall for the given bbox and time range."""
    # ... fetch and return an xr.Dataset with coordinates:
    #     time (datetime64), latitude (float), longitude (float)
    ...
```

The returned dataset must use the canonical coordinate names: `time`, `latitude`, `longitude`. Any additional keyword arguments from `default_params` in the YAML template are forwarded as `**kwargs`.

Install your package in the same environment as the Climate API:

```bash
pip install ./mypackage
```

## Step 2: Create a dataset template YAML

Create a directory for your custom templates and add a YAML file. Each file contains a list of templates (even if there is only one):

```yaml
# datasets/enacts_rainfall.yaml
- id: enacts_rainfall_daily
  name: ENACTS Rainfall (daily)
  short_name: Rainfall
  variable: rainfall
  period_type: daily
  sync_kind: temporal
  sync_execution: append
  cache_info:
    eo_function: mypackage.sources.enacts.download
  units: mm
  resolution: 4 km x 4 km
  source: ENACTS
  source_url: https://enacts.example.org
```

### Template field reference

**Identity**

| Field        | Required | Description |
| ------------ | -------- | ----------- |
| `id`         | Yes | Unique template identifier. Dataset IDs in the API are `{id}_{extent_id}`, e.g. `enacts_rainfall_daily_rwa` |
| `name`       | Yes | Full human-readable name shown in API responses and STAC metadata |
| `short_name` | No  | Short label used in compact displays |
| `variable`   | Yes | Name of the data variable in the Zarr store (e.g. `precip`, `t2m`, `rainfall`) |
| `source`     | No  | Name of the upstream data source |
| `source_url` | No  | URL to the upstream dataset documentation or landing page |

**Period and sync**

| Field            | Required | Values | Description |
| ---------------- | -------- | ------ | ----------- |
| `period_type`    | Yes | `hourly`, `daily`, `monthly`, `yearly` | Temporal resolution; controls Zarr chunk sizes |
| `sync_kind`      | Yes | `temporal` | Data grows over time — sync appends new time steps |
|                  |     | `release`  | Data is published as versioned releases — sync checks for a newer release |
|                  |     | `static`   | Data does not change — never synced automatically |
| `sync_execution` | No  | `append` | New time steps are appended to the existing Zarr store |
|                  |     | `rematerialize` | The full store is rebuilt on each sync |

**Download function**

| Field                          | Required | Description |
| ------------------------------ | -------- | ----------- |
| `cache_info.eo_function`       | Yes | Dotted import path to the download function |
| `cache_info.default_params`    | No  | Extra keyword arguments forwarded to the download function |
| `cache_info.multiscales`       | No  | Build a multi-resolution Zarr pyramid (see below) |

**Sync availability** — how the API determines the latest available data:

```yaml
sync_availability:
  latest_available_function: mypackage.sources.enacts.latest_available
  lag_hours: 48          # optional: data is delayed by this many hours
  allow_future: false    # optional: allow requesting future dates (e.g. forecasts)
```

`latest_available_function` must accept a `dataset` dict and return a `datetime`. Omit `sync_availability` entirely for `static` datasets or when you always want to sync up to the requested end date.

**Units**

| Field           | Required | Description |
| --------------- | -------- | ----------- |
| `units`         | No  | Physical units of the raw download (e.g. `mm`, `kelvin`, `m`) |
| `convert_units` | No  | Target unit; the API converts automatically using Pint (e.g. `degC`, `mm`) |

**Multiscale pyramid** — for high-resolution raster datasets rendered in map viewers:

```yaml
cache_info:
  eo_function: mypackage.sources.enacts.download
  multiscales:
    levels: 4    # number of pyramid levels (default: 4)
    method: mean # aggregation method (default: mean)
```

## Step 3: Point the instance at your templates directory

Add `datasets_dir` to your `climate-api.yaml`:

```yaml
extent:
  id: rwa
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]

datasets_dir: ./datasets/
```

All `*.yaml` and `*.yml` files in `datasets_dir` are loaded and merged with the built-in templates (CHIRPS3, ERA5-Land, WorldPop). Custom templates are additive — the built-ins remain available unless you deliberately override one by using the same `id`.

## Step 4: Ingest and publish

Once the API is running with `CLIMATE_API_CONFIG` pointing to your updated config, ingest as usual:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "enacts_rainfall_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "extent_id": "rwa",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Verify it appears in the STAC catalog:

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq '.links[] | select(.rel == "child")'
```

## Minimal example

The smallest valid template for a static dataset with no sync:

```yaml
- id: my_static_dataset
  name: My static dataset
  variable: value
  period_type: daily
  sync_kind: static
  cache_info:
    eo_function: mypackage.sources.my_source.download
```
