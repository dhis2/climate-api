# Adding custom datasets

This guide explains how to add a new dataset source to your Climate API instance — for example a national meteorological service, a regional satellite product, or a custom model output.

The built-in dataset templates (CHIRPS3, ERA5-Land, WorldPop) ship as package data. Custom datasets are layered on top by pointing `plugins_dir` in your `climate-api.yaml` at a plugins directory. That directory serves two purposes: YAML dataset templates go in its `datasets/` subfolder, and Python modules placed directly under it are importable by their dotted path (e.g. `mypackage.sources.download`) without installing them as a package.

## Overview

Adding a custom dataset involves two things:

1. **An `IngestionPlugin` class** — streams data directly into an Icechunk store one period at a time.
2. **A dataset template YAML** — a file that describes the dataset and tells the API which plugin to call.

## Step 1: Create a dataset template YAML

Create a directory for your custom templates and add a YAML file. Each file contains a list of templates (even if there is only one):

```yaml
# datasets/enacts_rainfall.yaml
- id: enacts_rainfall_daily
  name: ENACTS Rainfall (daily)
  short_name: Rainfall
  variable: rainfall
  period_type: daily
  sync:
    kind: temporal
    execution: append
  ingestion:
    plugin: mypackage.sources.EnactsPlugin
    params:
      variable: rainfall
  units: mm
  resolution: 4 km x 4 km
  source: ENACTS
  source_url: https://enacts.example.org
```

### Template field reference

**Identity**

| Field        | Required | Description |
| ------------ | -------- | ----------- |
| `id`         | Yes | Unique template identifier. This becomes the dataset ID in the API, e.g. `enacts_rainfall_daily` |
| `name`       | Yes | Full human-readable name shown in API responses and STAC metadata |
| `short_name` | No  | Short label used in compact displays |
| `variable`   | Yes | Name of the data variable in the Zarr store (e.g. `precip`, `t2m`, `rainfall`) |
| `source`     | No  | Name of the upstream data source |
| `source_url` | No  | URL to the upstream dataset documentation or landing page |

**Period and sync**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `period_type` | Yes | Temporal resolution: `hourly`, `daily`, `monthly`, `yearly` |
| `sync.kind` | Yes | `temporal` — data grows over time; `release` — versioned releases; `static` — never synced |
| `sync.execution` | No | `append` — new time steps appended to existing store; `rematerialize` — full rebuild on each sync |
| `sync.availability` | No | Provider availability policy — see below |

**Sync availability** — how the API determines the latest available data:

```yaml
sync:
  kind: temporal
  execution: append
  availability:
    latest_available_function: climate_api.providers.availability.lagged_latest_available
    lag_hours: 48
```

| Field | Description |
| ----- | ----------- |
| `latest_available_function` | Dotted path to a built-in availability function in `climate_api.providers.availability` |
| `lag_hours` / `lag_days` | Data is delayed by this many hours or days |
| `allow_future` | Allow requesting future dates (e.g. forecasts or projections). Default: `false` |

Omit `sync.availability` entirely for `static` datasets or when you always want to sync up to the requested end date.

**Ingestion**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `ingestion.plugin` | Yes | Dotted path to an `IngestionPlugin` class |
| `ingestion.params` | No | Constructor keyword arguments forwarded to the plugin class |

**Transforms** — applied after download, before writing to Zarr:

```yaml
transforms:
  - climate_api.transforms.kelvin_to_celsius
  - mypackage.transforms.my_custom_transform
```

See [Transforms](transforms.md) for the full pipeline description, built-in options, and how to write a custom transform.

**Spatial and temporal extents** — declares what the source dataset covers. Used to validate ingest requests before hitting the provider:

```yaml
extents:
  spatial:
    bbox: [-180, -50, 180, 50]   # [xmin, ymin, xmax, ymax] in WGS84
    crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
  temporal:
    begin: "1981-01-01"
    end: "2030-12-31"            # omit if ongoing
    trs: http://www.opengis.net/def/uom/ISO-8601/0/Gregorian
    resolution: P1D              # ISO 8601 duration: PT1H, P1D, P1M, P1Y
```

If an ingest request's bounding box has no overlap with `extents.spatial.bbox`, the API returns HTTP 400 immediately. Partial overlap is allowed — the provider will return data for the intersecting area.

**Units and display**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `units` | No | Physical units of the stored data (e.g. `mm`, `degC`, `m`) |
| `resolution` | No | Human-readable spatial resolution (e.g. `5 km x 5 km`) |
| `display.colormap` | No | Colormap name for map rendering (e.g. `blues`, `rdbu_r`) |
| `display.range` | No | `[min, max]` display range for the colormap |
| `display.nodata` | No | No-data / fill value |

**Multiscale pyramid** — pyramid Zarr stores are built automatically when the ingested dataset's spatial dimensions exceed 2048×2048 pixels. No YAML configuration is required; the pyramid level count is derived from the data shape and coarsening always uses mean aggregation.

## Step 3: Point the instance at your plugins directory

Add `plugins_dir` to your `climate-api.yaml` and place your YAML file in the `datasets/` subfolder:

```
plugins/
└── datasets/
    └── enacts_rainfall.yaml
```

```yaml
extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]

data_dir: ./data
plugins_dir: ./plugins/
```

All `*.yaml` files in `plugins_dir/datasets/` are loaded and merged with the built-in templates (CHIRPS3, ERA5-Land, WorldPop). Custom templates are additive — the built-ins remain available unless you deliberately override one by using the same `id`.

## Step 4: Ingest and publish

Once the API is running with `CLIMATE_API_CONFIG` pointing to your updated config, ingest as usual:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "enacts_rainfall_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
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
  sync:
    kind: static
  ingestion:
    plugin: mypackage.sources.my_plugin.MyPlugin
```

---

## Ingestion plugin

For sources that need streaming access or resumable long ingests, implement an `IngestionPlugin` instead of a download function. The plugin streams data directly into the Icechunk store one period at a time — no intermediate files, no full-rebuild on sync.

### Plugin skeleton

```python
# mypackage/sources/my_plugin.py
from __future__ import annotations

import asyncio
from typing import Any

import numpy as np
import xarray as xr

from climate_api.ingest.protocol import GridSpec, enumerate_periods


class MyPlugin:
    max_concurrency = 2   # fetch this many periods in parallel
    commit_batch_size = 1  # cursor checkpoint interval

    def __init__(self, variable: str) -> None:
        self.variable = variable

    def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Return grid shape and CRS without downloading data."""
        # Derive shape from known resolution, or open a small metadata request.
        xmin, ymin, xmax, ymax = bbox
        res = 0.05  # degrees per pixel
        import math
        nx = max(1, math.ceil((xmax - xmin) / res))
        ny = max(1, math.ceil((ymax - ymin) / res))
        return GridSpec(shape=(ny, nx), crs=4326, dtype=np.dtype("float32"), nodata=-9999.0)

    def periods(self, start: str, end: str) -> list[str]:
        """Return the ordered list of period IDs to fetch."""
        # enumerate_periods handles daily/hourly/monthly/yearly enumeration and
        # optional availability cutoff clamping.
        return enumerate_periods(start, end, "daily")

    def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one period. Must return a Dataset with a 'time' dimension."""
        # Blocking I/O is fine — the orchestrator runs this in asyncio.to_thread.
        ...
```

### Dataset template

```yaml
- id: my_streaming_dataset
  name: My streaming dataset
  variable: rainfall
  period_type: daily
  sync:
    kind: temporal
    execution: append
  extents:
    spatial:
      bbox: [-180, -50, 180, 50]
    temporal:
      begin: "2000-01-01"
      resolution: P1D
  ingestion:
    plugin: mypackage.sources.my_plugin.MyPlugin
    params:
      variable: rainfall
  units: mm
  resolution: 5 km x 5 km
  source: My source
```

### Key conventions for `fetch_period`

- The returned Dataset must have a `time` dimension with exactly the period's time steps as coordinate values.
- Spatial dimensions should be named `x` and `y` (or match `GridSpec.x_dim` / `GridSpec.y_dim`).
- Clear all encoding before returning and pin the time encoding: `ds["time"].encoding.update({"units": "days since 1970-01-01", "dtype": "int32"})`.
- For sources where blocking I/O is unavoidable (rioxarray, requests), run it in a `ThreadPoolExecutor` as shown above.

See the built-in plugins (`climate_api/ingest/plugins/`) for complete worked examples: `chirps3.py` (COG range requests), `era5_land.py` (remote zarr), and `worldpop.py` (full-file download).
