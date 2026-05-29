# openEO process graphs

The Climate API implements the [openEO HTTP API v1.2.0](https://openeo.org/documentation/1.0/api/) standard. Any standard openEO client can connect to it directly and execute process graphs against published collections — no Climate API-specific knowledge required.

---

## Connecting

```python
import openeo

conn = openeo.connect("http://127.0.0.1:8000")
print(conn.capabilities().api_version())  # 1.2.0
```

No authentication is required for local deployments. `openeo.connect` discovers the API via `GET /.well-known/openeo` and negotiates the version automatically.

---

## Available collections

Collections map 1:1 to published datasets. They are exposed at `/collections` and are compatible with both openEO clients and STAC browsers.

```python
for c in conn.list_collections():
    print(c["id"], "—", c["title"])
```

Each collection includes `cube:dimensions` (spatial `x`/`y`, temporal `t`, `bands`), extent, and variable metadata.

---

## Building a process graph

Process graphs are DAGs of composable operations. The openEO Python client builds them lazily — no data moves until you call `execute()` or `download()`.

```python
cube = conn.load_collection(
    "worldpop_population_yearly",
    spatial_extent={"west": -13.3, "south": 7.0, "east": -10.3, "north": 10.0},
    temporal_extent=["2015-01-01", "2021-01-01"],
    bands=["pop_total"],
)
```

Chain operations exactly as in the [openEO Python client docs](https://open-eo.github.io/openeo-python-client/):

```python
# Scale values and take the temporal maximum across the loaded years
cube = cube.apply(lambda x: x / 1_000_000).max_time()
```

---

## Synchronous execution

`POST /result` executes a process graph in the foreground and returns the result immediately. For raster results, the server returns datacube metadata (dimensions, dtype, coordinate ranges) as JSON.

```python
result = conn.execute(cube)
print(result)
# {"type": "datacube", "name": "pop_total", "dims": {"y": 3600, "x": 3600}, ...}
```

Equivalent with curl:

```bash
curl -s -X POST http://127.0.0.1:8000/result \
  -H "Content-Type: application/json" \
  -d '{
    "process": {
      "process_graph": {
        "load": {
          "process_id": "load_collection",
          "arguments": {
            "id": "worldpop_population_yearly",
            "temporal_extent": ["2020-01-01", "2021-01-01"],
            "spatial_extent": {"west": -13.3, "south": 7.0, "east": -10.3, "north": 10.0}
          }
        },
        "result": {
          "process_id": "save_result",
          "arguments": {"data": {"from_node": "load"}, "format": "Zarr"},
          "result": true
        }
      }
    }
  }'
```

---

## Batch jobs

For long-running computations, create a batch job and poll its status.

```python
job = cube.create_job(title="worldpop-max-2015-2020")
job.start_job()

# Poll until finished
import time
while (status := job.status()) not in ("finished", "error"):
    print("status:", status)
    time.sleep(2)

# Retrieve result asset links
print(job.get_results().get_assets())
```

REST equivalent:

```bash
# 1 — create
curl -s -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"process": {"process_graph": {...}}, "title": "my-job"}'

# 2 — start
curl -s -X POST http://127.0.0.1:8000/jobs/{job_id}/results

# 3 — poll
curl -s http://127.0.0.1:8000/jobs/{job_id}

# 4 — download result
curl -s http://127.0.0.1:8000/jobs/{job_id}/results
```

Completed batch jobs write their output to disk and expose it as an asset link at `GET /jobs/{id}/results/{filename}`. The output format is controlled by the `format` argument of `save_result` — see [Export formats](#export-formats) below.

---

## Available processes

`GET /processes` returns all 120+ standard openEO processes from [openeo-processes-dask](https://github.com/Open-EO/openeo-processes-dask), plus `load_collection` and `save_result` which are implemented by this backend. All processes listed are callable from process graphs.

Key processes for climate work:

| Process | What it does |
|---|---|
| `load_collection` | Open a published dataset as an openEO data cube |
| `filter_temporal` | Restrict the time dimension to an interval |
| `filter_bbox` | Restrict the spatial extent |
| `filter_bands` | Select a subset of variables/bands |
| `apply` | Apply an element-wise callback to every pixel |
| `reduce_dimension` | Collapse a dimension with a reducer (e.g. mean, sum) |
| `aggregate_temporal_period` | Group by calendar period (month, season, year) and reduce |
| `aggregate_spatial` | Zonal statistics over GeoJSON geometries |
| `resample_cube_spatial` | Reproject and resample to a target grid |
| `merge_cubes` | Combine two aligned cubes |
| `save_result` | Finalise the result — controls the output format |

---

## Export formats

The `format` argument of `save_result` controls what the server writes. `GET /file_formats` advertises all supported formats to clients.

| Format key | Title | Output type | Notes |
|---|---|---|---|
| `ZARR` | Zarr | Raster | Default. Zarr v3 directory store; served chunk-by-chunk |
| `NETCDF` | NetCDF | Raster | Raw float values — compatible with CDO, NCO, xarray, R |
| `GTIFF` | GeoTIFF | Raster | Raw float values with embedded CRS — compatible with QGIS, GDAL |
| `PNG` | PNG | Raster | Styled image using the collection's colormap and rescale range; transparent background |
| `CSV` | CSV | Raster / Vector | Tabular — ideal for time series and zonal statistics output |
| `GEOJSON` | GeoJSON | Vector | Default for `aggregate_spatial` results; one feature per geometry |
| `PARQUET` | GeoParquet | Vector | Columnar binary — efficient for large vector datasets |

```bash
# Monthly precipitation totals as NetCDF
curl -X POST http://127.0.0.1:8000/result \
  -H "Content-Type: application/json" \
  -d '{
    "process": {
      "process_graph": {
        "load": { "process_id": "load_collection", "arguments": { "id": "chirps3_precipitation_daily", "temporal_extent": ["2026-01-01", "2026-03-31"] } },
        "agg":  { "process_id": "aggregate_temporal_period", "arguments": { "data": {"from_node": "load"}, "period": "month", "reducer": { "process_graph": { "sum": { "process_id": "sum", "arguments": { "data": {"from_parameter": "data"} }, "result": true } } } } },
        "save": { "process_id": "save_result", "arguments": { "data": {"from_node": "agg"}, "format": "NetCDF" }, "result": true }
      }
    }
  }' --output monthly_precip.nc
```

---

## User-defined processes (UDPs)

UDPs are named, parameterized process graphs stored server-side. They let you define reusable pipelines and invoke them by name from any other process graph.

```bash
# Store a UDP
curl -s -X PUT http://127.0.0.1:8000/process_graphs/pop_millions \
  -H "Content-Type: application/json" \
  -d '{
    "summary": "Load WorldPop population in millions",
    "parameters": [
      {"name": "temporal_extent", "schema": {"type": "array"}}
    ],
    "process_graph": {
      "load": {
        "process_id": "load_collection",
        "arguments": {
          "id": "worldpop_population_yearly",
          "temporal_extent": {"from_parameter": "temporal_extent"}
        }
      },
      "scale": {
        "process_id": "apply",
        "arguments": {
          "data": {"from_node": "load"},
          "process": {
            "process_graph": {
              "div": {
                "process_id": "divide",
                "arguments": {"x": {"from_parameter": "x"}, "y": 1000000},
                "result": true
              }
            }
          }
        }
      },
      "result": {
        "process_id": "save_result",
        "arguments": {"data": {"from_node": "scale"}, "format": "Zarr"},
        "result": true
      }
    }
  }'

# Invoke it from another process graph
curl -s -X POST http://127.0.0.1:8000/result \
  -H "Content-Type: application/json" \
  -d '{
    "process": {
      "process_graph": {
        "run": {
          "process_id": "pop_millions",
          "arguments": {"temporal_extent": ["2020-01-01", "2025-01-01"]},
          "result": true
        }
      }
    }
  }'
```

---

## Custom processes (plugins)

Processing plugins are Python functions registered via YAML that extend the process library. A plugin with the same `id` as a standard process shadows the built-in. See [Extensibility — Processes](extensibility.md#processes) for the plugin contract.

---

## Example script

See [`examples/openeo_process_graph.py`](../examples/openeo_process_graph.py) for a complete end-to-end example using the openEO Python client.
