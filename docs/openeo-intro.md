# openEO — A Team Introduction

openEO is an **open standard API** for accessing and processing Earth Observation (EO) data. Instead of downloading raw satellite or climate data and writing custom processing scripts, you describe *what you want to compute* as a process graph, and the server runs it for you on its own data.

---

## Why openEO?

Traditional EO data access is fragmented: each data provider has its own API, format, and tools. openEO solves this by defining a vendor-neutral HTTP API so the same client code works against any compliant backend.

## Why openEO for the Open Climate Service?

The Open Climate Service stores climate datasets — precipitation, temperature, population — as managed Zarr/Icechunk stores and exposes them to DHIS2 users and applications. openEO gives us a standardised, well-documented way to query and transform those datasets without building a bespoke query language.

Concretely it means:
- **DHIS2 analytics apps** can request district-level climate aggregates (monthly sum, seasonal mean) without downloading raw daily rasters — the computation runs server-side and returns a small result.
- **Data scientists** can use the standard openEO Python client or web editor directly against the service without learning a DHIS2-specific API.
- **New datasets** added to the service are immediately queryable through the same process graph interface, with no additional API work.
- **Interoperability** — process graphs written for the Open Climate Service work, with minor configuration changes, against any other openEO-compliant backend (Copernicus CDSE, EarthServer, etc.), and vice versa.

---

## Key concepts

### Collections

A **collection** is a dataset available on the backend — equivalent to a STAC collection. Each collection has a spatial and temporal extent, a list of variables (bands), and metadata describing its dimensions.

```
GET /collections                               → list all available datasets
GET /collections/chirps3_precipitation_daily   → metadata for one dataset
```

### Processes

A **process** is a single operation — `load_collection`, `filter_temporal`, `reduce_dimension`, `save_result`, etc. The backend exposes 120+ standard processes from the [openEO process specification](https://processes.openeo.org/) plus any custom ones registered by the operator.

```
GET /processes                        → full catalogue
GET /processes/reduce_dimension       → spec for one process
```

### Process graphs

A **process graph** is a directed acyclic graph (DAG) of connected processes — the openEO equivalent of a pipeline or workflow. Nodes are processes; edges connect output of one node to input of the next.

Example — compute monthly precipitation sum for a bounding box:

```json
{
  "load": {
    "process_id": "load_collection",
    "arguments": {
      "id": "chirps3_precipitation_daily",
      "spatial_extent": {"west": -13.3, "south": 7.8, "east": -10.3, "north": 9.9},
      "temporal_extent": ["2025-01-01", "2025-12-31"]
    }
  },
  "aggregate": {
    "process_id": "aggregate_temporal_period",
    "arguments": {
      "data": {"from_node": "load"},
      "period": "month",
      "reducer": {"process_graph": {"sum": {"process_id": "sum", "arguments": {"data": {"from_parameter": "data"}}, "result": true}}}
    }
  },
  "save": {
    "process_id": "save_result",
    "arguments": {"data": {"from_node": "aggregate"}, "format": "NetCDF"},
    "result": true
  }
}
```

### Jobs

A **batch job** runs a process graph asynchronously. You create it, start it, poll for status, then download results.

```
POST /jobs                  → create job (returns job_id)
POST /jobs/{id}/results     → start execution
GET  /jobs/{id}             → poll status (queued → running → finished)
GET  /jobs/{id}/results     → download output files
DELETE /jobs/{id}           → clean up
```

### Synchronous execution

For quick queries, `POST /result` runs the process graph immediately and returns the output in the HTTP response body — no polling needed. Practical for small spatial extents or short time ranges.

### User-Defined Processes (UDPs)

UDPs let you store a named, reusable process graph on the backend — think of them as stored procedures. Once stored via `PUT /process_graphs/{id}`, the UDP can be called like any built-in process.

---

## Interacting with the Open Climate Service

### Web editor

Open [editor.openeo.org](https://editor.openeo.org) and connect to your running instance:

```
http://localhost:8000
```

Or use the shortcut at `GET /openeo` which redirects automatically. The editor provides a visual graph builder, a process catalogue browser, and a job manager.

### Python client

```bash
pip install openeo
```

```python
import openeo

conn = openeo.connect("http://localhost:8000")

cube = (
    conn.load_collection(
        "chirps3_precipitation_daily",
        spatial_extent={"west": -13.3, "south": 7.8, "east": -10.3, "north": 9.9},
        temporal_extent=["2025-01-01", "2025-12-31"],
    )
    .aggregate_temporal_period(period="month", reducer="sum")
)

# Synchronous — download immediately
cube.download("monthly_precip.nc", format="NetCDF")

# Batch job — run asynchronously
job = cube.save_result("NetCDF").create_job(title="Monthly precip 2025")
job.start_and_wait()
job.get_results().download_files("./output/")
```

See [`examples/openeo_process_graph.py`](../examples/openeo_process_graph.py) for a full walkthrough, and [`examples/zonal_statistics.py`](../examples/zonal_statistics.py) for computing district-level statistics with DHIS2 organisation unit IDs.

### Supported output formats

| Format | `save_result(format=…)` | Use case |
|--------|------------------------|----------|
| Zarr | `"ZARR"` | Default; chunk-based, streamable |
| NetCDF | `"NetCDF"` | Interoperability with GIS tools |
| GeoTIFF | `"GTiff"` | Single time step rasters |
| PNG | `"PNG"` | Visualisation; styled with collection colormap |
| CSV | `"CSV"` | Tabular summaries |
| GeoJSON | `"GeoJSON"` | Vector output after spatial aggregation |
| GeoParquet | `"Parquet"` | Large tabular outputs |

---

## How the Open Climate Service implements openEO

```
openEO client
      │
      ▼
POST /result  ──────────────────────────────────────► immediate response
POST /jobs → POST /jobs/{id}/results → GET /jobs/{id}/results
      │
      ▼
openeo-pg-parser-networkx   ← parses the process graph DAG
      │
      ▼
openeo-processes-dask       ← executes each node (120+ standard processes)
      │
      ▼
load_collection             ← reads from Icechunk/Zarr managed dataset store
      │
      ▼
save_result                 ← writes output file; returns asset href
```

openEO is an additional access layer on top of the existing dataset store — the same data served via the native ingestion and sync endpoints is available through process graphs with no duplication.

---

## Resources

| Resource | Link |
|---|---|
| openEO.org — overview and use cases | <https://openeo.org> |
| API specification (v1.2.0) | <https://openeo.org/documentation/1.0/api/> |
| Standard process catalogue | <https://processes.openeo.org> |
| Python client documentation | <https://open-eo.github.io/openeo-python-client/> |
| Web editor (hosted) | <https://editor.openeo.org> |
| openEO cookbook (Python examples) | <https://openeo.org/documentation/1.0/cookbook/> |
| openeo-processes-dask (our execution engine) | <https://github.com/Open-EO/openeo-processes-dask> |
| Open Climate Service openEO guide | [`docs/openeo.md`](openeo.md) |
| Full Python client example | [`examples/openeo_process_graph.py`](../examples/openeo_process_graph.py) |
| Zonal statistics example | [`examples/zonal_statistics.py`](../examples/zonal_statistics.py) |
