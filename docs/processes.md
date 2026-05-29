# Processes

The Climate API exposes two complementary processing interfaces:

- **openEO process graphs** — the primary interface for data analysis. Submit a DAG of composable operations via `POST /result` (synchronous) or `POST /jobs` (batch). 120+ standard processes are available out of the box. See the [openEO guide](openeo.md).
- **Native processes** — custom plugin functions registered via YAML, callable synchronously at `POST /processes/{id}/execution` and from openEO process graphs. Shaped to align progressively with [OGC API Processes](https://ogcapi.ogc.org/processes/).

This page documents the native processes. For process graph execution, see [openeo.md](openeo.md).

---

---

## How processes work

A process takes parameters from the JSON request body, executes a computation, and returns a JSON response. The result is typically a new derived dataset artifact that can be opened via the Zarr endpoint or published to the OGC API catalog.

```
POST /processes/{id}/execution
Content-Type: application/json

{ ...parameters... }
```

Available processes are listed at `GET /processes`.

---

## Temporal aggregation

Temporal aggregation is done via the openEO process graph interface rather than a dedicated native process. Use `load_collection → aggregate_temporal_period → save_result`:

```bash
curl -s -X POST http://127.0.0.1:8000/result \
  -H "Content-Type: application/json" \
  -d '{
    "process": {
      "process_graph": {
        "load": {
          "process_id": "load_collection",
          "arguments": {
            "id": "chirps3_precipitation_daily",
            "temporal_extent": ["2024-01-01", "2024-03-31"]
          }
        },
        "agg": {
          "process_id": "aggregate_temporal_period",
          "arguments": {
            "data": {"from_node": "load"},
            "period": "month",
            "reducer": {
              "process_graph": {
                "sum": {"process_id": "sum", "arguments": {"data": {"from_parameter": "data"}}, "result": true}
              }
            }
          }
        },
        "result": {
          "process_id": "save_result",
          "arguments": {"data": {"from_node": "agg"}, "format": "Zarr"},
          "result": true
        }
      }
    }
  }'
```

For the supported periods (`day`, `week`, `dekad`, `month`, `season`, `year`, …) and the openEO Python client equivalent, see the [openEO guide — available processes](openeo.md#available-processes).

---

## Custom processes

You can register additional processes from a `plugins_dir/processes/` directory. See [Extensibility — Processes](extensibility.md#processes) for the YAML format and execution function contract.
