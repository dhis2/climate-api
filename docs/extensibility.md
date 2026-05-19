# Extensibility

The Climate API is designed around a consistent plugin pattern: built-in behaviour lives in the package, and custom behaviour is layered on top through a `plugins_dir` directory and Python dotted paths — without forking or patching core code.

The same pattern applies at every extension point:

| Extension point | How to extend | Plugin location |
| --------------- | ------------- | --------------- |
| [Dataset templates](#dataset-templates) | YAML files | `plugins_dir/datasets/` |
| [Ingestion plugins](#ingestion-plugins) | Python class implementing `IngestionPlugin` | any importable path |
| [Transform functions](#transform-functions) | Python function, dotted path in YAML | any importable path |
| [Processes](#processes) | YAML file + Python function | `plugins_dir/processes/` |

---

## Dataset templates

Dataset templates are YAML files that describe a data source. Built-ins live in the package (`climate_api/data/datasets/`). Custom templates are loaded from `plugins_dir/datasets/`.

```
plugins/
└── datasets/
    └── enacts_rainfall.yaml
```

```yaml
# climate-api.yaml
plugins_dir: ./plugins/
```

All `*.yaml` files in `plugins_dir/datasets/` are merged with the built-ins. A custom template with the same `id` as a built-in overrides it — useful for adjusting lag times, display ranges, or availability settings on an existing dataset.

See [Adding custom datasets](adding_custom_datasets.md) for the full template field reference.

---

## Ingestion plugins

The `ingestion.plugin` field in a dataset template is a dotted Python path to an `IngestionPlugin` class. The plugin streams data directly into the Icechunk store one period at a time — no intermediate files, resumable on restart.

```yaml
ingestion:
  plugin: mypackage.sources.MyPlugin
  params:
    variable: rainfall
    stage: final
```

### Plugin protocol

A plugin implements three focused async methods. The Climate API layer owns the orchestration loop — plugins never write to zarr or Icechunk directly:

```python
from climate_api.ingest.protocol import GridSpec
import xarray as xr

class MyPlugin:
    max_concurrency: int = 1    # parallel fetch limit
    commit_batch_size: int = 1  # periods per Icechunk commit

    async def probe(self, bbox: list[float], **params) -> GridSpec:
        """Metadata-only source probe. Returns grid shape, CRS, dtype. No data transfer."""
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        """Return the ordered list of available period IDs from start to end."""
        ...

    async def fetch_period(self, period_id: str, bbox: list[float], **params) -> xr.Dataset:
        """Fetch one period. Return a dataset with a 'time' dimension in source CRS."""
        ...
```

**`GridSpec`** is the return type of `probe()`:

```python
@dataclass
class GridSpec:
    shape: tuple[int, int]       # (ny, nx) grid dimensions
    crs: int                     # EPSG code, e.g. 4326 or 32633
    dtype: np.dtype              # data type, e.g. np.dtype("float32")
    nodata: float | None = None  # fill value
    time_dim: bool = True        # False for static (time-invariant) datasets
    extra_dims: dict[str, int] = field(default_factory=dict)  # e.g. {"age_group": 20}
```

Set `time_dim=False` for static (time-invariant) datasets — the orchestrator issues a single write with no append dimension.

### What the orchestrator does

1. Calls `probe()` once to fix the Icechunk store's chunk shape and write GeoZarr attributes.
2. Calls `periods()` once to get the full period list; filters against already-committed time coordinates.
3. Creates all fetch tasks upfront so up to `max_concurrency` fetches are in flight simultaneously.
4. Awaits tasks in chronological order so writes are always sequential.
5. Commits to the Icechunk store after every `commit_batch_size` periods.
6. On restart, resumes from the last committed period — a crash loses at most one uncommitted batch.
7. After all periods are written, runs a rechunk pass if the plugin declares `rechunk_time`, then expires intermediate Icechunk snapshots to prune history.

See [Adding custom datasets](adding_custom_datasets.md#ingestion-plugin) for a worked example.

---

## Transform functions

Transforms are functions applied to a dataset after download and before the Zarr store is written. They are declared as a list of dotted paths in the dataset template:

```yaml
transforms:
  - climate_api.transforms.kelvin_to_celsius
  - mypackage.transforms.clamp_negatives
```

Each transform receives the `xr.Dataset` and the dataset template dict, and returns a (possibly modified) `xr.Dataset`:

```python
import xarray as xr
from typing import Any

def clamp_negatives(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    varname = dataset["variable"]
    return ds.assign({varname: ds[varname].clip(min=0)})
```

Dict entries with params are also supported:

```yaml
transforms:
  - function: mypackage.transforms.scale
    params:
      factor: 0.01
```

The `params` dict is forwarded as keyword arguments to the transform function. Custom transforms can live in any importable package or in a module under `plugins_dir`.

For the built-in transforms and a full description of the pipeline, see [Transforms](transforms.md).

---

## Processes

Processes are named operations that produce derived datasets (e.g. temporal resampling). They are backed by YAML files and dispatched via `POST /processes/{id}/execution`.

Built-in processes live in `climate_api/data/processes/`. Custom processes are loaded from `plugins_dir/processes/`.

```
plugins/
└── processes/
    └── my_process.yaml
```

### Process YAML

```yaml
- id: my_process
  title: My custom process
  description: Describe what this process does.
  version: "0.1.0"
  expose: true
  jobControlOptions:
    - sync-execute
  execution:
    function: mypackage.processes.my_process.execute
```

| Field | Required | Description |
| ----- | -------- | ----------- |
| `id` | Yes | Unique process identifier. Used in `POST /processes/{id}/execution` |
| `title` | Yes | Human-readable title exposed through the public process catalogue |
| `description` | No | Longer description shown in API responses |
| `version` | No | Process version string exposed through the public process description |
| `expose` | No | Whether the process appears in the public `/processes` listing. Default: `true` |
| `jobControlOptions` | No | Supported execution modes exposed publicly. Default: `["sync-execute"]` |
| `execution.function` | Yes | Dotted path to the Python function that runs the process |

A custom process with the same `id` as a built-in overrides it.

### Execution function

The current built-in execution path accepts the raw JSON request body as keyword arguments and returns a JSON-serialisable dict:

```python
from typing import Any

def execute(*, source_dataset_id: str, factor: float, **_ignored: Any) -> dict[str, Any]:
    ...
    return {"status": "completed", "artifact_id": "..."}
```

Invalid or missing arguments raise `TypeError`, which the route dispatcher catches and returns as HTTP 400.

For the built-in `resample` process and usage examples, see [Processes](processes.md).

---

## What is not pluggable

**Availability functions** (`sync.availability.latest_available_function`) accept a dotted path but only resolve built-in functions in `climate_api.providers.availability`. Plugin paths are not reliably supported — the path is resolved without `plugins_dir` on `sys.path`. Use one of the built-in availability functions instead, or open an issue if a new provider cadence is needed.

See [issue #95](https://github.com/dhis2/climate-api/issues/95) for the planned fix.
