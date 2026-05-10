# Extensibility

The Climate API is designed around a consistent plugin pattern: built-in behaviour lives in the package, and custom behaviour is layered on top through a `plugins_dir` directory and Python dotted paths — without forking or patching core code.

The same pattern applies at every extension point:

| Extension point | How to extend | Plugin location |
| --------------- | ------------- | --------------- |
| [Dataset templates](#dataset-templates) | YAML files | `plugins_dir/datasets/` |
| [Ingestion functions](#ingestion-functions) | Python function, dotted path in YAML | any importable path |
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

## Ingestion functions

The `ingestion.function` field in a dataset template is a dotted Python path to the download function that fetches data for that dataset.

```yaml
ingestion:
  function: mypackage.sources.enacts.download
```

The function must follow the download function contract (see [Adding custom datasets](adding_custom_datasets.md#step-1-write-the-download-function)). It can live anywhere that is importable — either an installed package or a module placed directly under `plugins_dir` (which is automatically added to `sys.path`).

---

## Transform functions

Transforms are functions applied to a dataset after download and before the Zarr store is written. They are declared as a list of dotted paths in the dataset template:

```yaml
transforms:
  - climate_api.transforms.kelvin_to_celsius
  - mypackage.transforms.clip_to_bbox
```

Each transform receives the `xr.Dataset` and the dataset template dict, and returns a (possibly modified) `xr.Dataset`:

```python
import xarray as xr
from typing import Any

def clip_to_bbox(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
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
  name: My custom process
  description: Describe what this process does.
  execution_function: mypackage.processes.my_process.execute
```

| Field | Required | Description |
| ----- | -------- | ----------- |
| `id` | Yes | Unique process identifier. Used in `POST /processes/{id}/execution` |
| `name` | Yes | Human-readable name |
| `description` | No | Longer description shown in API responses |
| `execution_function` | Yes | Dotted path to the Python function that runs the process |

A custom process with the same `id` as a built-in overrides it.

### Execution function

The execution function receives the raw JSON request body as keyword arguments and returns a JSON-serialisable dict:

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
