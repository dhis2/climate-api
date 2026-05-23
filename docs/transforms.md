# Transforms

Transforms are functions applied to a dataset **after download and before the Zarr store is written**. They handle things like unit conversion that would be awkward or costly to do at read time.

---

## How transforms work

When an ingestion runs, the pipeline is:

```
download → (transforms) → write Zarr
```

Transforms are applied in declaration order. Each function receives the full `xr.Dataset` and the dataset template dict, and returns a (possibly modified) `xr.Dataset`. The modified dataset is then passed to the next transform, or written to Zarr if there are no more.

Transforms are declared in the dataset YAML as a list of dotted Python paths:

```yaml
transforms:
  - climate_api.transforms.kelvin_to_celsius
  - mypackage.transforms.clip_to_valid_range
```

---

## Built-in transforms

### `climate_api.transforms.kelvin_to_celsius`

Converts the dataset's primary variable from Kelvin to degrees Celsius.

```
°C = K − 273.15
```

Used by: ERA5-Land 2 m temperature (`era5land_temperature_hourly`).

### `climate_api.transforms.metres_to_mm`

Converts the dataset's primary variable from metres to millimetres.

```
mm = m × 1000
```

Used by: ERA5-Land total precipitation (`era5land_precipitation_hourly`).

---

## Reprojection

No automatic reprojection occurs. Data is stored in whatever CRS the plugin returns (declared via `GridSpec.crs` in `probe()`).

If you need to reproject to a different CRS, declare `reproject_to_instance_crs` as an explicit transform. It reprojects from `source_crs` (default `EPSG:4326`) to the instance CRS set in `climate-api.yaml`:

```yaml
transforms:
  - function: climate_api.transforms.reproject_to_instance_crs
    params:
      source_crs: EPSG:32633
```

---

## Passing parameters to a transform

If a transform needs configuration, use the dict form instead of a bare dotted path:

```yaml
transforms:
  - function: mypackage.transforms.scale_variable
    params:
      factor: 0.01
      units: m
```

The `params` dict is forwarded to the function as extra keyword arguments:

```python
def scale_variable(ds: xr.Dataset, dataset: dict[str, Any], *, factor: float, units: str) -> xr.Dataset:
    ...
```

---

## Writing a custom transform

A transform is any callable with this signature:

```python
import xarray as xr
from typing import Any

def my_transform(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    varname = dataset["variable"]
    return ds.assign({varname: ds[varname].clip(min=0)})
```

`dataset` is the full template dict, so you can read `dataset["variable"]`, `dataset["units"]`, or any other field declared in the YAML.

The function can live in any importable package, or in a Python module placed directly under `plugins_dir` (which is added to `sys.path` automatically). Reference it by its dotted path:

```yaml
transforms:
  - myplugin.transforms.my_transform
```

For built-in and custom transform examples, see [Extensibility — Transform functions](extensibility.md#transform-functions).
