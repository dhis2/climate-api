# User Guide

This guide shows how to discover and access data from a running Climate API instance. It assumes the API is running locally at `http://127.0.0.1:8000` and that at least one dataset has been ingested and published.

For configuring a new instance for your country, see [setup_guide.md](setup_guide.md). For the full ingestion and sync API reference, see [managed_data_api_guide.md](managed_data_api_guide.md).

## Discovering datasets

The STAC catalog is the starting point for data discovery. It lists all published GeoZarr datasets as STAC Collections.

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq
```

Each entry in `links` with `"rel": "child"` points to one dataset collection. Use the `href` from the catalog to fetch it:

```bash
# Replace {dataset_id} with any id from the catalog above, e.g. chirps3_precipitation_daily_sle
curl -s http://127.0.0.1:8000/stac/collections/{dataset_id} | jq
```

The `assets.zarr` field contains everything needed to open the dataset:

```json
{
  "assets": {
    "zarr": {
      "href": "http://127.0.0.1:8000/zarr/chirps3_precipitation_daily_rwa",
      "xarray:open_kwargs": { "consolidated": true }
    }
  }
}
```

## Opening a dataset with xarray

The `climate_api.client` module provides a `Client` class for discovering and opening datasets:

```python
from climate_api.client import Client

api = Client("http://127.0.0.1:8000")

datasets = api.catalog()
for link in datasets:
    print(link["id"], "—", link["title"])

ds = api.open(datasets[0]["id"])  # open whichever dataset is published first
print(ds)
```

The `base_url` defaults to the `CLIMATE_API_BASE_URL` environment variable (falling back to `http://127.0.0.1:8000`), so module-level functions work without any argument when the env var is set:

```python
from climate_api.client import list_datasets, open_dataset  # reads CLIMATE_API_BASE_URL

dataset_id = list_datasets()[0]["id"]
ds = open_dataset(dataset_id)
```

Each dataset has a `time` dimension, `x` and `y` spatial dimensions, and a data variable matching the variable (e.g. `precip` for CHIRPS, `t2m` for ERA5-Land temperature).

Select the first time step:

```python
snapshot = ds.isel(time=0)
print(snapshot)
```

Select a spatial point by sampling the centre of the domain:

```python
variable = list(ds.data_vars)[0]  # precip, t2m, tp, or pop_total depending on the dataset
centre_y = ds.y.mean().item()
centre_x = ds.x.mean().item()
point = ds.sel(y=centre_y, x=centre_x, method="nearest")
print(point[variable].values)
```

Compute the spatial mean over the first 10 time steps (slicing first avoids reading the full dataset over HTTP):

```python
spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=["y", "x"])
print(spatial_mean.to_dataframe())
```

## Dataset IDs

Dataset IDs follow the pattern `{source_dataset_id}_{extent_id}`, for example `chirps3_precipitation_daily_sle` for CHIRPS3 daily precipitation over Sierra Leone (`sle`).

Available datasets in the built-in catalogue:

| Dataset ID prefix               | Variable    | Period | Source    |
| ------------------------------- | ----------- | ------ | --------- |
| `chirps3_precipitation_daily`   | `precip`    | Daily  | CHIRPS v3 |
| `era5land_temperature_hourly`   | `t2m`       | Hourly | ERA5-Land |
| `era5land_precipitation_hourly` | `tp`        | Hourly | ERA5-Land |
| `worldpop_population_yearly`    | `pop_total` | Yearly | WorldPop  |

## What's next

- See [`examples/stac_discover_and_open.py`](../examples/stac_discover_and_open.py) for a complete discovery and access script.
- See [`examples/zarr_direct_access.py`](../examples/zarr_direct_access.py) for direct Zarr access and spatial/temporal subsetting.
- See [managed_data_api_guide.md](managed_data_api_guide.md) for the full ingestion and sync API reference.
