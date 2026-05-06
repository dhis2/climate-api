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
# Replace the URL with the href of any child link from the catalog above
curl -s http://127.0.0.1:8000/stac/collections/chirps3_precipitation_daily_rwa | jq
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

for link in api.catalog():
    print(link["title"], "—", link["href"])

ds = api.open("chirps3_precipitation_daily_rwa")
print(ds)
```

The `base_url` defaults to the `CLIMATE_API_BASE_URL` environment variable (falling back to `http://127.0.0.1:8000`), so module-level functions work without any argument when the env var is set:

```python
from climate_api.client import open_dataset  # reads CLIMATE_API_BASE_URL

ds = open_dataset("chirps3_precipitation_daily_rwa")
```

Each dataset has a `time` dimension, `longitude` and `latitude` spatial dimensions, and a data variable matching the variable (e.g. `precip` for CHIRPS, `t2m` for ERA5-Land temperature).

Select the first time step:

```python
snapshot = ds.isel(time=0)
print(snapshot)
```

Select a spatial point by sampling the centre of the domain:

```python
variable = list(ds.data_vars)[0]  # precip, t2m, tp, or pop_total depending on the dataset
centre_lat = ds.latitude.mean().item()
centre_lon = ds.longitude.mean().item()
point = ds.sel(latitude=centre_lat, longitude=centre_lon, method="nearest")
print(point[variable].values)
```

Compute the spatial mean over the first 10 time steps (slicing first avoids reading the full dataset over HTTP):

```python
spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=["latitude", "longitude"])
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
