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

Use the `href` of any child link from the catalog to open a dataset with xarray:

```python
import requests
import xarray as xr

# Get the first published dataset from the catalog
catalog = requests.get("http://127.0.0.1:8000/stac/catalog.json").json()
collection_url = next(l["href"] for l in catalog["links"] if l["rel"] == "child")

collection = requests.get(collection_url).json()

asset = collection["assets"]["zarr"]
ds = xr.open_zarr(
    asset["href"],
    consolidated=asset["xarray:open_kwargs"]["consolidated"],
)
print(ds)
```

Each dataset has a time dimension, two spatial dimensions, and one data variable matching the climate variable (e.g. `precip` for CHIRPS, `t2m` for ERA5-Land temperature). Coordinate names are preserved from the source data and vary across datasets — always detect them at runtime rather than assuming a fixed name:

- **Time**: `valid_time` (ERA5-Land) or `time` (all others)
- **Spatial**: `lat`/`lon`, `latitude`/`longitude`, or `y`/`x` depending on the source

Detect the actual names before selecting:

```python
time_dim = "valid_time" if "valid_time" in ds.coords else "time"
coords = set(ds.coords)
if "lat" in coords:
    y_dim, x_dim = "lat", "lon"
elif "latitude" in coords:
    y_dim, x_dim = "latitude", "longitude"
else:
    y_dim, x_dim = "y", "x"
```

Select a time slice:

```python
daily = ds.sel({time_dim: "2024-01-15"})
print(daily)
```

Select a spatial point closest to a location (e.g. Freetown, Sierra Leone):

```python
variable = list(ds.data_vars)[0]  # precip, t2m, tp, or pop_total depending on the dataset
point = ds.sel({y_dim: 8.48, x_dim: -13.23}, method="nearest")
print(point[variable].values)
```

Compute the spatial mean over the full domain for each time step:

```python
spatial_mean = ds[variable].mean(dim=[y_dim, x_dim])
print(spatial_mean.to_dataframe())
```

## Dataset IDs

Dataset IDs follow the pattern `{source_dataset_id}_{extent_id}`, for example `chirps3_precipitation_daily_sle` for CHIRPS3 daily precipitation over Sierra Leone (`sle`).

Available datasets in the built-in catalogue:

| Dataset ID prefix              | Variable | Period | Source          |
| ------------------------------ | -------- | ------ | --------------- |
| `chirps3_precipitation_daily`  | `precip` | Daily  | CHIRPS v3       |
| `era5land_temperature_hourly`  | `t2m`    | Hourly | ERA5-Land       |
| `era5land_precipitation_hourly`| `tp`     | Hourly | ERA5-Land       |
| `worldpop_population_yearly`   | `pop_total` | Yearly | WorldPop     |

## What's next

- See [`examples/stac_discover_and_open.py`](../examples/stac_discover_and_open.py) for a complete discovery and access script.
- See [`examples/zarr_direct_access.py`](../examples/zarr_direct_access.py) for direct Zarr access and spatial/temporal subsetting.
- See [managed_data_api_guide.md](managed_data_api_guide.md) for the full ingestion and sync API reference.
