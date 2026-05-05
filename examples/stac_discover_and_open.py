"""Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Climate API instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import httpx
import xarray as xr

BASE_URL = "http://127.0.0.1:8000"


def list_datasets() -> list[dict]:
    """Return all child dataset links from the STAC catalog."""
    response = httpx.get(f"{BASE_URL}/stac/catalog.json")
    response.raise_for_status()
    catalog = response.json()
    return [link for link in catalog["links"] if link["rel"] == "child"]


def open_dataset(dataset_id: str) -> xr.Dataset:
    """Open a published dataset via its STAC collection asset."""
    response = httpx.get(f"{BASE_URL}/stac/collections/{dataset_id}")
    response.raise_for_status()
    collection = response.json()
    asset = collection["assets"]["zarr"]
    return xr.open_zarr(
        asset["href"],
        consolidated=asset["xarray:open_kwargs"]["consolidated"],
    )


def main() -> None:
    """Discover and open the first published dataset."""
    datasets = list_datasets()

    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    print("Published datasets:")
    for i, link in enumerate(datasets):
        print(f"  [{i}] {link['title']}  —  {link['href']}")

    # Open the first available dataset
    first = datasets[0]
    dataset_id = first["href"].rstrip("/").split("/")[-1]
    print(f"\nOpening: {first['title']} ({dataset_id})")

    ds = open_dataset(dataset_id)
    print(ds)

    # Print temporal coverage (ERA5-Land uses valid_time; CHIRPS/WorldPop use time)
    time_dim = "valid_time" if "valid_time" in ds.coords else "time"
    print(f"\nTime range: {ds[time_dim].values[0]}  →  {ds[time_dim].values[-1]}")
    print(f"Time steps: {ds.sizes[time_dim]}")

    # Spatial coordinate names vary by dataset: lon/lat, longitude/latitude, or x/y
    coords = set(ds.coords)
    if "lat" in coords:
        y_dim, x_dim = "lat", "lon"
    elif "latitude" in coords:
        y_dim, x_dim = "latitude", "longitude"
    else:
        y_dim, x_dim = "y", "x"
    print(f"Latitude:  {float(ds[y_dim].min()):.4f}  →  {float(ds[y_dim].max()):.4f}")
    print(f"Longitude: {float(ds[x_dim].min()):.4f}  →  {float(ds[x_dim].max()):.4f}")

    # Print a sample value at the spatial centre of the domain, first time step
    variable = list(ds.data_vars)[0]
    centre_y = float((ds[y_dim].min() + ds[y_dim].max()) / 2)
    centre_x = float((ds[x_dim].min() + ds[x_dim].max()) / 2)
    sample = ds[variable].isel({time_dim: 0}).sel({y_dim: centre_y, x_dim: centre_x}, method="nearest")
    print(f"\n{variable} at domain centre, t=0: {float(sample.values):.4f}")


if __name__ == "__main__":
    main()
