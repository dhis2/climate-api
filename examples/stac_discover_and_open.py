"""
Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Climate API instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import requests
import xarray as xr

BASE_URL = "http://127.0.0.1:8000"


def list_datasets() -> list[dict]:
    catalog = requests.get(f"{BASE_URL}/stac/catalog.json").json()
    return [link for link in catalog["links"] if link["rel"] == "child"]


def open_dataset(dataset_id: str) -> xr.Dataset:
    collection = requests.get(f"{BASE_URL}/stac/collections/{dataset_id}").json()
    asset = collection["assets"]["zarr"]
    return xr.open_zarr(
        asset["href"],
        consolidated=asset["xarray:open_kwargs"]["consolidated"],
    )


def main() -> None:
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

    # Print temporal coverage
    print(f"\nTime range: {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"Time steps: {ds.sizes['time']}")

    # Print spatial coverage (coordinates are named x/longitude and y/latitude)
    y_dim = "latitude" if "latitude" in ds.coords else "y"
    x_dim = "longitude" if "longitude" in ds.coords else "x"
    print(f"Latitude:  {float(ds[y_dim].min()):.4f}  →  {float(ds[y_dim].max()):.4f}")
    print(f"Longitude: {float(ds[x_dim].min()):.4f}  →  {float(ds[x_dim].max()):.4f}")

    # Print a sample value at the spatial centre of the domain, first time step
    variable = list(ds.data_vars)[0]
    centre_y = float((ds[y_dim].min() + ds[y_dim].max()) / 2)
    centre_x = float((ds[x_dim].min() + ds[x_dim].max()) / 2)
    sample = ds[variable].isel(time=0).sel(
        {y_dim: centre_y, x_dim: centre_x}, method="nearest"
    )
    print(f"\n{variable} at domain centre, t=0: {float(sample.values):.4f}")


if __name__ == "__main__":
    main()
