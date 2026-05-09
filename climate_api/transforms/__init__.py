"""Built-in dataset transform functions for the transforms pipeline.

Each function has the signature:
    (ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset

Functions can be referenced by their dotted module path in the dataset YAML
``transforms`` list, the same way ``ingestion.function`` works.
"""

from .deaccumulate import deaccumulate_era5
from .reproject import reproject_to_instance_crs
from .unit_conversion import convert_units

__all__ = ["convert_units", "deaccumulate_era5", "reproject_to_instance_crs"]
