"""Built-in dataset transform functions for the transforms pipeline.

Each function has the signature:
    (ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset

Functions can be referenced by their dotted module path in the dataset YAML
``transforms`` list declared on a dataset template.
"""

from .pipeline import run_dataset_transforms
from .unit_conversion import kelvin_to_celsius, metres_to_mm

__all__ = ["kelvin_to_celsius", "metres_to_mm", "run_dataset_transforms"]
