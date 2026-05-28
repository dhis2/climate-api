# Vendor patch for openeo-processes-dask

Patched from upstream v2025.10.1 to unblock installation on Python 3.13 / ARM64.

## Changes

**`pyproject.toml`** — removed `rqadeforestation` direct dependency.
The package contains an ARM64-incompatible native binary and is only used by
the experimental deforestation module, which we don't need.

**`openeo_processes_dask/process_implementations/experimental/__init__.py`** —
wrapped `from .rqadeforestation import *` in `try/except` so the rest of the
library loads cleanly if the package is absent.

## When to remove this patch

Once upstream PR #372 (Python 3.13 / zarr v3 / NumPy 2 support) is merged and
released, replace this vendor copy with a direct PyPI dependency and remove
the `[tool.uv.sources]` override from `pyproject.toml`.
