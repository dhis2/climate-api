# Vendor patch for openeo-processes-dask

Patched from upstream v2025.10.1 to unblock installation on macOS and ARM64 Linux.

## Background

`openeo-processes-dask` has a hard dependency on `rqadeforestation`. That package's
wheel is mislabelled `py3-none-any` but contains an x86-64 ELF `.so` file. The wheel
installs on every platform without error, but **fails at import time** on macOS (any
architecture — ELF is not Mach-O) and ARM64 Linux (wrong ELF class). The root-cause
bug is tracked upstream at:

  https://github.com/EarthyScience/RQADeforestation.py/issues/3

## Changes

**`pyproject.toml`** — removed `rqadeforestation` direct dependency.
It is only used by the experimental deforestation module, which we don't need.

**`openeo_processes_dask/process_implementations/experimental/__init__.py`** —
wrapped `from .rqadeforestation import *` in `try/except` so the rest of the
library loads cleanly if the package is absent.

## When to remove this patch

Once `rqadeforestation` publishes platform-specific wheels (or a pure-Python
fallback) **and** `openeo-processes-dask` publishes a new release on PyPI that
works on Python 3.13, replace this vendor copy with a direct PyPI dependency
and remove the `[tool.uv.sources]` override from `pyproject.toml`.
