"""Unit conversion transform: scale + offset applied to the dataset variable."""

import logging
from typing import Any

import xarray as xr

logger = logging.getLogger(__name__)

# (from_units, to_units) -> (display_label, scale, offset)
# Applied as: converted = original * scale + offset
_CONVERSIONS: dict[tuple[str, str], tuple[str, float, float]] = {
    ("kelvin", "degc"): ("degC", 1.0, -273.15),
    ("m", "mm"): ("mm", 1000.0, 0.0),
}


def convert_units(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    """Convert the dataset variable from ``units`` to ``convert_units``.

    Reads ``units`` and ``convert_units`` from the dataset template dict.
    Returns the dataset unchanged if either field is absent or the conversion
    is not registered in ``_CONVERSIONS``.
    """
    convert_to = dataset.get("convert_units")
    if not convert_to:
        return ds
    units = dataset.get("units", "")
    key = (units.lower(), convert_to.lower())
    conversion = _CONVERSIONS.get(key)
    if conversion is None:
        logger.warning("No unit conversion registered for %s -> %s; skipping", units, convert_to)
        return ds
    label, scale, offset = conversion
    varname = dataset["variable"]
    logger.info("Converting %s from %s to %s", varname, units, label)
    da = ds[varname]
    converted = da * scale + offset if scale != 1.0 else da + offset
    return ds.assign({varname: converted.assign_attrs({**da.attrs, "units": label})})
