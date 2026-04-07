"""Xarray-backed TiTiler router configuration"""

from titiler.xarray.factory import TilerFactory
from titiler.xarray.io import Reader
from titiler.xarray.extensions import VariablesExtension

router = TilerFactory(
    reader=Reader,
    extensions=[VariablesExtension()]
).router
