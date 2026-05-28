import dask.array as da
import numpy as np
import xarray as xr


def get_scalar_type(obj):
    if np.isscalar(obj):
        # np.obj2sctype removed in NumPy 2.0; np.array(obj).dtype.type is equivalent
        return np.array(obj).dtype.type
    if hasattr(obj, "dtype"):
        return obj.dtype
    return np.object_
