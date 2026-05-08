import numpy as np
import pytest
import xarray as xr

from climate_api.transforms import convert_units, deaccumulate_era5
from climate_api.transforms.unit_conversion import _CONVERSIONS


def _ds(varname: str, values: list[float], time_steps: int = 1) -> xr.Dataset:
    if time_steps > 1:
        data = np.array(values, dtype=float).reshape(time_steps, -1)
        return xr.Dataset({varname: xr.DataArray(data, dims=["time", "x"])})
    return xr.Dataset({varname: xr.DataArray(np.array(values, dtype=float))})


class TestConvertUnits:
    def test_kelvin_to_celsius(self):
        ds = _ds("t2m", [273.15, 293.15, 313.15])
        result = convert_units(ds, {"variable": "t2m", "units": "kelvin", "convert_units": "degC"})
        np.testing.assert_allclose(result["t2m"].values, [0.0, 20.0, 40.0])
        assert result["t2m"].attrs["units"] == "degC"

    def test_metres_to_mm(self):
        ds = _ds("tp", [0.001, 0.005])
        result = convert_units(ds, {"variable": "tp", "units": "m", "convert_units": "mm"})
        np.testing.assert_allclose(result["tp"].values, [1.0, 5.0])
        assert result["tp"].attrs["units"] == "mm"

    def test_no_convert_units_field_is_noop(self):
        ds = _ds("t2m", [300.0])
        result = convert_units(ds, {"variable": "t2m", "units": "kelvin"})
        np.testing.assert_array_equal(result["t2m"].values, ds["t2m"].values)

    def test_unknown_conversion_is_noop(self):
        ds = _ds("x", [1.0])
        result = convert_units(ds, {"variable": "x", "units": "foo", "convert_units": "bar"})
        np.testing.assert_array_equal(result["x"].values, ds["x"].values)

    def test_preserves_existing_attrs(self):
        ds = xr.Dataset({"t2m": xr.DataArray([300.0], attrs={"long_name": "temperature", "units": "K"})})
        result = convert_units(ds, {"variable": "t2m", "units": "kelvin", "convert_units": "degC"})
        assert result["t2m"].attrs["long_name"] == "temperature"


class TestDeaccumulateEra5:
    def test_differences_along_time(self):
        ds = _ds("tp", [0.0, 1.0, 3.0, 6.0], time_steps=4)
        result = deaccumulate_era5(ds, {"variable": "tp"})
        assert result.sizes["time"] == 3
        np.testing.assert_array_equal(result["tp"].values.flatten(), [1.0, 2.0, 3.0])

    def test_clips_negative_values(self):
        ds = _ds("tp", [3.0, 1.0, 4.0], time_steps=3)
        result = deaccumulate_era5(ds, {"variable": "tp"})
        assert (result["tp"].values >= 0).all()

    def test_preserves_attrs(self):
        data = np.array([[0.0], [1.0]])
        ds = xr.Dataset({"tp": xr.DataArray(data, dims=["time", "x"], attrs={"units": "m"})})
        result = deaccumulate_era5(ds, {"variable": "tp"})
        assert result["tp"].attrs["units"] == "m"


class TestRunTransformsPipeline:
    def test_pipeline_via_dotted_path(self):
        ds = _ds("t2m", [273.15])
        dataset = {
            "variable": "t2m",
            "units": "kelvin",
            "convert_units": "degC",
            "transforms": ["climate_api.transforms.convert_units"],
        }
        from climate_api.data_manager.services.downloader import _run_transforms
        result = _run_transforms(ds, dataset)
        np.testing.assert_allclose(result["t2m"].values, [0.0])

    def test_empty_transforms_is_noop(self):
        ds = _ds("x", [1.0, 2.0])
        from climate_api.data_manager.services.downloader import _run_transforms
        result = _run_transforms(ds, {"variable": "x", "transforms": []})
        np.testing.assert_array_equal(result["x"].values, ds["x"].values)

    def test_no_transforms_key_is_noop(self):
        ds = _ds("x", [1.0])
        from climate_api.data_manager.services.downloader import _run_transforms
        result = _run_transforms(ds, {"variable": "x"})
        np.testing.assert_array_equal(result["x"].values, ds["x"].values)

    def test_dict_entry_with_params(self):
        ds = _ds("t2m", [273.15])
        dataset = {
            "variable": "t2m",
            "units": "kelvin",
            "convert_units": "degC",
            "transforms": [{"function": "climate_api.transforms.convert_units"}],
        }
        from climate_api.data_manager.services.downloader import _run_transforms
        result = _run_transforms(ds, dataset)
        np.testing.assert_allclose(result["t2m"].values, [0.0])
