import numpy as np
import os
import sys
import xarray as xr
from openeo.udf import XarrayDataCube

wheel_path = "/data/users/Public/emile.sonneveld/python/climate_indices-1.0.13-py2.py3-none-any.whl"
if not os.path.exists(wheel_path):
    raise Exception("Path not found: " + wheel_path)
sys.path.append(wheel_path)
import climate_indices
from climate_indices import indices

############################## SETTING PARAMETERS  for the SPI ##############################

scale = 3
distribution = climate_indices.indices.Distribution.gamma  # Fixed
data_start_year = 1980
calibration_year_initial = 1980
calibration_year_final = 2023
periodicity = climate_indices.compute.Periodicity.monthly  # Fixed

if calibration_year_final - calibration_year_initial <= 2:
    print("Gamma correction in SPI on only 2 years will give bad looking results")


def spi_wrapped(values: np.ndarray):
    return indices.spi(
        values=values,
        scale=scale,
        distribution=distribution,
        data_start_year=data_start_year,
        calibration_year_initial=calibration_year_initial,
        calibration_year_final=calibration_year_final,
        periodicity=periodicity,
    )[np.newaxis].T


def proccessingNETCDF(data: xr.DataArray):
    """Process the data to serve as input to de SPI function
    Args:
        data: netcdf file

        Returns
        DataArrayGroupBy grouped over point (y and x coordinates)
    """
    num_days_month = data.t.dt.days_in_month
    # num_days_month = 30.4  # Average number of days in a month

    # Rescaling values no longer needed
    data_precip = data
    data_precip *= 1000 * num_days_month
    data_precip = data_precip.squeeze()

    # Giving the appropriate shape to da data
    data_grouped = data_precip.stack(point=("y", "x")).groupby("point")
    print("""Data is prepared to serve as input for the SPI index.""")

    return data_grouped


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()

    data_grouped = proccessingNETCDF(array)
    spi_results = xr.apply_ufunc(
        spi_wrapped,
        data_grouped,
        # input_core_dims=[["t"]],
        # output_core_dims=[["t"]],
    )

    BAND_NAME = "SPI"
    spi_results = spi_results.expand_dims(dim="bands", axis=0).assign_coords(bands=[BAND_NAME])
    spi_results = spi_results.unstack("point")
    spi_results = spi_results.rename({"y": "lat", "x": "lon"})  # Necessary step
    spi_results = spi_results.reindex(lat=list(reversed(spi_results["lat"])))
    spi_results = spi_results.rename({"lat": "y", "lon": "x"})
    # No need to specify crs here
    return XarrayDataCube(spi_results.astype(np.float32))


# if __name__ == "__main__":
#     print("Running test code!")
#     import datetime
#     import rioxarray as rxr
#
#     now = datetime.datetime.now()
#
#     dataset = rxr.open_rasterio("/home/emile/openeo/drought-indices/SPI/out-2024-02-27_15_59_34.228187/openEO.nc")
#     if dataset.dims == ("variable", "band", "y", "x"):
#         array = dataset.swap_dims({"variable": "bands", "band": "t"})
#     elif dataset.dims == ("band", "y", "x"):
#         array = dataset.swap_dims({"band": "t"})
#     else:
#         array = dataset.swap_dims({"variable": "bands"})
#
#     array = array.astype(np.float32)
#
#     ret = apply_datacube(XarrayDataCube(array), dict())
#     arr = ret.array
#     data_crs = dataset.rio.crs
#     arr.rio.write_crs(data_crs, inplace=True)
#     arr = arr.squeeze()  # remove unneeded dimensions
#     # First timeframes are NaN, which is confusing, as it shows nothing in Q-GIS. Drop them:
#     arr = arr.dropna(dim="t", how="all")  # Might be nice to only trim beginning and end, in all dimensions
#     if len(arr.dims) > 3:
#         print("Taking only first time sample to avoid too many dimensions")
#         arr = arr.isel(t=0)
#     arr.rio.to_raster("tmp/out-" + str(now).replace(":", "_").replace(" ", "_") + ".nc")
#     print(ret)
