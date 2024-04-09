import logging
import numpy as np
import os
import sys
import xarray as xr
from openeo.udf import XarrayDataCube

p = '/data/users/Public/emile.sonneveld/python/climate_indices-1.0.13-py2.py3-none-any.whl'
if not os.path.exists(p):
    raise Exception("Path not found: " + p)
sys.path.append(p)
from climate_indices import utils

_log = logging.getLogger("FAPAR_Anomaly_UDF")


def anomaly_calculator(input_array: np.ndarray):
    values_per_month = [[], [], [], [], [], [], [], [], [], [], [], []]  # Made with repr([[]] * 12)
    for i in range(input_array.size):
        month = i % 12
        values_per_month[month].append(input_array.data[i])

    average_per_month = list(map(lambda month_values: np.array(month_values).mean(), values_per_month))
    sd_per_month = list(map(lambda month_values: np.array(month_values).std(), values_per_month))

    array_z_score = [np.nan] * input_array.size
    for i in range(input_array.size):
        month = i % 12
        values_per_month[month].append(input_array.data[i])
        array_z_score[i] = (input_array.data[i] - average_per_month[month]) / sd_per_month[month]

    # No need to specify crs here
    return xr.DataArray(
        data=array_z_score,
        dims=["t"],
    )


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    input_array_with_bands = cube.get_array()
    # _log.warn("input_array_with_bands.shape: " + str(input_array_with_bands.shape))

    output_array_with_bands = xr.apply_ufunc(anomaly_calculator,
                                             input_array_with_bands,
                                             input_core_dims=[["t"]],
                                             output_core_dims=[["t"]],
                                             vectorize=True,
                                             )
    # _log.warning("spi_wrapped(...) ret.shape: " + str(
    #     ret.shape) + " input_array.shape: " + str(ret.shape))
    return XarrayDataCube(output_array_with_bands)


if __name__ == "__main__":
    # Test code:
    array = xr.DataArray(
        data=[1.0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
              13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30],
        dims=["t"],
    )
    array = array.expand_dims(dim='bands', axis=1).assign_coords(bands=["band_name"])
    array = array.expand_dims(dim='x', axis=1)
    array = array.expand_dims(dim='y', axis=1)

    ret = apply_datacube(XarrayDataCube(array), dict())
    print(ret)
