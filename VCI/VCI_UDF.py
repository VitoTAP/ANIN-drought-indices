import logging
import numpy as np
import os
import sys
import xarray as xr
from openeo.udf import XarrayDataCube

_log = logging.getLogger("VCI_UDF")


def calculator_function(input_array: np.ndarray):
    min_per_month = [np.nan] * 12
    max_per_month = [np.nan] * 12
    for i in range(input_array.size):
        month = i % 12
        val = input_array.data[i]
        if np.isnan(min_per_month[month]):
            min_per_month[month] = val
        else:
            min_per_month[month] = min(min_per_month[month], val)

        if np.isnan(max_per_month[month]):
            max_per_month[month] = val
        else:
            max_per_month[month] = max(max_per_month[month], val)

    output_array = [np.nan] * input_array.size
    for i in range(input_array.size):
        month = i % 12
        reach = max_per_month[month] - min_per_month[month]
        if reach == 0:
            # Avoid ZeroDivisionError: float division by zero
            output_array[i] = np.nan
        else:
            output_array[i] = (input_array.data[i] - min_per_month[month]) / reach

    # No need to specify crs here
    return xr.DataArray(
        data=output_array,
        dims=["t"],
    )


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    input_array_with_bands = cube.get_array()

    output_array_with_bands = xr.apply_ufunc(calculator_function,
                                             input_array_with_bands,
                                             input_core_dims=[["t"]],
                                             output_core_dims=[["t"]],
                                             vectorize=True,
                                             )
    return XarrayDataCube(output_array_with_bands)


if __name__ == "__main__":
    # Test code:
    array = xr.DataArray(
        data=[
            [[-101, 1, 101],
             [-102, 2, 102],
             [-103, 3, 103]],
            [[np.nan, 1, 101],
             [np.nan, 2, 102],
             [np.nan, 3, 103]],
            300 + np.random.randn(3, 3) + 5,
            400 + np.random.randn(3, 3) + 5,
            500 + np.random.randn(3, 3) + 5,
            600 + np.random.randn(3, 3) + 5,
            700 + np.random.randn(3, 3) + 5,
            800 + np.random.randn(3, 3) + 5,
            900 + np.random.randn(3, 3) + 5,
            1000 + np.random.randn(3, 3) + 5,
            1200 + np.random.randn(3, 3) + 5,
            1300 + np.random.randn(3, 3) + 5,
            1400 + np.random.randn(3, 3) + 5,
            1500 + np.random.randn(3, 3) + 5,
            1600 + np.random.randn(3, 3) + 5,
            1700 + np.random.randn(3, 3) + 5,
            1800 + np.random.randn(3, 3) + 5,
            1900 + np.random.randn(3, 3) + 5,
        ],
        dims=["t", "x", "y"],
    )
    # Sample time slice:    array.sel(t=0).data

    array = array.expand_dims(dim='bands', axis=1).assign_coords(bands=["band01"])

    ret = apply_datacube(XarrayDataCube(array), dict()).array
    print(ret)
