import logging
import numpy as np
import os
import sys
import xarray as xr
from openeo.udf import XarrayDataCube

_log = logging.getLogger("python_UDF")


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()
    _log.warning("array.shape: " + str(array.shape))
    # Not sure if shifting one month or one day.
    array.shift(t=1)  # Probably uses NaN as filler value
    _log.warning("array.shape: " + str(array.shape))
    _log.warning("array.dims: " + ", ".join(array.dims))

    # num_days_month = array.t.dt.days_in_month
    # num_days_month = 30

    # No need to specify crs here
    return XarrayDataCube(array)
