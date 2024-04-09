import os

del os.environ["GDAL_DATA"]  # Clear so that GDAL_DATA from package is used
import rasterio.crs
import rioxarray as rxr
import pandas as pd
import numpy as np
import scipy

# The following gives: Internal Proj Error: proj_create: unhandled CS type: "ellipsoidal"
# from pyproj import CRS
# crs_project = CRS.from_epsg(4326)  # WGS84

# os.environ["GDAL_DATA"] = "/home/emile/openeo/venv/lib/python3.8/site-packages/rasterio/gdal_data/"  # contains 'gcs.csv'
print('os.environ["GDAL_DATA"] = ' + repr(os.environ["GDAL_DATA"]), flush=True)


SPI_ouput_file_orig = "/home/emile/openeo/openeo-geopyspark-driver/tests/integrations/tmp/2023-12-11 16_37_14.502791_ekaterina_AABB.json.tiff"
SPI_ouput_file_fixed = "/home/emile/openeo/openeo-geopyspark-driver/tests/integrations/tmp/2023-12-11 16_37_14.502791_ekaterina_AABB.json_FIXED.tiff"


data = rxr.open_rasterio(SPI_ouput_file_orig, masked=True)

# data.rio.estimate_utm_crs()
data.data = scipy.ndimage.gaussian_filter(data, sigma=3)
data = np.ceil((data - 5) / 200) * 200 + 5  # 100Mb->1Mb
# data = data * 0
data2 = data.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)

# First timeframes are NaN, which is confusing, as it shows nothing in Q-GIS. Drop them:
# data_trimmed = data.dropna(dim="time", how="all")  # Might be nice to only trim beginning and end, in all dimensions
data_trimmed = data

encode = {"__xarray_dataarray_variable__": {'zlib': True, 'complevel': 5}}
data_trimmed.rio.to_raster(SPI_ouput_file_fixed)  # , encoding=encode
"""
d = new Date(1900, 01, 01)
date2 = new Date(2021, 08, 01)
console.log((date2 - d) / 36e5); // 1065766
date3 = new Date(2022, 08, 01)
console.log((date3 - d) / 36e5); // 1074526
dateCDISample = new Date(2021, 09, 01)
console.log((dateCDISample - d) / 36e5); // 1066486 
"""
