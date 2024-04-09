import os

del os.environ["GDAL_DATA"]  # Clear so that GDAL_DATA from package is used
import rasterio.crs
import rioxarray as rxr
import pandas as pd

# The following gives: Internal Proj Error: proj_create: unhandled CS type: "ellipsoidal"
# from pyproj import CRS
# crs_project = CRS.from_epsg(4326)  # WGS84

# os.environ["GDAL_DATA"] = "/home/emile/openeo/venv/lib/python3.8/site-packages/rasterio/gdal_data/"  # contains 'gcs.csv'
print('os.environ["GDAL_DATA"] = ' + repr(os.environ["GDAL_DATA"]), flush=True)
crs_project_rasterio = rasterio.crs.CRS.from_epsg(4326)

SPI_ouput_file_orig = "/home/emile/Desktop/ToShareWithVito/SPI/SPI_test.nc"
SPI_ouput_file_fixed = "/home/emile/Desktop/ToShareWithVito/SPI/SPI_test_fixed.nc"
data = rxr.open_rasterio(SPI_ouput_file_orig, masked=True)

data.rio.write_crs(crs_project_rasterio, inplace=True)
# data.rio.estimate_utm_crs()

# First timeframes are NaN, which is confusing, as it shows nothing in Q-GIS. Drop them:
data_trimmed = data.dropna(dim="time", how="all")  # Might be nice to only trim beginning and end, in all dimensions
encode = {"__xarray_dataarray_variable__": {'zlib': True, 'complevel': 4}}
data_trimmed.to_netcdf(SPI_ouput_file_fixed, encoding=encode)

"""
d = new Date(1900, 01, 01)
date2 = new Date(2021, 08, 01)
console.log((date2 - d) / 36e5); // 1065766
date3 = new Date(2022, 08, 01)
console.log((date3 - d) / 36e5); // 1074526
dateCDISample = new Date(2021, 09, 01)
console.log((dateCDISample - d) / 36e5); // 1066486 
"""
