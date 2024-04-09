import glob
import os

del os.environ["GDAL_DATA"]  # Clear so that GDAL_DATA from package is used

# import numpy as np
import xarray as xr
import rioxarray
import rasterio
import numpy as np
from rioxarray.merge import merge_arrays  # Note: You can merge datasets with the merge_datasets method

import matplotlib.pyplot as plt

import osgeo.gdal
import osgeo.ogr
import osgeo.osr

band_dictionary = [
    {
        "agera5_name": "dewpoint-temperature",
        "era5land_name": "2_metre_dewpoint_temperature",
        "gmv_name": "d2m",
    },
    {
        "agera5_name": "vapour-pressure",
        "era5land_name": "surface_pressure",
        "gmv_name": "sp",
    },
    {
        "agera5_name": "solar-radiation-flux",
        "era5land_name": "surface_solar_radiation_downwards",
        "gmv_name": "ssrd",  # "Surface short-wave (solar) radiation downwards"
    },
    {
        "agera5_name": "wind-speed",
        # "era5land_name": ['10m_u_component_of_wind', '10m_v_component_of_wind'],  # Johan
        "era5land_name": ['10_metre_u_wind_component', '10_metre_v_wind_component'],  # Emile
        "gmv_name": ['u10', 'v10'],
    },
    {
        "agera5_name": "wind-speed U",
        "era5land_name": '10_metre_u_wind_component',
        "gmv_name": 'u10',
    },
    {
        "agera5_name": "wind-speed V",
        "era5land_name": '10_metre_v_wind_component',
        "gmv_name": 'v10',
    },
    {
        "agera5_name": "precipitation-flux",
        "era5land_name": "total_precipitation",
        "gmv_name": "tp",
    },
    {
        "agera5_name": "temperature-min",
        "era5land_name": "2m_temperature_min",
    },
    {
        "agera5_name": "temperature-mean",
        "era5land_name": "2m_temperature_mean",
    },
    {
        "agera5_name": "temperature-max",
        "era5land_name": "2m_temperature_max",
    },
]

pathGMV = "/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-ANIN/tiff_collection_unscale/2020/01/01/"
pathOEO = "/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v2/tiff_collection/2020/01/01/"
pathOEO = pathGMV


def MSE(a, b):
    # https://github.com/libvips/pyvips/issues/296
    err2 = (b - a) ** 2
    return float(err2.mean())
    # err = err2.sum() ** (1 / 2)
    # return float(err/ len(err2))
    # return float(err/ len(err2))
    # return err.mean(skipna=True).values.take(0)


#
# year = "1980"
# month = "01"
# day = "01"
# for band_struct in band_dictionary:
#     # band_struct = {
#     #     "agera5_name": "dewpoint-temperature",
#     #     "era5land_name": "2_metre_dewpoint_temperature",
#     #     "gmv_name": "d2m",
#     # }
#     if (
#             isinstance(band_struct["era5land_name"], list)
#             or "era5land_name" not in band_struct
#             or "gmv_name" not in band_struct
#     ):
#         continue
#     gmv_name = band_struct["gmv_name"]
#     era5land_name = band_struct["era5land_name"]
#     print(f"{gmv_name=}")
# tiff_path_gmv = f"/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-ANIN/tiff_collection/{year}/{month}/{day}/{year}-{month}-{day}_{gmv_name}.tif"
# tiff_path_oeo = f"/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v4/tiff_collection/{year}/{month}/{day}/{year}-{month}-{day}_{era5land_name}.tiff"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/VCI/VCI_with_Crop_Mask/ANIN_VCI_300m_SouthAfrica_202007.tif"
# tiff_path_oeo = "/home/emile/openeo/openeo-geopyspark-driver/tests/integrations/tmp_test_run_graph/2024-02-20_23_05_20.650711_process_graph.json.nc"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/VCI/VCI_with_Crop_Mask/ANIN_VCI_300m_SouthAfrica_202207.tif"
# tiff_path_oeo = "/home/emile/openeo/drought-indices/VCI/out-2024-02-21_18_09_30.873493/openEO_2022-07-01Z.tif"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/CDI/FAPAR_anomaly_RT2/April2022_FAPAR_Anomaly_crop_mask.tif"
# tiff_path_oeo = "/home/emile/openeo/drought-indices/FAPAR_Anomaly/out-2024-02-18_21_24_18.987372/openEO_2022-04-01Z.tif"
# tiff_path_oeo = "/home/emile/Downloads/openEO_2022-04-01Z.tif"

# arrOEO = "/home/emile/openeo/drought-indices/SPEI/out-2024-02-14_10_22_46.460937/openEO.nc"
# arrGMV = "/home/emile/Desktop/ToShareWithVito/SPEI/outputs/SPEI3.nc"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/SPI/outputs/nc_to_tiffs/2022-01-01_.tiff"
# tiff_path_oeo = "/home/emile/openeo/drought-indices/SPEI/out-2024-02-27_12_27_06.508531/openEO_2022-01-01Z.tif"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/SPI/outputs/nc_to_tiffs/2022-01-01_.tiff"
# tiff_path_oeo = "/home/emile/openeo/drought-indices/SPI/out-2024-02-21_22_40_39.195282/openEO_2022-01-01Z.tif"

# tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/SPI/outputs/nc_to_tiffs/2020-01-01_.tiff"
# tiff_path_oeo = "/home/emile/openeo/drought-indices/SPI/out-2024-02-21_22_40_39.195282/openEO_2020-01-01Z.tif"

tiff_path_gmv = "/home/emile/Desktop/ToShareWithVito/CDI/Sample output/2021-09-01_CDI.tif"
tiff_path_oeo = "/home/emile/openeo/drought-indices/CDI/out-2024-04-03_17_42_51.706013/openEO_2021-09-01Z.tif"

print(f"{tiff_path_gmv=}")
print(f"{tiff_path_oeo=}")

# Avoid 'DataArray' object has no attribute 'set_close' by changing the code in the open_rasterio method
arrGMV = rioxarray.open_rasterio(tiff_path_gmv)
arrOEO = rioxarray.open_rasterio(tiff_path_oeo)

# scale_factor_GMV = arrGMV.attrs[band_struct["gmv_name"] + "_scale_factor"]
# add_offset_GMV = arrGMV.attrs[band_struct["gmv_name"] + "_add_offset"]
#
# scale_factor_OEO = arrOEO.attrs["scale_factor"]
# add_offset_OEO = arrOEO.attrs["add_offset"]

# add_offset_OEO = 278.5124215656377
# scale_factor_OEO = 0.00054749372453573
#
# arrGMV = (arrGMV * scale_factor_GMV) + add_offset_GMV
# arrOEO = (arrOEO * scale_factor_OEO) + add_offset_OEO

arrGMV = arrGMV.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)
arrOEO = arrOEO.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)

arrOEO = arrOEO.rio.reproject_match(arrGMV)

arrOEO = arrOEO.values.ravel()
arrGMV = arrGMV.values.ravel()

missing_value = -32767
mask = np.isnan(arrOEO) | (arrOEO == missing_value) | np.isnan(arrGMV) | (arrGMV == missing_value)
# remove all elements using mask:
arrOEO = arrOEO[~mask]
arrGMV = arrGMV[~mask]

# When taking more than 172480 elements, numpy mean will return inf
# arrOEO = arrOEO[:111000]
# arrGMV = arrGMV[:111000]

print(f"{MSE(arrOEO, arrGMV)=}")
print(f"{np.linalg.norm(arrOEO-arrGMV)=}")

def dice_coefficient(y_true, y_pred):
    intersection = np.sum(y_true * y_pred)
    return (2. * intersection) / (np.sum(y_true) + np.sum(y_pred))


dice = dice_coefficient(arrOEO == arrGMV, np.repeat(1, len(arrOEO)))
print('Dice similarity score is {}'.format(dice))  # CDI: 97%

# Check correlation: (a good match should be 0.9999999...)
# VCI has 0.8
corr = xr.corr(xr.DataArray(arrOEO), xr.DataArray(arrGMV)).values.take(0)
print(f"{corr=}")

fig, axs = plt.subplots()
axs.scatter(arrOEO, arrGMV)
axs.set_aspect('equal', 'box')
x = np.linspace(-10, 10, 100)
y = x
axs.plot(x, y, color='red')
plt.show()

exit()
#
# code_str = ""
# for band_struct in band_dictionary:
#     if (
#             isinstance(band_struct["era5land_name"], list)
#             or "era5land_name" not in band_struct
#             or "gmv_name" not in band_struct
#     ):
#         continue
#     print(band_struct["era5land_name"] + " - " + band_struct["gmv_name"] + ": ")
#     oeo_path = glob.glob(pathOEO + "*_" + band_struct["gmv_name"] + ".tif")[0]  # era5land_name
#     # gmv_path = glob.glob(pathGMV + "*_" + band_struct["gmv_name"] + ".tif")[0]
#     # arrOEO = osgeo.gdal.Open(str(oeo_path))
#     # arrGMV = osgeo.gdal.Open(str(gmv_path))
#     # arrOEO = arrOEO.ReadAsArray()
#     # arrGMV = arrGMV.ReadAsArray()
#
#     #  {ValueError}ValueError("conflicting sizes for dimension 'time': length 1 on the data but length 519 on coordinate 'time'")
#     arrOEO = rioxarray.open_rasterio(oeo_path)
#     # arrOEO = arrOEO.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)
#     # arrGMV = rioxarray.open_rasterio(gmv_path)
#
#     scale_factor = arrOEO.attrs[band_struct["gmv_name"] + "_scale_factor"]
#     add_offset = arrOEO.attrs[band_struct["gmv_name"] + "_add_offset"]
#
#     #     code_str += f"""elif band_name == "{band_struct["era5land_name"]}":
#     #     scale_factor = {scale_factor}
#     #     add_offset = {add_offset}
#     #     tmp = (tmp * scale_factor) + add_offset
#     # """
#
#     arrGMV = arrGMV.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)
#     arrOEO = arrOEO.rio.reproject_match(arrGMV)
#
#     arrOEO = arrOEO.values.ravel()
#     arrGMV = arrGMV.values.ravel()
#
#     missing_value = -32767
#     mask = np.isnan(arrOEO) | (arrOEO == missing_value) | np.isnan(arrGMV) | (arrGMV == missing_value)
#     # remove all elements using mask:
#     arrOEO = arrOEO[~mask]
#     arrGMV = arrGMV[~mask]
#
#     print(f"{MSE(arrOEO, arrGMV)=}")
#
#     # Check correlation: (a good match should be 0.9999999...)
#     corr = xr.corr(xr.DataArray(arrOEO), xr.DataArray(arrGMV)).values.take(0)
#     print(f"{corr=}")
#
# print(code_str)
# exit()

arrOEO = rioxarray.open_rasterio("/home/emile/openeo/drought-indices/SPEI/out-2024-02-14_10_22_46.460937/openEO.nc")
arrGMV = rioxarray.open_rasterio("/home/emile/Desktop/ToShareWithVito/SPEI/outputs/SPEI3.nc")

arrOEO = arrOEO.rio.reproject_match(arrGMV)
# arrOEO = arrOEO.shift(y=-1)  # To match better
arrOEO = arrOEO.rio.reproject_match(arrGMV)
# arrOEO.rio.to_raster("/home/emile/openeo/drought-indices/SPEI/out-2024-01-10_09_33_03.636274/openEO_resampled2.nc")

# merged = merge_arrays([arr1, arr2])
# range(100):  #
rms = []

# for time_idx in range(2, arrOEO.shape[0]):
#     # For each time idx, find the root squared error at
#     # each pixel between grnd_truth and monthly_data
#
#     imgOEO = arrOEO[time_idx, :, :]
#
#     imgGMV = arrGMV[time_idx, :, :]
#     err2 = (imgOEO - imgGMV) ** 2
#     err = err2 ** (1 / 2)
#     rms.append(err.mean(skipna=True).values.min())

means = arrOEO.mean(dim=["x", "y"])  # pip install nc-time-axis
# xarray.plot.line()
arrOEO.mean(dim=["x", "y"]).plot()
arrGMV.mean(dim=["x", "y"]).plot()
# xr.plot.FacetGrid.set_ticks(30)

plt.show()

print(f"{len(rms)=}")
print(f"{rms=}")
mseMin = xr.DataArray(rms).min(skipna=True)
mseMax = xr.DataArray(rms).max(skipna=True)

# mseMin = np.array(rms).min(skipna=True)
# mseMin = np.array(rms).min(skipna=True)
print(f"{mseMin=} {mseMax=}")
