import os

del os.environ["GDAL_DATA"]  # Clear so that GDAL_DATA from package is used

import rioxarray
import numpy as np

from shapely.geometry import mapping
import rasterio

import utils

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

tiff_path_reference = '/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v4/tiff_collection/1980/01/01/1980-01-01_2_metre_dewpoint_temperature.tiff'
arrREF = rioxarray.open_rasterio(tiff_path_reference)
crs_project = rasterio.crs.CRS.from_epsg(4326)
arrREF = arrREF.rio.write_crs(crs_project, inplace=True)

mask_layer = utils.load_south_africa_shape()

# just in case:
arrREF = arrREF.rio.clip(mask_layer.geometry.apply(mapping), crs=crs_project, all_touched=True,
                         from_disk=True).squeeze()

state = "margin_left"
for year in range(1970, 2070):
    for month_i in range(1, 13):
        month = "{:02d}".format(month_i)
        day = "01"  # Only one sample per month
        for band_struct in band_dictionary:
            if (
                    isinstance(band_struct["era5land_name"], list)
                    or "era5land_name" not in band_struct
                    or "gmv_name" not in band_struct
            ):
                continue
            gmv_name = band_struct["gmv_name"]
            era5land_name = band_struct["era5land_name"]
            tiff_path = f"/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-ANIN/tiff_collection/{year}/{month}/{day}/{year}-{month}-{day}_{gmv_name}.tif"
            # tiff_path = f"/dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v4/tiff_collection/{year}/{month}/{day}/{year}-{month}-{day}_{era5land_name}.tiff"
            if state == "margin_left":
                if os.path.exists(tiff_path):
                    state = "data_started"
            elif state == "data_started":
                if not os.path.exists(tiff_path):
                    state = "margin_right"
            elif state == "margin_right":
                if os.path.exists(tiff_path):
                    raise Exception("Hole in data found")

            if state == "data_started":
                print(tiff_path)
                arrOEO = rioxarray.open_rasterio(tiff_path)
                arrOEO = arrOEO.rio.write_crs(crs_project, inplace=True)
                missing_value = -32767
                mask = np.isnan(arrOEO) | (arrOEO == missing_value)
                s1 = mask.sum().values.take(0)
                s2 = (~mask).sum().values.take(0)
                # an image should not be purly nodata. Or purly opaque
                assert s1 != 0
                assert s2 != 0

                arrREF = arrREF.rio.reproject_match(arrOEO)  # Hopefully noop in many cases
                arrOEO = arrOEO.rio.clip(mask_layer.geometry.apply(mapping), crs=crs_project, all_touched=True,
                                         from_disk=True).squeeze()
                maskOEO = ~np.isnan(arrOEO) & (arrOEO != missing_value)
                maskREF = ~np.isnan(arrREF) & (arrREF != missing_value)
                diff = maskOEO != maskREF
                # maskREF.astype(np.uint16).rio.to_raster("tmp/maskedOEO.nc")
                # maskREF.astype(np.uint16).rio.to_raster("tmp/maskedREF.nc")
                print("diff: " + str(diff.sum().values.take(0)))
