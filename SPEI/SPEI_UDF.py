import numpy as np
import os
import sys
import xarray as xr
import logging
from openeo.udf import XarrayDataCube, inspect

wheel_path = "/dataCOPY/users/Public/emile.sonneveld/python/climate_indices-1.0.13-py2.py3-none-any.whl"
if not os.path.exists(wheel_path):
    wheel_path = wheel_path.replace("dataCOPY/", "data/")
    if not os.path.exists(wheel_path):
        raise Exception("Path not found: " + wheel_path)

sys.path.insert(0, wheel_path)
import climate_indices
from climate_indices import indices

############################## SETTING PARAMETERS  ##############################
_log = logging.getLogger("SPEI_UDF.py")

scale = 3
distribution = climate_indices.indices.Distribution.gamma
data_start_year = 1980
calibration_year_initial = 1980
calibration_year_final = 2023
periodicity = climate_indices.compute.Periodicity.monthly

if calibration_year_final - calibration_year_initial <= 2:
    print("Gamma correction on only 2 years will give bad looking results")


def spei_wrapped(precips_mm, pet_mm):
    tmp = indices.spei(
        precips_mm=precips_mm,
        pet_mm=pet_mm,
        scale=scale,
        distribution=distribution,
        data_start_year=data_start_year,
        calibration_year_initial=calibration_year_initial,
        calibration_year_final=calibration_year_final,
        periodicity=periodicity,
    )
    tmp = tmp.squeeze()
    tmp = tmp[np.newaxis].T
    return tmp


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xr.DataArray = cube.get_array()

    bands = [
        "2_metre_dewpoint_temperature",
        "surface_pressure",
        "surface_solar_radiation_downwards",
        "10_metre_u_wind_component",
        "10_metre_v_wind_component",
        "total_precipitation",
        "temperature-min",
        "temperature-mean",
        "temperature-max",
    ]

    def band_index(band_name):
        if band_name not in bands:
            raise Exception("Unknown band: " + band_name)
        if os.path.exists("/dataCOPY/"):
            return bands.index(band_name)  # when running locally
        else:
            return band_name  # when running in openeo

    def get_band(band_name):
        tmp = array.sel(bands=band_index(band_name))
        kelvin_to_celsius_offset = 273.15
        if band_name == "2_metre_dewpoint_temperature":
            tmp = tmp - kelvin_to_celsius_offset
        elif band_name == "surface_pressure":
            tmp = tmp * (pow(10, -3))  # The original units are Pa, we change them to KPa
        elif band_name == "surface_solar_radiation_downwards":
            tmp = tmp * pow(10, -6)  # The original units are J/m2, we change them to MJ/m2
        elif band_name == "total_precipitation":
            num_days_month = 30
            tmp = tmp * 1000 * num_days_month
        elif band_name == "10_metre_u_wind_component":
            pass
        elif band_name == "10_metre_v_wind_component":
            pass
        elif band_name == "temperature-min":
            tmp = tmp - kelvin_to_celsius_offset
        elif band_name == "temperature-mean":
            tmp = tmp - kelvin_to_celsius_offset
        elif band_name == "temperature-max":
            tmp = tmp - kelvin_to_celsius_offset
        else:
            raise Exception("Unknown band: " + band_name)

        return tmp

    try:
        array.sel(bands="2_metre_dewpoint_temperature")
        _log.warning('SUCCEEDED : array.sel(bands="2_metre_dewpoint_temperature") ')
    except Exception as e:
        _log.warning('FAILED : array.sel(bands="2_metre_dewpoint_temperature") ' + repr(e))

    try:
        array.sel({"bands": 1})
        _log.warning('SUCCEEDED : array.sel({"bands": 1}) ')
    except Exception as e:
        _log.warning('FAILED : array.sel({"bands": 1})' + repr(e))

    # noinspection SpellCheckingInspection
    def get_pet_mm():
        Tmean = get_band("temperature-mean")

        # Rn - net radiation at the crop surface MJ m-2 day-1
        Rn = get_band("surface_solar_radiation_downwards")

        # G -  soil heat flux density MJ m-2 day-1  Fixed value
        G = 0
        svpc = (4098 * (0.6108 * np.exp((17.27 * Tmean) / (Tmean + 237.3)))) / ((Tmean + 237.3) ** 2)

        P = get_band("surface_pressure")

        Cp = 0.001013  # specific heat at constant pressure MJ kg-1 °C-1
        epsi = 0.622  # ratio molecular weight of water vapour/dry air
        lamb = 2.45  # latent heat of vaporization MJ kg-1
        psi_cnt = (Cp * P) / (epsi * lamb)  # Psychometric constant

        u10 = get_band("10_metre_u_wind_component")
        v10 = get_band("10_metre_v_wind_component")
        u2 = ((u10**2) + (v10**2)) ** 0.5  # Getting wind component

        Tmin = get_band("temperature-min")
        Tmax = get_band("temperature-max")

        e0Tmax = 0.6108 * np.exp((17.27 * Tmax) / (Tmax + 237.3))
        e0Tmin = 0.6108 * np.exp((17.27 * Tmin) / (Tmin + 237.3))
        es = (e0Tmax - e0Tmin) / 2  # saturation vapour pressure kPa

        Tdew = get_band("2_metre_dewpoint_temperature")

        ea = 0.6108 * np.exp((17.27 * Tdew) / (Tdew + 237.3))  # actual vapour pressure kPa

        return (((0.408 * svpc) * (Rn - G)) + (psi_cnt * (900 / (Tmean + 273))) * u2 * (es - ea)) / (
            svpc + (psi_cnt * (1 + (0.34 * u2)))
        )

    precips_mm = get_band("total_precipitation").astype(np.dtype("float64"))
    pet_mm = get_pet_mm().astype(np.dtype("float64"))

    inspect(data=[precips_mm], message="inspect precips_mm")

    try:
        inspect(data=[precips_mm.variable], message="inspect precips_mm.variable")
        _log.warning('SUCCEEDED: inspect(data=[precips_mm.variable], message="inspect precips_mm.variable") ')
    except Exception as e:
        _log.warning('FAILED: inspect(data=[precips_mm.variable], message="inspect precips_mm.variable") ' + repr(e))

    precips_mm = precips_mm.squeeze(drop=True)
    pet_mm = pet_mm.squeeze(drop=True)
    if os.path.exists("/dataCOPY/"):
        # when running locally
        precips_mm = precips_mm.drop_vars("variable")
        pet_mm = pet_mm.drop_vars("variable")
    else:
        # when running in openeo
        try:
            precips_mm = precips_mm.drop("bands")
            pet_mm = pet_mm.drop("bands")
            _log.warning('SUCCEEDED: precips_mm.drop("bands") ')
        except Exception as e:
            _log.warning('FAILED: precips_mm.drop("bands") ' + repr(e))

    precips_mm_grouped = precips_mm.stack(point=("y", "x")).groupby("point", squeeze=True)
    pet_mm_grouped = pet_mm.stack(point=("y", "x")).groupby("point", squeeze=True)

    # ValueError: apply_ufunc can only perform operations over multiple GroupBy objects at once if they are all grouped the same way
    spi_results = xr.apply_ufunc(
        spei_wrapped,
        precips_mm_grouped,
        pet_mm_grouped,
    )

    BAND_NAME = "SPEI"
    spi_results = spi_results.expand_dims(dim="bands", axis=0).assign_coords(bands=[BAND_NAME])

    spi_results = spi_results.unstack("point")
    spi_results = spi_results.rename({"y": "lat", "x": "lon"})  # Necessary step
    spi_results = spi_results.reindex(lat=list(reversed(spi_results["lat"])))
    spi_results = spi_results.rename({"lat": "y", "lon": "x"})

    # No need to specify crs here
    return XarrayDataCube(spi_results)


if __name__ == "__main__":
    print("Running test code!")
    import datetime
    import rioxarray as rxr

    now = datetime.datetime.now()

    dataset = rxr.open_rasterio("/home/emile/openeo/drought-indices/SPEI/era5_raw_bands_GMV.nc")
    if dataset.to_array().dims == ("variable", "band", "y", "x"):
        array = dataset.to_array().swap_dims({"variable": "bands", "band": "t"})
    else:
        array = dataset.to_array().swap_dims({"variable": "bands"})

    array = array.astype(np.float32)

    ret = apply_datacube(XarrayDataCube(array), dict())
    arr = ret.array
    data_crs = dataset.rio.crs
    arr.rio.write_crs(data_crs, inplace=True)
    arr = arr.squeeze()  # remove unneeded dimensions
    # First timeframes are NaN, which is confusing, as it shows nothing in Q-GIS. Drop them:
    arr = arr.dropna(dim="t", how="all")  # Might be nice to only trim beginning and end, in all dimensions
    if len(arr.dims) > 3:
        print("Taking only first time sample to avoid too many dimensions")
        arr = arr.isel(t=0)
    arr.rio.to_raster("tmp/out-" + str(now).replace(":", "_").replace(" ", "_") + ".nc")
    print(ret)
