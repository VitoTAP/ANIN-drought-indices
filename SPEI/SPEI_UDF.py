import logging
import numpy as np
import os
import rasterio
import sys
import xarray as xr
from openeo.udf import XarrayDataCube

p = '/data/users/Public/emile.sonneveld/python/climate_indices-1.0.13-py2.py3-none-any.whl'
if not os.path.exists(p):
    raise Exception("Path not found: " + p)
sys.path.append(p)
import climate_indices
from climate_indices import indices

############################################################################ SETTING PARAMETERS  for the SPI #################################################################

scale = 3
distribution = climate_indices.indices.Distribution.gamma  # Fixed
data_start_year = 2015
calibration_year_initial = 2015
calibration_year_final = 2024
periodicity = climate_indices.compute.Periodicity.monthly  # Fixed

if calibration_year_final - calibration_year_initial <= 2:
    print("Gamma correction in SPI on only 2 years will give bad looking results")

_log = logging.getLogger("python_UDF")


# _log.warning("spi_wrapped(...) os.environ.get('GDAL_DATA'): " + str(os.environ.get('GDAL_DATA')))  # /opt/venv/lib64/python3.8/site-packages/rasterio/gdal_data

# crs_project = rasterio.crs.CRS.from_epsg(4326)


def spi_wrapped(values: np.ndarray):
    # _log.warning("spi_wrapped(...) values.shape: " + str(values.shape))
    ret = indices.spi(
        values=values,
        scale=scale,
        distribution=distribution,
        data_start_year=data_start_year,
        calibration_year_initial=calibration_year_initial,
        calibration_year_final=calibration_year_final,
        periodicity=periodicity,
    )
    ret = ret[np.newaxis].T
    # _log.warning("spi_wrapped(...) values.shape: " + str(values.shape) + " ret.shape: " + str(ret.shape))
    # print("spi_wrapped(...) values.shape: " + str(values.shape) + " ret.shape: " + str(ret.shape))
    return ret


# Processing the data (masking and reshaping)
def proccessingNETCDF(data):
    """Process the data to serve as input to de SPI function
    Args:
        data: netcdf file

        Returns
        DataArrayGroupBy grouped over point (y and x coordinates)
    """
    num_days_month = data.t.dt.days_in_month

    data_precip = (data * 2.908522800670776e-07) + 0.009530702520736942  # Rescaling the values
    data_precip = data_precip * 1000 * num_days_month  # The original units are meters, we change them to millimeters, and multiply by the days of the month

    # Reverse the Y dimension values to increasing values (This is an issue of ERA5 datasets and other climatic datasets)
    # data_precip = data_precip.rename({'y': 'lat', 'x': 'lon'})  # Necessary step
    # data_precip = data_precip.reindex(lat=list(reversed(data_precip['lat'])))
    # data_precip = data_precip.rename({'lat': 'y', 'lon': 'x'})

    # # Mask the country
    # mapped = mask_layer.geometry.apply(mapping)
    # # mapped.rio.write_crs(crs_project, inplace=True)
    # # mapped.to_netcdf(f'{path_out}mapped.nc')
    #
    # data_precip_masked = data_precip.rio.clip(mapped, crs=crs_project, all_touched=True,
    #                                           from_disk=True).squeeze()
    data_precip_masked = data_precip
    data_precip_masked = data_precip_masked.squeeze()

    # Giving the appropriate shape to da data
    data_grouped = data_precip_masked.stack(point=('y', 'x')).groupby('point')
    print("""Data is prepared to serve as input for the SPI index.""")

    return data_grouped


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()

    data_grouped = proccessingNETCDF(array)
    spi_results = xr.apply_ufunc(spi_wrapped,
                                 data_grouped,
                                 # input_core_dims=[["t"]],
                                 # output_core_dims=[["t"]],
                                 )

    # spi_results = spi_results.expand_dims(dim={"bands": 1})
    BAND_NAME = 'SPI'
    spi_results = spi_results.expand_dims(dim='bands', axis=0).assign_coords(bands=[BAND_NAME])
    spi_results = spi_results.unstack('point')
    spi_results = spi_results.rename({'y': 'lat', 'x': 'lon'})  # Necessary step
    spi_results = spi_results.reindex(lat=list(reversed(spi_results['lat'])))
    spi_results = spi_results.rename({'lat': 'y', 'lon': 'x'})
    # No need to specify crs here
    return XarrayDataCube(spi_results)


if __name__ == "__main__":
    # Test code:
    import pandas as pd

    d = [1.0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
         13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
         25, 26, 27, 28, 29, 30]
    array = xr.DataArray(
        data=d,
        coords=pd.to_datetime(d),
        dims=["t"],
    )

    # array = array)
    array = array.expand_dims(dim='bands').assign_coords(bands=["band_name"])
    array = array.expand_dims(dim='x')
    array = array.expand_dims(dim='y')

    ret = apply_datacube(XarrayDataCube(array), dict())
    print(ret)
