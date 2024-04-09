import logging

import xarray as xr
from openeo.udf import XarrayDataCube

_log = logging.getLogger("CDI_UDF.py")


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()

    FAPAR_Anomaly_band = array[0]
    SPI_band = array[1]
    SPI_previous_month_band = array[2]
    SMA_band = array[3]

    # Watch class: when SPI-3 is less than -1 and make no data as 0
    CDI = xr.where(SPI_band < -1, 1, 0)
    # Warning class: where SPI-3 < -1 and SMA < -1
    CDI = xr.where((SMA_band < -1) & (CDI == 1), 2, CDI)
    # Alert class: where SPI-3 < -1 and FAPAR anomaly < -1
    CDI = xr.where((FAPAR_Anomaly_band < -1) & (CDI == 1), 3, CDI)
    # Partial recovery:  where FAPAR anomaly < -1 and SPI-3 m-1 < -1 and SPI-3 > -1
    CDI = xr.where((FAPAR_Anomaly_band < -1) & (SPI_band > -1) & (SPI_previous_month_band < -1), 4, CDI)
    # Full recovery:  where FAPAR anomaly > -1 and SPI-3 m-1 < -1 and SPI-3 > -1
    CDI = xr.where((FAPAR_Anomaly_band > -1) & (SPI_band > -1) & (SPI_previous_month_band < -1), 5, CDI)
    # make no data as nan
    CDI = CDI.where(CDI != 0)

    # No need to specify crs here
    return XarrayDataCube(CDI)
