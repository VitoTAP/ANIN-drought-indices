import os

del os.environ["GDAL_DATA"]  # Clear so that GDAL_DATA from package is used

import numpy as np
import rioxarray
import rasterio

for month_i in range(1, 13):
    month = "{:02d}".format(month_i)
    print(month)
    input_path = f"/home/emile/Desktop/ToShareWithVito/VCI/VCI_with_Crop_Mask/ANIN_VCI_300m_SouthAfrica_2021{month}.tif"
    output_path = f"/dataCOPY/users/Public/emile.sonneveld/ANIN/VCI/MASK/2020-{month}-01.tif"

    arrGMV = rioxarray.open_rasterio(input_path)
    arrGMV = arrGMV.rio.write_crs(rasterio.crs.CRS.from_epsg(4326), inplace=True)

    missing_value = -32767
    mask = np.isnan(arrGMV) | (arrGMV == missing_value)
    # mask = ~mask
    # mask.rio.write_nodata(0, inplace=True)
    mask.rio.to_raster(raster_path=output_path, dtype="int16", compress="DEFLATE")
