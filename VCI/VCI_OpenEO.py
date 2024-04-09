import os
import json
import openeo
import datetime
from openeo_utils.utils import *

connection = get_connection()

band = "NDVI"
NDVI_dc = connection.load_collection(
    "CGLS_NDVI_V3_GLOBAL",  # 1km resolution resolution, [1999, 2020]
    temporal_extent=["1999-01-01", "2020-01-01"],  # This temporal extent ends up in the UDF, so keep small.
    # To avoid "No spatial filter could be derived to load this collection"
    spatial_extent={  # South Africa
        "west": 10,
        "south": -40,
        "east": 40,
        "north": -20,
    },
    bands=[band],
)

# TODO, merge this band to get recent results: NDVI_recent_dc = connection.load_collection(
#     "CGLS_NDVI300_V2_GLOBAL",  # 300m resolution resolution, [2020, present]
#     temporal_extent=["2010-01-01", "2023-01-01"],  # This temporal extent ends up in the UDF, so keep small.
#     # To avoid "No spatial filter could be derived to load this collection"
#     spatial_extent={  # South Africa
#         "west": 10,
#         "south": -40,
#         "east": 40,
#         "north": -20,
#     },
#     bands=[band],
# )
# NDVI_dc = NDVI_dc.resample_spatial(resolution=50.0, projection=4326)
# NDVI_dc = NDVI_dc.resample_spatial(projection=4326,
#                                      resolution=0.0089285714285 / 1000 * 400)  # based on 1km resolution
NDVI_dc = NDVI_dc.aggregate_temporal_period("month", reducer="mean")

# Linearly interpolate missing values. To avoid protobuf error.
NDVI_dc = NDVI_dc.apply_dimension(
    dimension="t",
    process="array_interpolate_linear",
)

UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "VCI_UDF.py"))
VCI_dc = NDVI_dc.apply_dimension(dimension="t", code=UDF_code, runtime="Python")
VCI_dc = VCI_dc.rename_labels('bands', ['VCI'])

if __name__ == "__main__":
    # Select smaller period for performance. (Min/Max still needs to be calculated on larger period)
    VCI_dc = VCI_dc.filter_temporal("2018-01-01", "2020-01-01")

    # pixel_size = 0.002976190476
    pixel_size = 0.1
    # VCI_dc = VCI_dc.resample_spatial(resolution=pixel_size, projection=4326)

    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()
    VCI_dc = VCI_dc.filter_spatial(geojson)

    custom_execute_batch(VCI_dc, job_options=heavy_job_options)
