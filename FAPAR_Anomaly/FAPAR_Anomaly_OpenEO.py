import os
import json
import openeo
import datetime
from openeo_utils.utils import *

connection = get_connection()

band = "FAPAR"
FAPAR_dc = connection.load_collection(
    # 'CGLS_FAPAR_V2_GLOBAL'  # 1km resolution, [1999,2020]
    "CGLS_FAPAR300_V1_GLOBAL",  # 300m resolution, [2014,present]
    temporal_extent=["2000-01-01", "2023-07-01"],  # This temporal extent ends up in the UDF, so keep small.
    # To avoid "No spatial filter could be derived to load this collection"
    # spatial_extent={  # South Africa
    #     "west": 10,
    #     "south": -40,
    #     "east": 40,
    #     "north": -20,
    # },
    bands=[band],
)

# FAPAR_dc = FAPAR_dc.resample_spatial(resolution=50.0, projection=4326)
# FAPAR_dc = FAPAR_dc.resample_spatial(projection=4326,
#                                      resolution=0.0089285714285 / 1000 * 400)  # based on 1km resolution
FAPAR_dc = FAPAR_dc.aggregate_temporal_period("month", reducer="mean")
# FAPAR_dc = (FAPAR_dc * 0.01) #

# Linearly interpolate missing values. To avoid protobuf error.
FAPAR_dc = FAPAR_dc.apply_dimension(
    dimension="t",
    process="array_interpolate_linear",
)
# FAPAR_anomaly_dc = FAPAR_dc
UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "FAPAR_Anomaly_UDF.py"))
# apply_dimension can use 13Gb+ memory.
FAPAR_anomaly_dc = FAPAR_dc.apply_dimension(dimension="t", code=UDF_code, runtime="Python")
FAPAR_anomaly_dc = FAPAR_anomaly_dc.rename_labels('bands', ['FAPAR_anomaly'])

if __name__ == "__main__":
    # Select smaller period for performance. (Mean still needs to be calculated on larger period)
    FAPAR_anomaly_dc = FAPAR_anomaly_dc.filter_temporal("2021-01-01", "2023-01-01")

    # pixel_size = 0.002976190476
    pixel_size = 0.1
    FAPAR_anomaly_dc = FAPAR_anomaly_dc.resample_spatial(resolution=pixel_size, projection=4326)

    FAPAR_anomaly_dc = FAPAR_anomaly_dc.filter_bbox({
        "west": 10,
        "south": -40,
        "east": 40,
        "north": -20,
    })

    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()
    FAPAR_anomaly_dc = FAPAR_anomaly_dc.filter_spatial(geojson)

    custom_execute_batch(FAPAR_anomaly_dc, job_options=heavy_job_options)
