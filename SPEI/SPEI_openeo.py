import json
import os
import datetime
import openeo
import geopandas as gpd
import sys
from openeo_utils.utils import *

connection = get_connection()

SPI_dc = connection.load_collection(
    "AGERA5",
    temporal_extent=["2015-01-01", "2023-07-01"],
    spatial_extent={  # South Africa
        "west": 10,
        "south": -40,
        "east": 40,
        "north": -20,
    },
    bands=["precipitation-flux"],
)
#Getting the components
es = get_es(Tmax, Tmin)
ea = get_ea(Tdew)
svpc = get_svpc(Tmean)
psi_cnt = get_psi_cnt(P)

#Calculation pet
pet_mm = get_pet_mm(svpc, Rn, G, psi_cnt, Tmean, u2, es, ea)

#Giving the appropriate shape to da data. Grouping
pet_mm_grouped = pet_mm.stack(point=('y', 'x')).groupby('point')


SPI_dc = SPI_dc.aggregate_temporal_period("month", reducer="sum")

# Linearly interpolate missing values. To avoid protobuf error.
SPI_dc = SPI_dc.apply_dimension(
    dimension="t",
    process="array_interpolate_linear",
)
SPI_dc = SPI_dc.rename_labels('bands', ['SPI'])

# SPI_dc = (SPI_dc * 0.01) # Scaling has no impact on Z-score

UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "SPI_UDF.py"))
SPI_dc = SPI_dc.apply_dimension(dimension="t", code=UDF_code, runtime="Python")

previous_month_UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "previous_month_UDF.py"))
SPI_previous_month_dc = SPI_dc.apply_dimension(dimension="t", code=previous_month_UDF_code, runtime="Python")
SPI_previous_month_dc = SPI_previous_month_dc.rename_labels('bands', ['SPI_previous_month'])

if __name__ == "__main__":
    # geojson = load_south_africa_geojson()
    geojson = load_johannesburg_geojson()
    SPI_dc = SPI_dc.filter_spatial(geojson)

    SPI_dc = SPI_dc.filter_temporal("2021-01-01", "2023-07-01")

    custom_execute_batch(SPI_dc)
