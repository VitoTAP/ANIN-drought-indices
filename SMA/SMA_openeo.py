import openeo
from openeo_utils.utils import *

connection = get_connection()

temporal_extent = get_temporal_extent_from_argv(["2023-09-01", "2023-10-01"])

SMA_dc = connection.load_stac(
    url="/data/users/Public/emile.sonneveld/ANIN/SMA_openeo_cropped_v03/stac/v0.2/collection.json",
    temporal_extent=temporal_extent,
    spatial_extent=spatial_extent_south_africa,
    bands=["SMA_openeo_cropped"],
)

SMA_dc = SMA_dc.aggregate_temporal_period("month", reducer="mean")
SMA_dc = SMA_dc.rename_labels("bands", ["SMA"])

if __name__ == "__main__":
    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()
    SMA_dc = SMA_dc.filter_spatial(geojson)
    custom_execute_batch(SMA_dc)  # , run_type="sync"
