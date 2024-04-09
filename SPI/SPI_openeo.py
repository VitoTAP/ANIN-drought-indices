import openeo
from openeo_utils.utils import *

connection = get_connection()
temporal_extent = ["1980-01-01", "2023-03-01"]
spatial_extent = spatial_extent_south_africa

load_collection = connection.load_stac(
    url="/data/users/Public/victor.verhaert/ANINStac/ERA5-TOTAL-PRECIPITATION/v0.1/collection.json",
    temporal_extent=temporal_extent,
    spatial_extent=spatial_extent,
    bands=["total_precipitation"],
)
ERA5_dc = load_collection

geojson = load_south_africa_geojson()
# geojson = load_johannesburg_geojson()
ERA5_dc = ERA5_dc.filter_spatial(geojson)

UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "SPI_UDF.py"))
SPI_dc = ERA5_dc.apply_dimension(dimension="t", code=UDF_code, runtime="Python")
SPI_dc = SPI_dc.rename_labels("bands", ["SPI"])

previous_month_UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "previous_month_UDF.py"))
SPI_previous_month_dc = SPI_dc.apply_dimension(dimension="t", code=previous_month_UDF_code, runtime="Python")
SPI_previous_month_dc = SPI_previous_month_dc.rename_labels("bands", ["SPI_previous_month"])
SPI_previous_month_dc = SPI_previous_month_dc.filter_temporal(temporal_extent)

SPI_dc = SPI_dc.filter_temporal(temporal_extent)


def main(temporal_extent_argument):
    global SPI_dc
    dc = SPI_previous_month_dc
    dc = dc.filter_temporal(temporal_extent_argument)
    resolution = 0.00297619047619  # 300m in degrees
    dc = dc.resample_spatial(resolution=resolution, projection=4326, method="bilinear")

    # out_format = "NetCDF"
    out_format = "GTiff"
    dc = dc.save_result(format=out_format)
    custom_execute_batch(dc, job_options=heavy_job_options, out_format=out_format)
    # custom_execute_batch(ERA5_dc, out_format="netCDF")


if __name__ == "__main__":
    main(get_temporal_extent_from_argv(["2020-01-01", "2023-03-01"]))
