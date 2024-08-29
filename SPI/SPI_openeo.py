import openeo
from openeo_utils.utils import *

connection = get_connection()
spatial_extent = spatial_extent_south_africa

use_experimental_era5 = False
if use_experimental_era5:
    temporal_extent = ["1980-01-01", "2024-01-01"]
    glob_pattern = (
        "/data/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v3/tiff_collection/*/*/*/*_total_precipitation.tiff"
    )
    date_regex = r".*tiff_collection/(\d{4})/(\d{2})/(\d{2})/.*"
    assert_glob_ok(glob_pattern, date_regex)

    load_collection = connection.load_disk_collection(
        format="GTiff",
        # Based on https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land-monthly-means
        glob_pattern=glob_pattern,
        options=dict(date_regex=date_regex),
    )
    load_collection._pg.arguments["featureflags"] = {"tilesize": 16}
    ERA5_dc = load_collection.rename_labels(
        "bands", ["total_precipitation"]
    ) * 1.0
    ERA5_dc = (ERA5_dc + 3.277e+4) / 4.585e+6  # found with linear regression
    ERA5_dc = ERA5_dc.filter_temporal(temporal_extent)
else:
    temporal_extent = ["1980-01-01", "2023-03-01"]
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
SPI_dc = ERA5_dc.apply_dimension(dimension="t", process=openeo.UDF(code=UDF_code, runtime="Python"))
SPI_dc = SPI_dc.rename_labels("bands", ["SPI"])

previous_month_UDF_code = load_udf(os.path.join(os.path.dirname(__file__), "previous_month_UDF.py"))
previous_month_UDF = openeo.UDF(code=previous_month_UDF_code, runtime="Python")
SPI_previous_month_dc = SPI_dc.apply_dimension(dimension="t", process=previous_month_UDF)
SPI_previous_month_dc = SPI_previous_month_dc.rename_labels("bands", ["SPI_previous_month"])
SPI_previous_month_dc = SPI_previous_month_dc.filter_temporal(temporal_extent)

SPI_dc = SPI_dc.filter_temporal(temporal_extent)


def main(temporal_extent_argument):
    dc = SPI_dc
    dc = dc.filter_temporal(temporal_extent_argument)
    # resolution = 0.00297619047619  # 300m in degrees
    # dc = dc.resample_spatial(resolution=resolution, projection=4326, method="bilinear")

    out_format = get_out_format_from_argv("GTiff")
    if out_format.lower() == "csv":
        dc = dc.aggregate_spatial(load_south_africa_secondary_catchment_geojson(), reducer="mean")
    custom_execute_batch(dc, job_options=heavy_job_options, out_format=out_format)
    # custom_execute_batch(ERA5_dc, out_format="netCDF")


if __name__ == "__main__":
    main(get_temporal_extent_from_argv(["1980-01-01", "2024-01-01"]))
