from openeo.internal.graph_building import PGNode

from FAPAR_Anomaly.FAPAR_Anomaly_openeo import FAPAR_anomaly_dc
from SMA.SMA_openeo import SMA_dc
from SPI.SPI_openeo import SPI_dc, SPI_previous_month_dc
from openeo_utils.utils import *

# Long time range can take long time to calculate
temporal_extent = get_temporal_extent_from_argv(["2001-01-01", "2023-09-01"])

resolution = 0.00297619047619  # 300m in degrees
SPI_dc = SPI_dc.filter_temporal(temporal_extent)
SPI_dc = SPI_dc.resample_spatial(resolution=resolution, projection=4326, method="bilinear")

SPI_previous_month_dc = SPI_previous_month_dc.filter_temporal(temporal_extent)
SPI_previous_month_dc = SPI_previous_month_dc.resample_spatial(
    resolution=resolution, projection=4326, method="bilinear"
)

SMA_dc = SMA_dc.filter_temporal(temporal_extent)
SMA_dc = SMA_dc.resample_spatial(resolution=resolution, projection=4326, method="bilinear")

merged_dc = FAPAR_anomaly_dc.filter_temporal(temporal_extent)
merged_dc = merged_dc.merge_cubes(SPI_dc)
merged_dc = merged_dc.merge_cubes(SPI_previous_month_dc)
merged_dc = merged_dc.merge_cubes(SMA_dc)
merged_dc = merged_dc.filter_temporal(temporal_extent)


udf_code = load_udf(os.path.join(os.path.dirname(__file__), "CDI_UDF.py"))
CDI_dc = merged_dc.reduce_dimension(
    dimension="bands",
    reducer=PGNode(
        process_id="run_udf",
        data={"from_parameter": "data"},
        udf=udf_code,
        runtime="Python",
    ),
)
CDI_dc = CDI_dc.add_dimension("bands", "CDI", type="bands")

geojson = load_south_africa_geojson()
# geojson = load_johannesburg_geojson()
CDI_dc = CDI_dc.filter_spatial(geojson)


def main(temporal_extent_argument):
    global CDI_dc
    dc = CDI_dc.filter_temporal(temporal_extent_argument)
    dc = dc.filter_spatial(geojson)

    out_format = get_out_format_from_argv("GTiff")
    if out_format.lower() == "csv":
        dc = dc.aggregate_spatial(load_south_africa_secondary_catchment_geojson(), reducer="mean")
    custom_execute_batch(dc, job_options=heavy_job_options, out_format=out_format)
    # custom_execute_batch(merged_dc, out_format="netcdf",job_options=heavy_job_options)


if __name__ == "__main__":
    # job = openeo.rest.job.BatchJob(job_id=f"vito-j-240805eebc104391ad007d9601d9fd65", connection=connection)  # CDI
    # output_dir = "/home/emile/openeo/ANIN-drought-indices/CDI/out-2024-08-06_00_58_11.723235"
    # job.get_results().download_files(output_dir)
    # import sys
    # sys.argv = ["CDI/CDI_openeo.py", "2001-01-01", "2024-01-01", "--out_format=CSV"]
    if len(sys.argv) < 3:
        raise Exception("Please provide start and end date as arguments")
    main(temporal_extent)
