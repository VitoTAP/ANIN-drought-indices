from openeo.internal.graph_building import PGNode

from FAPAR_Anomaly.FAPAR_Anomaly_openeo import FAPAR_anomaly_dc
from SMA.SMA_openeo import SMA_dc
from SPI.SPI_openeo import SPI_dc, SPI_previous_month_dc
from openeo_utils.utils import *

temporal_extent = get_temporal_extent_from_argv(["2023-08-01", "2023-09-01"])

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

geojson = load_south_africa_geojson()
# geojson = load_johannesburg_geojson()
CDI_dc = CDI_dc.filter_spatial(geojson)


def main(temporal_extent_argument):
    global CDI_dc
    CDI_dc = CDI_dc.filter_temporal(temporal_extent_argument)

    # out_format = "NetCDF"
    out_format = "GTiff"
    CDI_dc = CDI_dc.save_result(format=out_format)
    custom_execute_batch(CDI_dc, job_options=heavy_job_options, out_format=out_format)  # , run_type="sync"
    # custom_execute_batch(merged_dc, out_format="netcdf",job_options=heavy_job_options)


if __name__ == "__main__":
    # job = openeo.rest.job.BatchJob(job_id=f"vito-j-2404082abce44e9b922cb3736b0fcce4", connection=connection)  # CDI
    # output_dir = "/home/emile/openeo/drought-indices/CDI/out-test2/"
    # job.get_results().download_files(output_dir)
    if len(sys.argv) < 3:
        raise Exception("Please provide start and end date as arguments")
    main(temporal_extent)
