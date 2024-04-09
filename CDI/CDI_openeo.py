from openeo.internal.graph_building import PGNode

from FAPAR_Anomaly.FAPAR_Anomaly_OpenEO import FAPAR_anomaly_dc
from SMA.SMA_openeo import SMA_dc
from SPI.SPI_openeo import SPI_dc, SPI_previous_month_dc
from openeo_utils.utils import *

resolution = 0.00297619047619  # Original resolution
resolution = resolution * 10
merged_dc = SPI_dc.resample_spatial(resolution=resolution, projection=4326)
merged_dc = merged_dc.merge_cubes(SPI_previous_month_dc)
merged_dc = merged_dc.merge_cubes(SMA_dc)
merged_dc = merged_dc.merge_cubes(FAPAR_anomaly_dc)

# Linearly interpolate missing values. To avoid protobuf error.
# CDI_dc = CDI_dc.apply_dimension(
#     dimension="t",
#     process="array_interpolate_linear",
# )

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

if __name__ == "__main__":
    year = 2021
    start = f"{year}/01/01"
    end = f"{year + 2}/01/01"
    CDI_dc = CDI_dc.filter_temporal([start, end])

    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()
    CDI_dc = CDI_dc.filter_spatial(geojson)

    custom_execute_batch(CDI_dc, job_options=heavy_job_options)
