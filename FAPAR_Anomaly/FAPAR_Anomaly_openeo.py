from openeo_utils.utils import *

connection = get_connection()

spatial_extent = spatial_extent_south_africa

temporal_extent = get_temporal_extent_from_argv(["2020-01-01", "2023-09-01"])

band = "FAPAR"
# scale_factor = 0.004 Not needed
lc = connection.load_collection(
    "CGLS_FAPAR300_V1_GLOBAL",  # 300m resolution, [2014,present]
    temporal_extent=temporal_extent,
    spatial_extent=spatial_extent,
    bands=[band],
)
CGLS_FAPAR300_V1_GLOBAL_dc = (
    lc
    .aggregate_temporal_period("day", reducer="mean")  # Make weighted mean
    .apply_dimension(dimension="t", process="array_interpolate_linear")
    .aggregate_temporal_period("month", reducer="mean")
    .filter_temporal(temporal_extent)
    # Linearly interpolate missing values. To avoid protobuf error.
    .apply_dimension(
        dimension="t",
        process="array_interpolate_linear",
    )
    * 1.0
    # * scale_factor
)


def load_disk_collection_glass(subfolder):
    return (
        connection.load_stac(
            f"/data/MTDA/MODIS/GLASS_FAPAR/{subfolder}/STAC_catalogs/v0.2/collection.json",
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent,
        )
        .aggregate_temporal_period("month", reducer="mean")
        .rename_labels("bands", [subfolder])
    )


phenology_mask = connection.load_stac(
    "/data/users/Public/emile.sonneveld/ANIN/CROP_MASK/CROP_MASK_STAC/collection.json",
    spatial_extent=spatial_extent,
    temporal_extent=temporal_extent,
)

FAPAR_dc = CGLS_FAPAR300_V1_GLOBAL_dc
FAPAR_Mean = load_disk_collection_glass("tiff_collection_months_mean")
FAPAR_Sd = load_disk_collection_glass("tiff_collection_months_sd")

FAPAR_anomaly_dc = (FAPAR_dc - FAPAR_Mean) / FAPAR_Sd
FAPAR_anomaly_dc = FAPAR_anomaly_dc.rename_labels("bands", ["FAPAR_anomaly"])
FAPAR_anomaly_dc = FAPAR_anomaly_dc.mask(phenology_mask)


def main(temporal_extent_argument):
    global FAPAR_anomaly_dc

    FAPAR_anomaly_dc = FAPAR_anomaly_dc.filter_temporal(temporal_extent_argument)

    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()  # For faster debugging
    FAPAR_anomaly_dc = FAPAR_anomaly_dc.filter_spatial(geojson)

    # out_format = "NetCDF"
    out_format = "GTiff"
    dc = FAPAR_anomaly_dc
    dc = dc.save_result(format=out_format)
    custom_execute_batch(dc, out_format=out_format, run_type="batch_job")
    # custom_execute_batch(CGLS_FAPAR300_V1_GLOBAL_dc, out_format=out_format, run_type="batch_job")


if __name__ == "__main__":
    main(temporal_extent)
