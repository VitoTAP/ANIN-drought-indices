import openeo
from openeo_utils.utils import *

connection = get_connection()

glob_pattern = "/data/users/Public/emile.sonneveld/ANIN/SMA_layer/sma*_m_wld_*_t/sma*_m_wld_*_t.tif"
assert_glob_ok(glob_pattern)
SMA_dc = connection.load_disk_collection(
    format="GTiff",
    # Data was manually imported from https://edo.jrc.ec.europa.eu/gdo/php/index.php?id=2112
    # By making a free account on Terrascope, you can edit this folder too: https://terrascope.be/en/form/vm
    glob_pattern=glob_pattern,
    options=dict(date_regex=r".*_(\d{4})(\d{2})(\d{2})_t.tif"),
)

SMA_dc = SMA_dc.aggregate_temporal_period("day", reducer="mean")  # Make weighted mean
SMA_dc = SMA_dc.apply_dimension(dimension="t", process="array_interpolate_linear")
SMA_dc = SMA_dc.aggregate_temporal_period("month", reducer="mean")
SMA_dc = SMA_dc.rename_labels("bands", ["SMA"])

if __name__ == "__main__":
    start = f"2020-01-01"
    end = f"2024-02-01"
    SMA_dc = SMA_dc.filter_temporal([start, end])
    resolution = 0.00297619047619  # 300m in degrees
    SMA_dc = SMA_dc.resample_spatial(resolution=resolution, projection=4326, method="bilinear")

    geojson = load_south_africa_geojson()
    # geojson = load_johannesburg_geojson()
    SMA_dc = SMA_dc.filter_spatial(geojson)
    custom_execute_batch(SMA_dc)
