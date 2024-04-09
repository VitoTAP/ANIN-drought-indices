from openeo_utils.utils import *

connection = get_connection()

# geojson = load_johannesburg_geojson()
geojson = load_south_africa_geojson()


def calculate(f_name, month, reducer):
    print(f_name)
    temporal_extent = ["2000-02-27", "2021-12-28"]
    if month == "01":  # No January in first year
        temporal_extent[0] = "2001-" + month + "-01"
    else:
        temporal_extent[0] = "2000-" + month + "-01"
    GLASS_FAPAR = (
        connection.load_disk_collection(
            format="GTiff",
            glob_pattern=f"/data/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/tiff_collection_monthly/*/{month}/*/*-FAPAR-MODIS-Mean.tiff",
            options=dict(date_regex=r".*/(\d{4})/(\d{2})/(\d{2})/.*"),
        )
        .filter_temporal(temporal_extent)
        # .resample_spatial(
        #     resolution=0.002976190476,
        #     # resolution=0.5,
        #     projection=4326
        # )
    )

    # We only consider one month per year per time
    GLASS_FAPAR = GLASS_FAPAR.aggregate_temporal_period("year", reducer="mean")

    GLASS_FAPAR = GLASS_FAPAR * 1.0  # https://github.com/Open-EO/openeo-geotrellis-extensions/issues/225

    # FAPAR_Aggregated = GLASS_FAPAR
    FAPAR_Aggregated = GLASS_FAPAR.aggregate_temporal(intervals=temporal_extent,
                                                      labels=[temporal_extent[0]],
                                                      reducer=reducer, dimension="t")

    FAPAR_Aggregated = FAPAR_Aggregated.filter_spatial(geojson)

    job = FAPAR_Aggregated.create_job(
        title=f_name,
        out_format="GTiff",
        filename_prefix=f_name,
        job_options=heavy_job_options,
    )
    job.start_and_wait()
    job.get_results().download_files("tmp/", include_stac_metadata=False)


def main():
    # parallel_jobs = 1
    # with ThreadPool(parallel_jobs) as pool:
    for reducer in ["mean", "sd"]:  # "mean", "sd"
        # for month_i in range(1, 13):
        for month_i in reversed(range(1, 13)):
            month = "{:02d}".format(month_i)
            f_name = reducer + "_" + month

            if len(list(filter(lambda p: p.startswith(f_name), os.listdir("tmp")))) > 0:
                print("Already existing: " + f_name)
                continue

            # pool.apply_async(calculate, args=(f_name, month, reducer))
            calculate(f_name, month, reducer)
    # pool.close()
    # pool.join()


main()

path = "/data/MODIS/GLASS_FAPAR/tiff_collection_months_mean/"
files = list(Path(path).rglob("*.tif"))
for f in files:
    print(f)
    f_new = str(f).replace("2000-", "2020-")
    os.rename(f, f_new)

exit()
