import openeo

# url = "https://openeo.cloud"
url = "https://openeo-dev.vito.be"
connection = openeo.connect(url).authenticate_oidc()

datacube = (
    connection.load_disk_collection(
        format="GTiff",
        # example: vito-j-2311135bd1fd4558bc11ee2efbb93402
        glob_pattern=f"/data/MTDA/MODIS/GLASS_FAPAR/tiff_collection_months_mean/*.tif",
        options=dict(date_regex=r".*(\d{4})-(\d{2})-(\d{2}).*"),
    )
    .filter_temporal(["2000-02-01", "2001-01-01"])
    .filter_bbox(
        # South Africa:
        west=10,
        south=-40,
        east=40,
        north=-20,
    )
)

job = datacube.create_job(title="FAPAR_MTDA")
job.start_and_wait()
job.get_results().download_files()
