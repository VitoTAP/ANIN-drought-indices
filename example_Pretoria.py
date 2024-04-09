import openeo

url = "https://openeo.cloud"
connection = openeo.connect(url).authenticate_oidc()

spatial_extent_sansa = {
    "east": 28.275,
    "north": -25.740,
    "south": -25.755,
    "west": 28.260,
}

datacube = connection.load_collection(
    collection_id="SENTINEL2_L2A",
    bands=["B04", "B03", "B02"],
    spatial_extent=spatial_extent_sansa,
    temporal_extent=["2024-03-05", "2024-03-07"],
)

datacube.download("out-Pretoria.tiff")
