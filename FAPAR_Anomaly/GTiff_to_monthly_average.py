import random
import time
import traceback
from pathlib import Path

import rioxarray as rxr
from dateutil import relativedelta

# Run HDF_to_GTiff.py first

input_path = Path(
    "/data/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/tiff_collection/"
)
if not input_path.exists():
    raise Exception("Path not found: " + str(input_path))

output_path = Path(
    "/data/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/tiff_collection_monthly/"
)
if not output_path.exists():
    raise Exception("Path not found: " + str(output_path))


def get_child_dirs(dir_path):
    return sorted([name for name in os.listdir(dir_path) if os.path.isdir(dir_path / name)])


if __name__ == "__main__":
    from openeo_utils.utils import *

    connection = get_connection()

    geojson = load_south_africa_geojson()

    years = get_child_dirs(input_path)
    for year_str in years:
        months_in_year = get_child_dirs(input_path / year_str)
        for month_in_year in months_in_year:

            date = datetime.date(int(year_str), int(month_in_year), 1)
            output_file = (
                    output_path
                    / "{:04d}".format(date.year)
                    / "{:02d}".format(date.month)
                    / "{:02d}".format(date.day)
                    / "{:04d}-{:02d}-{:02d}-FAPAR-MODIS-Mean.tiff".format(date.year, date.month, date.day)
            )
            if output_file.exists():
                print("output already exists: " + str(output_file))
                try:
                    tile = rxr.open_rasterio(output_file, masked=True)
                    assert tile.sizes["band"] == 1
                    print("And is parsable. shape: " + str(tile.shape) + " crs: " + str(tile.rio.crs))
                    tile.close()
                    tile = True
                except Exception as e:
                    print("Could not parse file: " + str(output_file))
                    print(e)
                continue
            print("Processing: " + str(input_path / year_str))
            output_file.parent.mkdir(parents=True, exist_ok=True)

            try:
                temporal_extent = [date, date + relativedelta.relativedelta(months=1)]
                print(str(temporal_extent))
                datacube = (
                    connection.load_disk_collection(
                        format="GTiff",
                        glob_pattern=f"/data/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/tiff_collection/{year_str}/{month_in_year}/*/*.hdf.tiff",
                        options=dict(date_regex=r".*tiff_collection/(\d{4})/(\d{2})/(\d{2})/.*"),
                    )
                    .filter_temporal(temporal_extent)
                    .resample_spatial(
                        resolution=0.002976190476,
                        # resolution=0.5,
                        projection=4326
                    )
                    .aggregate_temporal_period("year", reducer="mean")
                )
                geojson = load_south_africa_geojson()
                datacube = datacube.filter_spatial(geojson)
                tmp_file_path = str(output_file) + "_tmp" + str(random.randint(10000, 99999))
                datacube.download(tmp_file_path)
                os.rename(tmp_file_path, output_file)  # atomic, to avoid corrupt files

            except Exception as e:
                print("Problem with: " + str(input_path / year_str / month_in_year))
                print(e)
                print(traceback.format_exc())
            time.sleep(10)

    print("Done")
