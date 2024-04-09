import datetime
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import openeo

connection = None

now = datetime.datetime.now()


def get_connection():
    global connection
    if connection is None:
        # Possible backends: "openeo.cloud" "openeo.vito.be"
        url = "https://openeo-dev.vito.be"
        # url = "https://openeo.cloud"  # TODO: Get more credits
        connection = openeo.connect(url).authenticate_oidc()
        print(connection.root_url + " time: " + str(now))
    return connection


def load_shape_file(filepath):
    """Loads the shape file desired to mask a grid.
    Args:
        filepath: Path to *.shp file
    """
    shpfile = gpd.read_file(filepath)
    print("""Shapefile loaded. To prepare for masking, run the function
        `select_shape`.""")
    return shpfile


# Create the mask
def select_shape(shpfile):
    """Select the submask of interest from the shapefile.
    Args:
        shpfile: (*.shp) loaded through `load_shape_file`
        category: (str) header of shape file from which to filter shape.
            (Run print(shpfile) to see options)
        name: (str) name of shape relative to category.
           Returns:
        shapely polygon
    """

    col_code = 'ISO3_CODE'
    country_codes = ['ZAF', 'LSO', 'SWZ']

    # Extract the rows that have 'ZAF', 'LSO', or 'SWZ' in the 'SOV_A3' column
    selected_rows = shpfile[shpfile[col_code].isin(country_codes)]

    # Combine the selected polygons into a single polygon
    unioned_polygon = selected_rows.geometry.unary_union

    # Convert the unioned polygon to a geopandas dataframe with a single row
    mask_polygon = gpd.GeoDataFrame(geometry=[unioned_polygon])
    mask_polygon = mask_polygon.explode()  # OpenEO also prefers list of Polygons compared to a giant multipolygon
    mask_polygon = mask_polygon.loc[mask_polygon.area > 0.0001]  # Remove tini islands

    print("""Mask created.""")

    return mask_polygon


def load_south_africa_geojson():
    # Load de shp
    # Once we decide the layer for each index it has to be fixed
    shpfile = load_shape_file('../SPI/shape/CNTR_RG_01M_2020_4326.shp')

    # Create the mask layer
    mask_layer = select_shape(shpfile)

    geojson = json.loads(mask_layer.to_json())

    with open("south_africa_mask.json", "w") as f:
        json.dump(geojson, f, indent=2)

    return geojson


def load_johannesburg_geojson():
    containing_folder = Path(__file__).parent
    with open(containing_folder / "johannesburg.json") as f:
        return json.load(f)


def load_udf(udf):
    """
    UDF: User Defined Function
    """
    with open(udf, 'r+', encoding="utf8") as fs:
        return fs.read()


false = False
true = True
heavy_job_options = {
    'driver-memory': '10G',
    'driver-memoryOverhead': '5G',
    'driver-cores': '1',
    'executor-memory': '10G',
    'executor-memoryOverhead': '5G',
    'executor-cores': '1',
    'executor-request-cores': '600m',
    'max-executors': '22',
    'executor-threads-jvm': '7',
    "udf-dependency-archives": [
        "https://artifactory.vgt.vito.be/auxdata-public/hrlvlcc/croptype_models/20230615T144208-24ts-hrlvlcc-v200.zip#tmp/model",
        "https://artifactory.vgt.vito.be:443/auxdata-public/hrlvlcc/openeo-dependencies/cropclass-1.0.5-20230810T154836.zip#tmp/cropclasslib",
        "https://artifactory.vgt.vito.be/auxdata-public/hrlvlcc/openeo-dependencies/vitocropclassification-1.4.0-20230619T091529.zip#tmp/vitocropclassification",
        "https://artifactory.vgt.vito.be/auxdata-public/hrlvlcc/openeo-dependencies/hrl.zip#tmp/venv_static",

        # 'https://artifactory.vgt.vito.be/auxdata-public/hrlvlcc/hrl-temp.zip#tmp/venv',
        # 'https://artifactory.vgt.vito.be/auxdata-public/hrlvlcc/hrl.zip#tmp/venv_static',
    ],
    "logging-threshold": "debug",
    "mount_tmp": false,  # or true
    "goofys": "false",
    "node_caching": true,
    # "sentinel-hub": {
    #     "client-alias": 'vito'
    # },
}


def custom_execute_batch(datacube, job_options=None):
    try:
        # os.system('find . -type d -empty -delete')  # better run manually
        import inspect
        parent_filename = inspect.stack()[1].filename  # HACK!

        with open(parent_filename, 'r') as file:
            job_description = "now: " + str(now) + " url: " + connection.root_url + "\n\n"
            job_description += "python code: \n\n\n```python\n" + file.read() + "```\n\n"

        try:
            from git import Repo
            repo = Repo(os.path.dirname(parent_filename), search_parent_directories=True)
            job_description += "GIT URL: " + list(repo.remotes[0].urls)[0] + "\n\n"
            job_description += "GIT branch: '" + repo.active_branch.name + "' commit: '" + repo.active_branch.commit.hexsha + "'\n\n"
            job_description += "GIT changed files: " + ", ".join(map(lambda x: x.a_path, repo.index.diff(None))) + "\n"
        except Exception as e:
            print("Could not attach GIT info: " + str(e))

        output_dir = Path("out-" + str(now).replace(":", "_").replace(" ", "_"))
        output_dir.mkdir(parents=True, exist_ok=True)
        datacube.print_json(file=output_dir / "process_graph.json", indent=2)
        print(str(output_dir.absolute()) + "/")
        # datacube.download("SPI_monthly.nc")
        job = datacube.create_job(
            title=os.path.basename(parent_filename),
            format="GTiff",
            # format="NetCDF",
            description=job_description,
            filename_prefix=os.path.basename(parent_filename).replace(".py", ""),
            job_options=job_options,
        )
        with open(output_dir / "job_id.txt", mode="w") as f:
            f.write(job.job_id)
        job.start_and_wait()
        job.get_results().download_files(output_dir)

        with open(output_dir / "logs.json", "w") as f:
            json.dump(job.logs(), f, indent=2)

        os.system('spd-say "Program terminated"')  # vocal feedback
    except KeyboardInterrupt:
        # No audio when user manually stops program
        pass
    except:
        os.system('spd-say "Program failed"')  # vocal feedback
        raise
    finally:
        print("custom_execute_batch end time: " + str(datetime.datetime.now()))


if __name__ == "__main__":
    get_connection()
    custom_execute_batch(None)
