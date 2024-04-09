import datetime
import glob
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import openeo
import requests

# Emile: Sometimes python hangs on IPv6 requests.
requests.packages.urllib3.util.connection.HAS_IPV6 = False

now = datetime.datetime.now()

containing_folder = Path(__file__).parent

connection = None
def get_connection():
    global connection
    if connection is not None:
        return connection
    # Possible backends:
    # url = "https://openeo-dev.vito.be"
    # url = "https://openeo.vito.be"
    url = "https://openeo.cloud"
    connection = openeo.connect(url).authenticate_oidc()
    print(connection.root_url + " time: " + str(now))
    return connection


def assert_glob_ok(glob_pattern: str):
    if glob_pattern.startswith("/data/") or glob_pattern.startswith("/dataCOPY/"):
        if os.path.exists("/dataCOPY/"):
            glob_pattern = glob_pattern.replace("/data/", "/dataCOPY/")
            # star_index = glob_pattern.find("*")
            # slash_index = glob_pattern.rfind("/", 0, star_index)
            glob_test = glob.glob(glob_pattern)
            if not glob_test:
                raise Exception("glob_pattern not found: " + glob_pattern)
            return True
    print("Could not verify GLOB pattern: " + glob_pattern)


spatial_extent_south_africa = {
    "west": 10,
    "south": -40,
    "east": 40,
    "north": -20,
}

spatial_extent_johannesburg = {  # Johannesburg
    "west": 27,
    "south": -27,
    "east": 30,
    "north": -26,
}


def get_temporal_extent_from_argv(default):
    if len(sys.argv) >= 3:
        ret = [sys.argv[1], sys.argv[2]]
        print("Using time range arguments from arguments: " + repr(ret))
        return ret
    else:
        return default


def load_south_africa_shape() -> gpd.GeoDataFrame:
    """
    This can be used as a mask
    """
    shape_df = gpd.read_file(containing_folder / "shape/CNTR_RG_01M_2020_4326.shp")

    col_code = "ISO3_CODE"
    country_codes = ["ZAF", "LSO", "SWZ"]

    # Extract the rows that have 'ZAF', 'LSO', or 'SWZ' in the 'SOV_A3' column
    selected_rows = shape_df[shape_df[col_code].isin(country_codes)]

    # Combine the selected polygons into a single polygon
    unioned_polygon = selected_rows.geometry.unary_union

    # Convert the unioned polygon to a geopandas dataframe with a single row
    geodata_polygon = gpd.GeoDataFrame(geometry=[unioned_polygon])

    # OpenEO also prefers list of Polygons compared to a giant multipolygon
    if gpd.__version__ == "0.13.2":  # Versions after probably need index_parts too
        geodata_polygon = geodata_polygon.explode(index_parts=True)
    else:
        geodata_polygon = geodata_polygon.explode()

    # Simplify to avoid "Trying to construct a datacube with a bounds Extent(....) that is not entirely inside the global bounds..."
    geodata_polygon = gpd.GeoDataFrame(geometry=geodata_polygon.simplify(0.011))
    # geodata_polygon = geodata_polygon.buffer(0.001)
    # geodata_polygon = geodata_polygon.simplify(0.005)

    geodata_polygon = geodata_polygon.loc[geodata_polygon.area > 0.0001]  # Remove tini islands

    try:
        geodata_polygon = geodata_polygon.set_crs("EPSG:4326")
    except:
        # EPSG:4326 is the default anyway
        pass
    return geodata_polygon


def load_south_africa_geojson():
    geodata_polygon = load_south_africa_shape()

    geojson = json.loads(geodata_polygon.to_json())

    # Dump the json for easy debugging:
    with open(containing_folder / "south_africa_mask.json", "w") as f:
        json.dump(geojson, f, indent=2)

    return geojson


def load_johannesburg_geojson():
    with open(containing_folder / "johannesburg.json") as f:
        return json.load(f)


def load_udf(udf):
    """
    UDF: User Defined Function
    """
    with open(udf, "r+", encoding="utf8") as fs:
        return fs.read()


false = False
true = True
heavy_job_options = {
    "driver-memory": "10G",
    "driver-memoryOverhead": "5G",
    "driver-cores": "1",
    "executor-memory": "10G",
    "executor-memoryOverhead": "5G",
    "executor-cores": "1",
    "executor-request-cores": "600m",
    "max-executors": "22",
    "executor-threads-jvm": "7",
    "udf-dependency-archives": [],
    "logging-threshold": "info",
    "mount_tmp": false,  # or true
    "goofys": "false",
    "node_caching": true,
}


def custom_execute_batch(datacube, job_options=None, out_format="GTiff", run_type="batch_job"):
    try:
        # os.system('find . -type d -empty -delete')  # better run manually
        import inspect

        parent_filename = inspect.stack()[1].filename  # HACK!

        with open(parent_filename, "r") as file:
            job_description = "now: `" + str(now) + "` url: <" + datacube.connection.root_url + ">\n\n"
            job_description += "python code: \n\n\n```python\n" + file.read() + "```\n\n"

        try:
            from git import Repo  # pip install GitPython

            repo = Repo(os.path.dirname(parent_filename), search_parent_directories=True)
            job_description += "GIT URL: <" + list(repo.remotes[0].urls)[0] + ">\n\n"
            job_description += "GIT branch: `" + repo.active_branch.name + "` commit: `" + repo.active_branch.commit.hexsha + "`\n\n"
            job_description += "GIT changed files: " + ", ".join(map(lambda x: x.a_path, repo.index.diff(None))) + "\n"
        except Exception as e:
            print("Could not attach GIT info: " + str(e))

        output_dir = Path(
            os.path.dirname(parent_filename),
        ) / ("out-" + str(now).replace(":", "_").replace(" ", "_"))
        output_dir.mkdir(parents=True, exist_ok=True)
        print("output_dir=" + str(output_dir))
        datacube.print_json(file=output_dir / "process_graph.json", indent=2)
        print(str(output_dir.absolute()) + "/")
        if run_type == "sync":
            datacube.download(output_dir / (os.path.basename(parent_filename) + ".nc"))
        elif run_type == "batch_job":
            if job_options is None:
                job_options = dict()
            if "filename_prefix" not in job_options:
                job_options["filename_prefix"] = os.path.basename(parent_filename).replace(".py", "")

            job = datacube.create_job(
                title=os.path.basename(parent_filename),
                out_format=out_format,
                description=job_description,
                job_options=job_options,
            )
            with open(output_dir / "job_id.txt", mode="w") as f:
                f.write(job.job_id + "\n")
                f.write(datacube.connection.root_url + "\n")
            job.start_and_wait()
            job.get_results().download_files(output_dir)

            links = job.get_results().get_metadata()["links"]
            links_href = list(filter(lambda x: x["rel"] == "canonical", links))[0]["href"]
            print(f"STAC results url: {links_href}")
        else:
            raise Exception("Invalid run_type: " + run_type)

        # with open(output_dir / "logs.json", "w") as f:
        #     json.dump(job.logs(), f, indent=2)  # too often timeout

        # os.system('spd-say "Program terminated"')  # vocal feedback
    except KeyboardInterrupt:
        # No audio when user manually stops program
        pass
    except:
        # os.system('spd-say "Program failed"')  # vocal feedback
        raise
    finally:
        print("custom_execute_batch end time: " + str(datetime.datetime.now()))


def download_existing_job(job_id: str, conn: "openeo.Connection"):
    job = openeo.rest.job.BatchJob(job_id, conn)

    import inspect

    parent_filename = inspect.stack()[1].filename  # HACK!
    output_dir = Path(os.path.dirname(parent_filename)) / job_id
    output_dir.mkdir(parents=True, exist_ok=True)

    job.get_results().download_files(output_dir)


if __name__ == "__main__":
    # For testing
    load_south_africa_shape()
    # get_connection()
    # custom_execute_batch(None)
