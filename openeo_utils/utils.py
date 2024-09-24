import datetime
import glob
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import geopandas as gpd
import openeo
import requests
from dateutil.relativedelta import relativedelta

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


def assert_glob_ok(glob_pattern: str, date_regex: Optional[str] = None):
    if glob_pattern.startswith("/data/") or glob_pattern.startswith("/dataCOPY/"):
        if os.path.exists("/dataCOPY/"):
            glob_pattern = glob_pattern.replace("/data/", "/dataCOPY/")
        star_index = glob_pattern.find("*")
        slash_index = glob_pattern.rfind("/", 0, star_index)
        base_path = glob_pattern[0: slash_index]
        if os.path.exists(base_path):
            glob_test = glob.iglob(glob_pattern)
            if not next(glob_test, False):
                raise Exception("glob_pattern not found: " + glob_pattern)
            if date_regex:
                def extract_date(s):
                    tup = re.match(date_regex, s).groups()
                    # Always use the first of the month
                    return datetime.date(int(tup[0]), int(tup[1]), 1)

                glob_test = list(glob.iglob(glob_pattern))
                dates = sorted(list(set(map(extract_date, glob_test))))
                min_date = min(dates)
                max_date = max(dates)

                months_difference = (max_date.year - min_date.year) * 12 + max_date.month - min_date.month
                missing_months = []
                for m in range(months_difference + 1):
                    expected_date = min_date + relativedelta(months=m)
                    if expected_date not in dates:
                        missing_months.append(expected_date)
                if missing_months:
                    raise Exception("Missing date(s): " + repr(missing_months))
                print(f"Dates OK! min_date:{min_date} max_date:{max_date} days:{len(glob_test)}")
                return
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
        # check if arg is date:
        if re.match(r"\d{4}-\d+-\d+", sys.argv[1]) and re.match(r"\d{4}-\d+-\d+", sys.argv[2]):
            ret = [sys.argv[1], sys.argv[2]]
            print("Using time range arguments from arguments: " + repr(ret))
            return ret
    return default


def smart_bool(value):
    """
    Convert given value to a boolean value, like `bool()` builtin,
    but in case of strings: interpret some common cases as `False`:
    "0", "no", "off", "false", ...
    """
    if isinstance(value, str) and value.lower() in ["0", "no", "off", "false"]:
        return False
    else:
        return bool(value)


def get_experimental_from_argv(default):
    for arg in sys.argv:
        flag = "--experimental="
        if arg.startswith(flag):
            val = arg[len(flag):]
            val = smart_bool(val)
            print("Using experimental from arguments: " + repr(val))
            return val
    return default


def get_out_format_from_argv(default):
    for arg in sys.argv:
        flag = "--out_format="
        if arg.startswith(flag):
            val = arg[len(flag):]
            print("Using out_format from arguments: " + repr(val))
            return val
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


def load_south_africa_secondary_catchment_geojson() -> gpd.GeoDataFrame:
    shape_df = gpd.read_file(containing_folder / "shape_secondary_catchment/Secondary Catchment.shp")

    # simplify shape_df while keeping all properties:
    for index, row in shape_df.iterrows():
        shape_df.at[index, 'geometry'] = row['geometry'].simplify(0.011)

    geojson = json.loads(shape_df.to_json())

    # Dump the json for easy debugging:
    with open(containing_folder / "shape_secondary_catchment/tmp_secondary_catchment.json", "w") as f:
        json.dump(geojson, f, indent=2)

    return geojson


def get_era5land_band_johan(era5land_name):
    if isinstance(era5land_name, list):
        raise Exception("Not implemented")
    assert era5land_name is not None
    glob_pattern = f"/data/users/Public/emile.sonneveld/ANIN/johan/reanalysis-era5-land_southafrica_float32/*/reanalysis-era5-land_{era5land_name}_*.tif"
    date_regex = r".*_(\d{4})(\d{2})(\d{2}).tif"
    assert_glob_ok(glob_pattern, date_regex)

    tmp = connection.load_disk_collection(
        format="GTiff",
        glob_pattern=glob_pattern,
        options=dict(date_regex=date_regex),
    )
    tmp._pg.arguments["featureflags"] = {"tilesize": 1}
    load_collection = tmp.rename_labels("bands", [era5land_name]) * 1.0
    return load_collection


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

        job_description = "now: `" + str(now) + "` url: <" + datacube.connection.root_url + ">\n\n"
        interactive_python = "ipykernel" in parent_filename
        if Path(parent_filename).exists():
            with open(parent_filename, "r") as file:
                job_description += "python code: \n\n\n```python\n" + file.read() + "```\n\n"

        try:
            from git import Repo  # pip install GitPython

            repo = Repo(os.path.dirname(parent_filename), search_parent_directories=True)
            job_description += "GIT URL: <" + list(repo.remotes[0].urls)[0] + ">\n\n"
            job_description += "GIT branch: `" + repo.active_branch.name + "` commit: `" + repo.active_branch.commit.hexsha + "`\n\n"
            job_description += "GIT changed files: " + ", ".join(map(lambda x: x.a_path, repo.index.diff(None))) + "\n"
        except Exception as e:
            if not interactive_python:
                print("Could not attach GIT info: " + str(e))

        if "drought-indices" in os.path.dirname(parent_filename):
            output_dir = Path(os.path.dirname(parent_filename))
        else:
            output_dir = Path(os.getcwd())

        output_dir = output_dir / ("out-" + str(now).replace(":", "_").replace(" ", "_"))
        output_dir.mkdir(parents=True, exist_ok=True)
        print("output_dir=" + str(output_dir))
        datacube.print_json(file=output_dir / "process_graph.json", indent=2)
        print(str(output_dir.absolute()) + "/")
        if run_type == "sync":
            datacube.download(output_dir / (os.path.basename(parent_filename) + ".nc"))
        elif run_type == "batch_job":
            if job_options is None:
                job_options = dict()

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
        return output_dir
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
    load_south_africa_secondary_catchment_geojson()
    # get_connection()
    # custom_execute_batch(None)
