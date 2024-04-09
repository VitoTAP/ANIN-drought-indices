import gc
import os
import shutil
import tempfile
import traceback
import typing
from pathlib import Path
import fnmatch

import geopandas as gpd
import osgeo.gdal
import osgeo.ogr
import pyproj
import rioxarray as rxr
import rioxarray.merge
import shapely.geometry
import shapely.ops
import shapely.ops
from shapely.geometry import mapping

# Run this to get the latest data:
# cd /dataCOPY/users/Public/emile.sonneveld/GLASS_FAPAR_Layer
# wget --mirror --domains www.glass.umd.edu --no-parent http://www.glass.umd.edu/FAPAR/MODIS/250m/ --accept "*.h19v1[123].*.hdf" --accept "*.h20v1[123].*.hdf" --accept "index.html"

# http://www.glass.umd.edu/Overview.html
input_path = Path(
    "/dataCOPY/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/www.glass.umd.edu/FAPAR/MODIS/250m/"
)
if not input_path.exists():
    raise Exception("Path not found: " + str(input_path))

output_path = Path(
    "/data/users/Public/emile.sonneveld/GLASS_FAPAR_Layer/tiff_collection/"
)
if not output_path.exists():
    raise Exception("Path not found: " + str(output_path))

tempdir = Path(tempfile.mkdtemp(prefix="tmp_" + os.path.basename(__file__)))



def read_hdf_extent_polygon(hdf_file_path) -> typing.Optional[gpd.GeoDataFrame]:
    hdf_data = osgeo.gdal.Open(str(hdf_file_path))
    if hdf_data is None:
        return None
    geo_transform = hdf_data.GetGeoTransform()
    minx = geo_transform[0]
    maxy = geo_transform[3]
    maxx = minx + geo_transform[1] * hdf_data.RasterXSize
    miny = maxy + geo_transform[5] * hdf_data.RasterYSize

    hdf_crs = pyproj.CRS.from_user_input(hdf_data.GetProjection())
    return gpd.GeoDataFrame(
        geometry=[shapely.geometry.box(minx, miny, maxx, maxy)], crs=hdf_crs
    )


def get_child_dirs(dir_path):
    return sorted([name for name in os.listdir(dir_path) if os.path.isdir(dir_path / name)])


if __name__ == "__main__":
    from openeo_utils.utils import *

    geodata_polygon = load_south_africa_shape()
    south_africa_polygon = shapely.ops.unary_union(geodata_polygon.geometry)
    south_africa_polygon = south_africa_polygon.convex_hull

    year_parts = get_child_dirs(input_path)
    year_parts = list(filter(lambda x: "2001" in x, year_parts))  # for debugging
    for year_part in year_parts:
        days_in_year = get_child_dirs(input_path / year_part)
        for day_in_year in days_in_year:
            print("Scanning: " + str(input_path / year_part / day_in_year))
            file_list = list((input_path / year_part / day_in_year).rglob("*.hdf"))

            year = int(year_part[0:4])
            day = int(day_in_year)
            date = datetime.date(year, 1, 1) + datetime.timedelta(days=day)
            output_file = (
                    output_path
                    / "{:04d}".format(date.year)
                    / "{:02d}".format(date.month)
                    / "{:02d}".format(date.day)
                    / "{:04d}-{:02d}-{:02d}-FAPAR-MODIS.tiff".format(date.year, date.month, date.day)
            )
            output_file.parent.mkdir(parents=True, exist_ok=True)
            if output_file.exists():
                print("output already exists: " + str(output_file))
                continue

            try:
                file_path_filtered = []
                for file_path in file_list:
                    hdf_df = read_hdf_extent_polygon(file_path)
                    if hdf_df is None:
                        print("Could not read file: " + str(file_path))
                        continue

                    if geodata_polygon is not None:
                        hdf_df = hdf_df.to_crs(geodata_polygon.crs)
                        hdf_polygon = shapely.ops.unary_union(hdf_df.geometry)
                        overlaps = south_africa_polygon.overlaps(hdf_polygon)
                        if not overlaps:
                            continue
                    file_path_filtered.append(file_path)
                filtered_a = fnmatch.filter(map(lambda x: str(x), file_path_filtered), "*.h19v1[123].*.hdf")
                filtered_b = fnmatch.filter(map(lambda x: str(x), file_path_filtered), "*.h20v1[123].*.hdf")
                file_path_filtered_2 = list(map(lambda x: Path(x), set(filtered_a).union(set(filtered_b))))
                print(file_path_filtered_2)

                tiles_to_merge = []
                for file_path in file_path_filtered_2:
                    file_path_tiff = (
                            output_path
                            / "{:04d}".format(date.year)
                            / "{:02d}".format(date.month)
                            / "{:02d}".format(date.day)
                            / (file_path.name + ".tiff"))
                    # file_path_tiff = f"{tempdir / file_path.name}.tif"

                    osgeo.gdal.Warp(
                        str(file_path_tiff),
                        f"{file_path}",
                        dstSRS='EPSG:4326',
                        options=["COMPRESS=DEFLATE"]
                    )
                    continue
                    tile1 = rxr.open_rasterio(file_path_tiff, masked=True)
                    os.remove(file_path_tiff)
                    # import xarray
                    # tile1 = xarray.open_rasterio(file_path_tiff)
                    tiles_to_merge.append(tile1)

                if len(tiles_to_merge) > 0:
                    merged_raster = rxr.merge.merge_arrays(dataarrays=tiles_to_merge)
                    # Clipping the mosiac to the AOI
                    clipped = merged_raster.rio.clip(geodata_polygon.geometry.apply(mapping),
                                                     crs=geodata_polygon.crs,
                                                     all_touched=True,
                                                     from_disk=True,
                                                     ).squeeze()
                    # Export clipped images
                    clipped.rio.to_raster(output_file, compress='DEFLATE')
                gc.collect()  # Avoid "numpy.core._exceptions.MemoryError: Unable to allocate array with shape (1, 12206, 14660) and data type float64"

            except Exception as e:
                print("Problem with: " + str(input_path / year_part / day_in_year))
                print(e)
                print(traceback.format_exc())
    print("Done")

shutil.rmtree(tempdir, ignore_errors=True)
