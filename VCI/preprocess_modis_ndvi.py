import calendar

from openeo_utils.utils import *

month_map = {month: index for index, month in enumerate(calendar.month_name) if month}

for name in ["MIN", "MAX"]:
    input_path = "/home/emile/Desktop/ToShareWithVito/VCI/" + name + "/"
    output_path = "/dataCOPY/users/Public/emile.sonneveld/ANIN/VCI/MODIS_NDVI/" + name + "/"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    input_files = sorted(glob.glob(input_path + "*.tif"))

    for input_file in input_files:
        month_name = os.path.basename(input_file).split("_")[0]
        month_number = month_map[month_name]
        dest_file_name = "2020-" + str(month_number).zfill(2) + "-01.tif"
        dest_file = os.path.join(output_path, dest_file_name)
        print(dest_file_name)

        os.system(f"gdal_translate -of GTiff -co COMPRESS=DEFLATE {input_file} {dest_file}")
