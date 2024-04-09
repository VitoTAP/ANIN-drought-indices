import os
import json
import re
import glob

# stac_path = "/dataCOPY/MTDA/MODIS/GLASS_FAPAR/tiff_collection_months_sd/STAC_catalogs/v0.2/"
stac_path = "/dataCOPY/MTDA/MODIS/GLASS_FAPAR/tiff_collection_months_mean/STAC_catalogs/v0.2/"
# stac_path = "/dataCOPY/users/Public/emile.sonneveld/ANIN/CROP_MASK/CROP_MASK_STAC/CROP_MASK_2020/"
# stac_path = "/dataCOPY/users/Public/emile.sonneveld/ANIN/VCI/MODIS_NDVI/MODIS_NDVI_DERIVATIONS_STAC/MODIS_NDVI_DERIVATIONS_2020/"

start_year = 2020
end_year = 2025
files = list(map(lambda x: os.path.relpath(x, stac_path), glob.glob(stac_path + "*/*.json")))
files_to_delete = list(filter(lambda x: str(start_year) not in x, files))
for file_name in files_to_delete:
    print("rm " + file_name)
    os.remove(stac_path + file_name)
files = list(filter(lambda x: str(start_year) in x, files))
files.sort()

for file_name in files:
    with open(stac_path + file_name, "r") as json_file:
        text = json_file.read()
        data = json.loads(text)

        for asset in data["assets"]:
            data["assets"][asset]["href"] = data["assets"][asset]["href"].replace("/dataCOPY/", "/data/")

        # Save on same location:
        json.dump(data, open(stac_path + file_name, "w"), indent=4)

    with open(stac_path + file_name, "r") as json_file:
        text = json_file.read()
        data = json.loads(text)
        print(data["properties"]["datetime"])
        for year in range(start_year, end_year):
            data_new = data.copy()
            data_new["id"] = re.sub(r"\d{4}-", str(year) + "-", data_new["id"])
            data_new["properties"]["datetime"] = re.sub(r"\d{4}-", str(year) + "-", data_new["properties"]["datetime"])
            data_new["properties"]["start_datetime"] = re.sub(r"\d{4}-", str(year) + "-", data_new["properties"]["start_datetime"])
            data_new["properties"]["end_datetime"] = re.sub(r"\d{4}-", str(year) + "-", data_new["properties"]["end_datetime"])

            file_name_new = re.sub(r"\d{4}-", str(year) + "-", file_name)
            file_path_new = stac_path + file_name_new
            if not os.path.exists(file_path_new):
                json.dump(data_new, open(file_path_new, "w"), indent=4)

files = list(map(lambda x: os.path.relpath(x, stac_path), glob.glob(stac_path + "*/*.json")))  # again
files.sort()
with open(stac_path + "collection.json", "r") as json_file:
    text = json_file.read()
    data = json.loads(text)

# filter out items from links:
links = data["links"]
links_new = list(filter(lambda x: x["rel"] != "item", links))

for file_name in files:
    links_new.append({"rel": "item", "href": "./" + os.path.relpath(stac_path + file_name, stac_path), "type": "application/json", "title": file_name})
data["links"] = links_new
orig_interval = data["extent"]["temporal"]["interval"][0][1]
data["extent"]["temporal"]["interval"][0][1] = re.sub(r"\d{4}-", str(end_year) + "-", orig_interval)
print("If this collection is contained in another collection, the interval should be updated there too!")

with open(stac_path + "collection.json", "w") as json_file:
    json.dump(data, json_file, indent=4)
