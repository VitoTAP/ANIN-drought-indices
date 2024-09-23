import os.path
import pprint
import subprocess
from pathlib import Path
from shutil import rmtree

# run pip install -e . in the root directory to install this package
import stacbuilder


def main(terrascope_dir):
    assert " " not in terrascope_dir
    terrascope_dir = terrascope_dir.rstrip("/")
    containing_folder = Path(__file__).parent

    collection_config_path = containing_folder / "config-collection.json"
    tiff_input_path = Path(terrascope_dir)
    tiffs_glob = '**/*_total_precipitation.tif*'

    output_path = Path(os.path.abspath(containing_folder / "tmp_stac/"))
    overwrite = True

    publish_path = Path(terrascope_dir).parent / "stac/"

    input_files = stacbuilder.list_input_files(
        glob=tiffs_glob,
        input_dir=tiff_input_path,
        max_files=None
    )
    assert input_files
    print(f"Found {len(input_files)} input files. 5 first files:")
    for i in input_files[:5]: print(i)

    asset_metadata = stacbuilder.list_asset_metadata(
        collection_config_path=collection_config_path,
        glob=tiffs_glob,
        input_dir=tiff_input_path,
        max_files=5
    )
    assert asset_metadata

    stac_items, failed_files = stacbuilder.list_stac_items(
        collection_config_path=collection_config_path,
        glob=tiffs_glob,
        input_dir=tiff_input_path,
        max_files=0
    )
    print(f"Found {len(stac_items)} STAC items")
    if failed_files: print(f"Failed files: {failed_files}")

    if os.path.exists(output_path):
        rmtree(output_path)

    stacbuilder.build_grouped_collections(
        collection_config_path=collection_config_path,
        glob=tiffs_glob,
        input_dir=tiff_input_path,
        output_dir=output_path,
        overwrite=overwrite,
    )

    stacbuilder.load_collection(
        collection_file=output_path / "collection.json"
    )

    stacbuilder.validate_collection(
        collection_file=output_path / "collection.json",
    )
    cmd = f"""rsync -av --delete --no-perms --no-owner --no-group {output_path}/ {publish_path}"""
    print(cmd)
    subprocess.call(cmd.split())

    return publish_path


if __name__ == "__main__":
    publish_path = main("/data/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-v4/tiff_collection/")
