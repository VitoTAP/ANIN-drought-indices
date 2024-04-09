from openeo.udf import XarrayDataCube


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()
    array = array.shift(t=1)  # Uses NaN as filler value

    # No need to specify crs here
    return XarrayDataCube(array)
