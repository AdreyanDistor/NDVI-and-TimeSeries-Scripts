import rasterio as rio
import numpy as np
from pyproj import Transformer
import rioxarray as rxr
from wkt_functions import load_wkt_as_geodataframe
import geopandas
def get_ndvi_value_from_latlon(latitude, longitude, file_path, src_crs='EPSG:4326', dst_crs=None):
    with rio.open(file_path) as dataset:
        if dst_crs is None:
            dst_crs = dataset.crs.to_string()

        transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
        x, y = transformer.transform(longitude, latitude)

        try:
            row, col = dataset.index(x, y)
            if (0 <= row < dataset.height) and (0 <= col < dataset.width):
                pixel_value = dataset.read(1)[row, col]
                return pixel_value
            else:
                print(f"Coordinates ({latitude}, {longitude}) are out of bounds for this image.")
                return 0
        except IndexError as e:
            print(f"Coordinates ({latitude}, {longitude}) caused an index error: {e}")
            return 0

def get_ndvi_from_range(wkt_string, raster_path='', crs='EPSG:4326'):
    aoi_gdf = load_wkt_as_geodataframe(wkt_string, crs)
    integer_array = []
    
    try:
        ndvi_image = rxr.open_rasterio(raster_path)
        aoi_gdf = aoi_gdf.to_crs(ndvi_image.rio.crs.to_string())
        clipped_band = ndvi_image.rio.clip(aoi_gdf.geometry, from_disk=True)
        masked_band_values = clipped_band.where(clipped_band > 0, np.nan).values

        cleaned_array = masked_band_values[~np.isnan(masked_band_values)]
        integer_array = cleaned_array.astype(int)

    except Exception as e:
        print(f"An error occurred: {e} with file {raster_path}. NOT AN NDVI IMAGE")
        pass

    return integer_array