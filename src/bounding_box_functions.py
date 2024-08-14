import rasterio as rio
from wkt_functions import *
#Returns bounding box in this order: min_lon, min_lat, max_lon, max_lat
def get_boundingbox(raster_path, user_crs = 'EPSG:4326'):   
    with rio.open(raster_path) as dataset:
        bounds = dataset.bounds
        crs = dataset.crs
    
    transformer = Transformer.from_crs(crs, user_crs, always_xy=True)
    
    # Convert the bounds
    min_lon, min_lat = transformer.transform(bounds.left, bounds.bottom)
    max_lon, max_lat = transformer.transform(bounds.right, bounds.top)
    
    return bounds_to_wkt(min_lon, min_lat, max_lon, max_lat)


def transform_coordinates(lat, lon, src_crs='EPSG:4326', dst_crs='EPSG:4326'):
    transformer = rio.from_crs(src_crs, dst_crs, always_xy=True)
    x, y = transformer.transform(lon, lat)  
    return x, y

#Helper Functions: Assuming Bounding box information has been given
#takes mbr as: [min_lon, min_lat, max_lon, max_lat]
def inBoundingBox_point(point_latitude, point_longitude, mbr=[0, 0, 0, 0], crs='EPSG:4326', target_crs='EPSG:4326'):
    min_lon, min_lat, max_lon, max_lat = mbr
    if crs != target_crs:
        point_lon, point_lat = transform_coordinates(point_latitude, point_longitude, src_crs=crs, dst_crs=target_crs)
    else:
        point_lon, point_lat = point_longitude, point_latitude
    return (min_lat <= point_lat <= max_lat and min_lon <= point_lon <= max_lon)

def inBoundingBox_range(wkt_string='', mbr=[0, 0, 0, 0], crs='EPSG:4326', target_crs='EPSG:4326'):
    min_lon, min_lat, max_lon, max_lat = mbr
    wkt_min_lon, wkt_min_lat, wkt_max_lon, wkt_max_lat = wkt_to_bounds(wkt_string)
    if crs != target_crs:
        wkt_min_lon, wkt_min_lat = transform_coordinates(wkt_min_lat, wkt_min_lon, src_crs=crs, dst_crs=target_crs)
        wkt_max_lon, wkt_max_lat = transform_coordinates(wkt_max_lat, wkt_max_lon, src_crs=crs, dst_crs=target_crs)
    return (min_lon <= wkt_max_lon and wkt_min_lon <= max_lon and min_lat <= wkt_max_lat and wkt_min_lat <= max_lat)