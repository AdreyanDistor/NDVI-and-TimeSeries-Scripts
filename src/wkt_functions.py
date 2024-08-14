from pyproj import Transformer
from shapely.geometry import mapping, Point, Polygon
import geopandas as gpd
import rioxarray as rxr
import numpy as np
from shapely import wkt
from shapely.geometry import mapping, Point, Polygon
def wkt_to_bounds(wkt_string, src_crs='EPSG:4326', dst_crs='EPSG:4326'):
    try:
        geometry = wkt.loads(wkt_string)
        if isinstance(geometry, Polygon):
            min_lon, min_lat, max_lon, max_lat = geometry.bounds
        elif isinstance(geometry, Point):
            min_lon, min_lat = geometry.x, geometry.y
            max_lon, max_lat = min_lon, min_lat
        else:
            raise ValueError("Only Point and Polygon geometries are accepted.")
        
        if src_crs != dst_crs:
            transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
            min_lon, min_lat = transformer.transform(min_lon, min_lat)
            max_lon, max_lat = transformer.transform(max_lon, max_lat)
        
        return [min_lon, min_lat, max_lon, max_lat]
    except Exception as e:
        print(f"Error Opening Geometry: {e}")
        return None

def bounds_to_wkt(min_lon, min_lat, max_lon, max_lat, src_crs='EPSG:4326', dst_crs='EPSG:4326'):
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    
    min_lon, min_lat = transformer.transform(min_lon, min_lat)
    max_lon, max_lat = transformer.transform(max_lon, max_lat)
    
    wkt_polygon = f"POLYGON (({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    return wkt_polygon
    
def import_wkt_file():
    return None


def load_wkt_as_geodataframe(wkt_string, crs='EPSG:4326'):
    geometry = wkt.loads(wkt_string)
    gdf = gpd.GeoDataFrame({'geometry': [geometry]}, crs=crs)
    return gdf