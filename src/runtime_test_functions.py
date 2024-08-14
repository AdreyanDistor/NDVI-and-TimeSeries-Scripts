import time
import csv
import os
from glob import glob
from shapely import wkt
from time_series_functions import *
import old_ndvi_functions_testing as old
import datetime as date

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    return wrapper

def calculate_polygon_area(polygon_wkt):
    shape = wkt.loads(polygon_wkt)
    return shape.area

def row_exists(filename, lat_lon=None, polygon_wkt=None):
    if not os.path.isfile(filename):
        return False

    polygon_area = calculate_polygon_area(polygon_wkt) if polygon_wkt else None

    with open(filename, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if lat_lon and 'Latitude, Longitude' in row and row['Latitude, Longitude'] == lat_lon:
                return True
            if polygon_area and 'Polygon Area' in row and abs(float(row['Polygon Area']) - polygon_area) < 1e-6:
                return True
    return False

def compare_functions_to_csv(func_old, func_new, params, filename, lat_lon=None, polygon_wkt=None):
    if row_exists(filename, lat_lon, polygon_wkt):
        print(f'Row with latitude/longitude or polygon area already exists in {filename}')
        return

    file_exists = os.path.isfile(filename)
    function_comparison = []

    old_func = measure_time(func_old)
    new_func = measure_time(func_new)

    old_result, exec_time_old = old_func(*params)
    new_result, exec_time_new = new_func(*params)

    abs_diff = abs(exec_time_new - exec_time_old)

    columns = ['Function', 'Old_Function_Time', 'New_Function_Time', 'Difference']
    row = [func_old.__name__, exec_time_old, exec_time_new, abs_diff]

    if lat_lon is not None:
        columns.append('Latitude, Longitude')
        row.append(lat_lon)

    if polygon_wkt is not None:
        columns.append('Polygon Area')
        columns.append('WKT String')
        polygon_area = calculate_polygon_area(polygon_wkt)
        row.append(polygon_area)
        row.append(polygon_wkt)

    function_comparison.append(row)
    
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(columns)
        writer.writerows(function_comparison)
    
    print(f'Function runtimes have been written to {filename}')

def load_params_from_files(lat_lon_file, wkt_file):
    with open(lat_lon_file, 'r') as f:
        lat_lon_lines = f.readlines()
    
    with open(wkt_file, 'r') as f:
        wkt_lines = f.readlines()

    points = [line.strip() for line in lat_lon_lines]
    polygons = [line.strip() for line in wkt_lines]

    return points, polygons

ndvi_timeseries_point_old = old.ndvi_timeseries_point
ndvi_timeseries_range_old = old.ndvi_timeseries_range

point_params = ('points.txt', 'polygons.txt')
points, polygons = load_params_from_files(*point_params)

for point in points:
    latitude, longitude = point.split(',')
    latitude, longitude = float(latitude.strip()), float(longitude.strip())
    compare_functions_to_csv(
        ndvi_timeseries_point_old, 
        ndvi_timeseries_point, 
        (latitude, longitude, date.datetime(2023, 1, 1), date.datetime(2023, 1, 5), 'landsat_8_and_satellite_data/Colorado_Basin_Landsat8_4B_ndvi_compressed'),
        'compare_point.csv',
        lat_lon=point
    )

for polygon in polygons:
    compare_functions_to_csv(
        ndvi_timeseries_range_old, 
        ndvi_timeseries_range, 
        (polygon, date.datetime(2023, 1, 1), date.datetime(2023, 1, 5), 'landsat_8_and_satellite_data/Colorado_Basin_Landsat8_4B_ndvi_compressed'),
        'compare_range.csv',
        polygon_wkt=polygon
    )
