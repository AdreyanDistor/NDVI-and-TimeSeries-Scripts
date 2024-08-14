import os
import numpy as np
import threading
from queue import Queue
import glob
from osgeo import gdal
import rasterio
from pyproj import Transformer
import shapely.wkt
import geopandas as gpd
import rioxarray as rxr
import pandas as pd
import datetime as date
from ndvi_image_functions import denormalize_ndvi

# Imports Red and NIR Bands using file paths, also returns the transform and projection
def import_red_nir_bands(red_file_path, nir_file_path):
    red = gdal.Open(red_file_path)
    nir = gdal.Open(nir_file_path)

    # Check if the geotransform and projection match
    red_gt = red.GetGeoTransform()
    red_proj = red.GetProjection()
    nir_gt = nir.GetGeoTransform()
    nir_proj = nir.GetProjection()

    if red_gt != nir_gt or red_proj != nir_proj:
        raise ValueError("Geotransform or projection of the bands do not match!")

    red_band = red.GetRasterBand(1)
    red = red_band.ReadAsArray() / 10000.0

    nir_band = nir.GetRasterBand(1)
    nir = nir_band.ReadAsArray() / 10000.0

    return red, nir, red_gt, red_proj

# Returns NDVI
def calculate_ndvi(red, nir):
    ndvi = (nir - red) / (nir + red)
    return ndvi

# Export compressed NDVI image
def export_ndvi_image(ndvi, gt, proj, file_name, file_path='', quality='60'):
    # Normalizing [-1, 1] to [1, 255], undefined values become 0's
    ndvi_min = np.nanmin(ndvi)
    ndvi_max = np.nanmax(ndvi)
    ndvi_normalized = 1 + ((ndvi - ndvi_min) * (255 - 1)) / (ndvi_max - ndvi_min)
    ndvi_normalized = np.nan_to_num(ndvi_normalized, nan=0.0)
    ndvi_normalized = np.round(ndvi_normalized).astype(int)

    # Create the mask of the image, to avoid exporting a background
    binmask = np.where(ndvi_normalized > 0, 1, 0)

    # Clip the data, so we have no background
    clipped_data = np.where(binmask != 0, ndvi_normalized, np.nan)

    # Export as a compressed TIFF file
    nodata_value = 0
    clipped_data[np.isnan(clipped_data)] = nodata_value

    driver = gdal.GetDriverByName("GTiff")
    if ".tif" not in file_name:
        file_name += ".tif"
    file_name = os.path.join(file_path, file_name)

    outds = driver.Create(file_name, xsize=ndvi_normalized.shape[1], ysize=ndvi_normalized.shape[0], bands=1, eType=gdal.GDT_Byte, options=["COMPRESS=JPEG", "JPEG_QUALITY=" + str(quality)])
    outds.SetGeoTransform(gt)
    outds.SetProjection(proj)

    outband = outds.GetRasterBand(1)
    outband.WriteArray(ndvi_normalized)
    outband.SetNoDataValue(nodata_value)

    outband.FlushCache()
    outband = None
    outds = None
    
#Initializes queue, takes in the main directory
def initialize_queue(main_dir):
    sub_directories = os.listdir(main_dir)
    dir_queue = Queue()
    for dir in sub_directories:
        dir_queue.put(dir)
    return dir_queue

#Processes all files in a directory
def process_directory(main_dir, current_working_directory, dir_queue, quality = '60'):
    while not dir_queue.empty():
        dir = dir_queue.get()
        try:
            process_single_directory(main_dir, current_working_directory, dir, quality)
        #Indicates a task is completed, whether a file does, or doesn't exist already
        finally:
            dir_queue.task_done()

#Helper Function for converting bands to ndvi images
def process_single_directory(main_dir, current_working_directory, dir, quality = '60'):
    full_path = os.path.join(current_working_directory, main_dir + "_ndvi_compressed", dir)
    os.makedirs(full_path, exist_ok=True)

    curr_files = os.listdir(full_path)
    curr_files = [file.split('.')[0] for file in curr_files]

    band_4_files = glob(os.path.join(main_dir, dir, "*_B4.TIF"))
    band_5_files = glob(os.path.join(main_dir, dir, "*_B5.TIF"))
    file_names = os.listdir(os.path.join(main_dir, dir))
    file_names = [name for name in file_names if name.lower().endswith('.tif')]
    file_names = [name.split('.')[0] for name in file_names]
    file_names.pop()
    file_names = set([name.split('_B')[0] for name in file_names])

    for band4, band5, file_name in zip(band_4_files, band_5_files, file_names):
        if file_name not in curr_files:
            red, nir, gt, proj = import_red_nir_bands(band4, band5)
            ndvi = calculate_ndvi(red, nir)
            export_ndvi_image(ndvi, gt, proj, file_name, full_path, quality)
            print(f"File {file_name} has been made")
        else:
            print(f"File {file_name} already exists")

#Used to initalize threads, and assign them each to tasks
def create_and_start_threads(main_dir, current_working_directory, dir_queue, num_threads=4, quality = '60'):
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=process_directory, args=(main_dir, current_working_directory, dir_queue, quality))
        thread.start()
        threads.append(thread)
    return threads

#Runs all threads
def wait_for_threads_to_complete(threads):
    for thread in threads:
        thread.join()

# #Used to convert ndvi from [1 to 255] back to float values
# def denormalize_ndvi_array(ndvi_normalized):
#     ndvi_array = np.array(ndvi_normalized)  # Convert list to NumPy array
#     ndvi_min = np.nanmin(ndvi_array)
#     ndvi_max = np.nanmax(ndvi_array)
#     denorm = ndvi_min + ((ndvi_array - 1) / (255 - 1)) * (ndvi_max - ndvi_min)
#     return denorm

#Extracts Ndvi Value from pixels based on latitude and longitude
def get_ndvi_value_from_latlon(latitude, longitude, file_path):
    with rasterio.open(file_path) as dataset:
        src_crs = 'EPSG:4326'
        dst_crs = dataset.crs
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
    return None

#Extracts time_series from latitude and longitude
def ndvi_timeseries_point(latitude, longitude, start_date, end_date, search_dir):
    search_files = os.listdir(search_dir)
    time_series = []

    for file in search_files:
        year, month, day = file.split('-')
        year, month, day = int(year), int(month), int(day)
        curr_date = date.datetime(year, month, day)

        if start_date <= curr_date <= end_date:
            curr_dir = os.path.join(search_dir, file)
            curr_list = os.listdir(curr_dir)
            curr_list = [f for f in curr_list if f.endswith('.tif')]

            for image in curr_list:
                path = os.path.join(curr_dir, image)
                pixel_val = get_ndvi_value_from_latlon(latitude, longitude, path)

                if pixel_val != 0:
                    time_series.append({
                        'Date': curr_date,
                        'File': image,
                        'PixelValue': pixel_val
                    })
                    print(f"Date: {curr_date}: {pixel_val}")

    df = pd.DataFrame(time_series)
    return df

# Helper functions: Uses string to convert wkt to GeoDataFrame, to easily clip values
def load_wkt_as_geodataframe(wkt_string, crs='EPSG:4326'):
    geometry = shapely.wkt.loads(wkt_string)
    gdf = gpd.GeoDataFrame({'geometry': [geometry]}, crs=crs)
    return gdf

#Clips ndvi images using a given wekt string
def clip_with_wkt(wkt_string, raster_path = '', crs='EPSG:4326'):
    aoi_gdf = load_wkt_as_geodataframe(wkt_string)
    integer_array = []
    ndvi_image = rxr.open_rasterio(raster_path)

    aoi_gdf = aoi_gdf.to_crs(ndvi_image.rio.crs.to_string())
    try:
        clipped_band = ndvi_image.rio.clip(aoi_gdf.geometry, from_disk=True)
        masked_band_values = (clipped_band.where(clipped_band > 0, np.nan)).values

        cleaned_array = masked_band_values[~np.isnan(masked_band_values)]
        integer_array = cleaned_array.astype(int)

    except Exception as e:
        print(f"An error occurred: {e} with file {raster_path}")
    return integer_array


#does not parse multpiple polgyons yet
#Returns
def ndvi_timeseries_range(wkt_string, start_date, end_date, search_dir):
    search_files = os.listdir(search_dir)
    time_series = []

    for file in search_files:
        year, month, day = file.split('-')
        year, month, day = int(year), int(month), int(day)
        curr_date = date.datetime(year, month, day)

        if start_date <= curr_date <= end_date:
            curr_dir = os.path.join(search_dir, file)
            curr_list = os.listdir(curr_dir)
            curr_list = [f for f in curr_list if f.endswith('.tif')]

            curr_ndvi_values = []
            for image in curr_list:
                path = os.path.join(curr_dir, image)
                aoi_values = clip_with_wkt(wkt_string, path)
                curr_ndvi_values.extend(aoi_values)  # Use extend instead of concatenate

            ndvi_values_array = np.array(curr_ndvi_values)

            if ndvi_values_array.size == 0:
                min_val, max_val, median_val, mean_val = np.nan, np.nan, np.nan, np.nan
            else:
                ndvi_values_denormalized = denormalize_ndvi(ndvi_values_array)
                
                if ndvi_values_denormalized.size == 0:
                    min_val, max_val, median_val, mean_val = np.nan, np.nan, np.nan, np.nan
                else:
                    min_val = np.nanmin(ndvi_values_denormalized)
                    max_val = np.nanmax(ndvi_values_denormalized)
                    median_val = np.nanmedian(ndvi_values_denormalized)
                    mean_val = np.nanmean(ndvi_values_denormalized)

            time_series.append({
                'Date': curr_date,
                'NDVI_MIN': min_val,
                'NDVI_MAX': max_val,
                'NDVI_MEDIAN': median_val,
                'NDVI_MEAN': mean_val,
            })

    df = pd.DataFrame(time_series)
    return df

