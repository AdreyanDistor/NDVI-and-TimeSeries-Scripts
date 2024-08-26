import os
import pandas as pd
from glob import glob
from queue import Queue
import threading as th
from osgeo import gdal
import numpy as np
from bounding_box_functions import get_boundingbox
from log_config import logger
import unittest

np.seterr(divide='ignore', invalid='ignore')

def import_red_nir_bands(red_file_path, nir_file_path):
    try:
        red = gdal.Open(red_file_path)
        nir = gdal.Open(nir_file_path)

        if red is None or nir is None:
            raise FileNotFoundError(f"Cannot open one of the files: {red_file_path} or {nir_file_path}")

        red_gt = red.GetGeoTransform()
        red_proj = red.GetProjection()
        nir_gt = nir.GetGeoTransform()
        nir_proj = nir.GetProjection()

        if not np.allclose(red_gt, nir_gt, atol=1e-6) or red_proj != nir_proj:
            raise ValueError("Geotransform or projection of the bands do not match!")

        red_band = red.GetRasterBand(1)
        red_array = red_band.ReadAsArray() / 10000.0

        nir_band = nir.GetRasterBand(1)
        nir_array = nir_band.ReadAsArray() / 10000.0

        if red_array is None or nir_array is None:
            raise ValueError("Failed to read one of the arrays")

        return red_array, nir_array, red_gt, red_proj

    except FileExistsError as f:
        logger.error(f"File exists error: {f}")
        return None, None, None, None
    except ValueError as v:
        logger.error(v)
        return None, None, None, None
    except Exception as e:
        logger.error(f"Error in import_red_nir_bands: Unable to process bands. {e}")
        return None, None, None, None

def calculate_ndvi(red, nir):
    try:
        ndvi = (nir - red) / (nir + red)
        ndvi = np.clip(ndvi, -1, 1)
        return ndvi
    except Exception:
        logger.error("Error in calculate_ndvi: Calculation failed")
        return np.full(red.shape, np.nan)

def normalize_ndvi(ndvi):
    ndvi_min = -1
    ndvi_max = 1
    ndvi_normalized = 1 + ((ndvi - ndvi_min) * (255 - 1)) / (ndvi_max - ndvi_min)
    return np.round(ndvi_normalized).astype(int)

def denormalize_ndvi(ndvi_normalized):
    ndvi_min = -1
    ndvi_max = 1
    ndvi = ndvi_min + ((ndvi_normalized - 1) / (255 - 1)) * (ndvi_max - ndvi_min)
    return ndvi

def export_ndvi_image(ndvi, gt, proj, file_name, file_path='', quality='60'):
    try:
        ndvi_normalized = normalize_ndvi(ndvi)
        binmask = np.where(ndvi_normalized > 0, 1, 0)
        clipped_data = np.where(binmask != 0, ndvi_normalized, np.nan)
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

    except Exception as e:
        logger.error(f"Error in export_ndvi_image: Unable to save NDVI image. {e}")

def initialize_queue(main_dir):
    sub_directories = [d for d in os.listdir(main_dir) if os.path.isdir(os.path.join(main_dir, d))]
    dir_queue = Queue()
    for dir in sub_directories:
        dir_queue.put(dir)
        logger.info(f"Queued directory: {dir}")
    return dir_queue

def process_directory(main_dir, output_directory, dir_queue, quality='60'):
    while not dir_queue.empty():
        dir = dir_queue.get()
        try:
            process_single_directory(main_dir, output_directory, dir, quality)
        except Exception:
            logger.error(f"Error processing directory {dir}")
        finally:
            dir_queue.task_done()

def process_single_directory(main_dir, output_directory, dir, quality='60'):
    full_path = os.path.join(output_directory, dir)
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
    raster_dict = {'FileName': [], 'MBR': []}

    for band4, band5, file_name in zip(band_4_files, band_5_files, file_names):
        if file_name not in curr_files:
            red, nir, gt, proj = import_red_nir_bands(band4, band5)
            
            if red is not None and nir is not None:
                ndvi = calculate_ndvi(red, nir)
                export_ndvi_image(ndvi, gt, proj, file_name, full_path, quality)
                logger.info(f"File {file_name} has been created")

                raster_path = os.path.join(full_path, file_name)
                MBR = get_boundingbox(raster_path + '.TIF')
                raster_dict['FileName'].append(file_name)
                raster_dict['MBR'].append(MBR)
            else:
                logger.error(f"Skipping file {file_name} due to errors reading bands")
        else:
            logger.info(f"File {file_name} already exists")
    
    raster_index_path = os.path.join(full_path, 'raster_index.csv')
    if not os.path.isfile(raster_index_path): 
        raster_index = pd.DataFrame(raster_dict)
        raster_index.to_csv(raster_index_path)

def create_and_start_threads(main_dir, output_directory, dir_queue, num_threads=4, quality='60'):
    threads = []
    for _ in range(num_threads):
        thread = th.Thread(target=process_directory, args=(main_dir, output_directory, dir_queue, quality))
        thread.start()
        threads.append(thread)
    return threads

def wait_for_threads_to_complete(threads):
    for thread in threads:
        thread.join()
