import os
import datetime as date
from bounding_box_functions import *
from ndvi_extraction_functions import *
import pandas as pd
from ndvi_image_functions import denormalize_ndvi
from log_config import logger

def ndvi_timeseries_point(latitude, longitude, start_date, end_date, search_dir):
    search_files = os.listdir(search_dir)
    time_series = []

    for file in search_files:
        try:
            year, month, day = file.split('-')
            year, month, day = int(year), int(month), int(day)
            curr_date = date.datetime(year, month, day)

            if start_date <= curr_date <= end_date:
                curr_dir = os.path.join(search_dir, file)
                curr_list = os.listdir(curr_dir)
                curr_list = [f for f in curr_list if f.endswith('.tif')]

                point_found = False

                for image in curr_list:
                    try:
                        raster_path = os.path.join(curr_dir, 'raster_index.csv')
                        raster_df = pd.read_csv(raster_path)
                        curr_mbr = wkt_to_bounds(raster_df.loc[raster_df['FileName'] == image.split('.')[0], 'MBR'].iloc[0])
                        
                        if inBoundingBox_point(latitude, longitude, curr_mbr):
                            path = os.path.join(curr_dir, image)
                            pixel_val = get_ndvi_value_from_latlon(latitude, longitude, path)
                            denormalize_pixel_val = denormalize_ndvi(pixel_val)
                            
                            if pixel_val != 0:
                                time_series.append({
                                    'Date': curr_date,
                                    'File': image,
                                    'PixelValue': denormalize_pixel_val
                                })
                                logger.info(f"Date: {curr_date}: {pixel_val}")
                                point_found = True
                                break  
                        else:
                            logger.info(f'Point not in {image}')
                    except Exception as e:
                        logger.warning(f"Error processing image {image}: {e}")

                if point_found:
                    break  

        except Exception as e:
            logger.warning(f"Error processing file {file}: {e}")

    df = pd.DataFrame(time_series)
    return df

def ndvi_timeseries_range(wkt_string, start_date, end_date, search_dir):
    search_files = os.listdir(search_dir)
    time_series = []

    for file in search_files:
        try:
            year, month, day = file.split('-')
            year, month, day = int(year), int(month), int(day)
            curr_date = date.datetime(year, month, day)

            if start_date <= curr_date <= end_date:
                curr_dir = os.path.join(search_dir, file)
                curr_list = os.listdir(curr_dir)
                curr_list = [f for f in curr_list if f.endswith('.tif')]

                curr_ndvi_values = []
                raster_path = os.path.join(curr_dir, 'raster_index.csv')
                raster_df = pd.read_csv(raster_path)
                for image in curr_list:
                    try:
                        curr_mbr = wkt_to_bounds(raster_df.loc[raster_df['FileName'] == image.split('.')[0], 'MBR'].iloc[0])
                        
                        if inBoundingBox_range(wkt_string, curr_mbr):
                            path = os.path.join(curr_dir, image)
                            aoi_values = get_ndvi_from_range(wkt_string, path)
                            curr_ndvi_values.extend(aoi_values)
                    except Exception as e:
                        logger.warning(f"Error processing image {image}: {e}")

                ndvi_values_array = np.array(curr_ndvi_values)

                if ndvi_values_array.size == 0:
                    min_val, max_val, median_val, mean_val = np.nan, np.nan, np.nan, np.nan
                else:
                    ndvi_values_denormalized = denormalize_ndvi(ndvi_values_array)
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

        except Exception as e:
            logger.warning(f"Error processing file {file}: {e}")

    df = pd.DataFrame(time_series)
    return df