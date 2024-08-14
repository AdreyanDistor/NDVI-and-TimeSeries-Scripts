import os
import datetime as date
import pandas as pd
import numpy as np
from time import time
from bounding_box_functions import *
from ndvi_extraction_functions import *
from ndvi_image_functions import denormalize_ndvi

def ndvi_timeseries_range(wkt_string, start_date, end_date, search_dir):
    search_files = os.listdir(search_dir)
    time_series = []

    total_start_time = time()

    iterating_files_time = 0
    reading_metadata_time = 0
    spatial_operations_time = 0
    ndvi_processing_time = 0
    array_operations_time = 0
    dataframe_construction_time = 0

    for file in search_files:
        iter_start_time = time()

        year, month, day = file.split('-')
        year, month, day = int(year), int(month), int(day)
        curr_date = date.datetime(year, month, day)

        iter_end_time = time()
        iterating_files_time += (iter_end_time - iter_start_time)

        if start_date <= curr_date <= end_date:
            curr_dir = os.path.join(search_dir, file)
            curr_list = os.listdir(curr_dir)
            curr_list = [f for f in curr_list if f.endswith('.tif')]

            curr_ndvi_values = []

            metadata_start_time = time()
            raster_path = os.path.join(curr_dir, 'raster_index.csv')
            raster_df = pd.read_csv(raster_path)
            metadata_end_time = time()
            reading_metadata_time += (metadata_end_time - metadata_start_time)

            for image in curr_list:
                spatial_start_time = time()
                curr_mbr = wkt_to_bounds(raster_df.loc[raster_df['FileName'] == image.split('.')[0], 'MBR'].iloc[0])
                if inBoundingBox_range(wkt_string, curr_mbr):
                    spatial_end_time = time()
                    spatial_operations_time += (spatial_end_time - spatial_start_time)

                    ndvi_start_time = time()
                    path = os.path.join(curr_dir, image)
                    print(image, ": MATCHES FILE")
                    aoi_values = get_ndvi_from_range(wkt_string, path)
                    curr_ndvi_values.extend(aoi_values)
                    ndvi_end_time = time()
                    ndvi_processing_time += (ndvi_end_time - ndvi_start_time)

            array_start_time = time()
            ndvi_values_array = np.array(curr_ndvi_values)
            if ndvi_values_array.size == 0:
                min_val, max_val, median_val, mean_val = np.nan, np.nan, np.nan, np.nan
            else:
                ndvi_values_denormalized = denormalize_ndvi(ndvi_values_array)
                min_val = np.nanmin(ndvi_values_denormalized)
                max_val = np.nanmax(ndvi_values_denormalized)
                median_val = np.nanmedian(ndvi_values_denormalized)
                mean_val = np.nanmean(ndvi_values_denormalized)
            array_end_time = time()
            array_operations_time += (array_end_time - array_start_time)

            time_series.append({
                'Date': curr_date,
                'NDVI_MIN': min_val,
                'NDVI_MAX': max_val,
                'NDVI_MEDIAN': median_val,
                'NDVI_MEAN': mean_val,
            })

    dataframe_start_time = time()
    df = pd.DataFrame(time_series)
    dataframe_end_time = time()
    dataframe_construction_time += (dataframe_end_time - dataframe_start_time)

    total_end_time = time()
    total_time = total_end_time - total_start_time

    print("Time Breakdown:")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Iterating through files: {iterating_files_time:.2f} seconds")
    print(f"Reading metadata: {reading_metadata_time:.2f} seconds")
    print(f"Spatial operations: {spatial_operations_time:.2f} seconds")
    print(f"NDVI processing: {ndvi_processing_time:.2f} seconds")
    print(f"Array operations and statistics: {array_operations_time:.2f} seconds")
    print(f"Dataframe construction: {dataframe_construction_time:.2f} seconds")

    return df

import datetime as dt

# Define the parameters for the test case
wkt_string = """
POLYGON ((-113.91450425821238 32.842567780110215, -116.7781932355039 35.73363365234446, -110.75717017922076 45.852774770139696, -108.41262916058245 44.91028979627324, -113.91450425821238 32.842567780110215))
"""
start_date = dt.datetime(2023, 1, 1)
end_date = dt.datetime(2023, 12, 31)
search_dir = "landsat_8_and_satellite_data/Colorado_Basin_Landsat8_4B_ndvi_compressed"

# Run the ndvi_timeseries_range function
ndvi_timeseries_df = ndvi_timeseries_range(wkt_string, start_date, end_date, search_dir)

# Print the resulting DataFrame
print(ndvi_timeseries_df)
