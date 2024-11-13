import pandas as pd
import rasterio
import numpy as np
import os

def extract_pixel_coords(input_tif_path, band_id=1, dire_name=''):
    with rasterio.open(input_tif_path) as src:
        band_arr = src.read(band_id)

        left, bottom, right, top = src.bounds
        nrows, ncols = band_arr.shape

        pixel_width = (right - left) / ncols
        pixel_height = (top - bottom) / nrows

        rows, cols = np.indices(band_arr.shape)

        lons = left + cols * pixel_width + (pixel_width / 2)
        lats = top - rows * pixel_height - (pixel_height / 2)

        x = lons.ravel()
        y = lats.ravel()
        values = band_arr.ravel()

        return x, y, values

def get_directory_pixel_values(directory_path, band_id=1):
    tif_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.tif')]
    if len(tif_files) != 0:
        dir_name = os.path.basename(directory_path)
        all_lons = []
        all_lats = []
        all_values = []

        for tif_file in tif_files:
            input_tif_path = os.path.join(directory_path, tif_file)
            lons, lats, values = extract_pixel_coords(input_tif_path, band_id)
            all_lons.extend(lons)
            all_lats.extend(lats)
            all_values.extend(values)

        df = pd.DataFrame({
            'longitude': all_lons,
            'latitude': all_lats,
            dir_name + ' values': all_values
        })
        return df
    return None

def combine_directories_to_csv(main_directory, output_csv, band_id=1):
    all_data = [] 
    for subdir, dirs, files in os.walk(main_directory):
        if subdir == main_directory:  
            continue
        df = get_directory_pixel_values(subdir, band_id=band_id)
        if df is not None:
            all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.replace(r'^\s*$', np.nan, regex=True)

        combined_df.to_csv(os.path.join(main_directory,output_csv), index=False)
        # print(f"Data saved to {output_csv}")

combine_directories_to_csv('Sentinel_Data', 'combined_pixel_data.csv', band_id=1)
