#https://gis.stackexchange.com/questions/346288/extract-all-pixels-values-from-geotiff-with-python

import pandas as pd
import rasterio
import numpy as np

def extract_pixel_coords(input_tif_path, output_csv_path='pixel_coords.csv', band_id=1):
    with rasterio.open(input_tif_path) as src:
        band_arr = src.read(band_id)

        left, bottom, right, top = src.bounds

        nrows, ncols = band_arr.shape

        pixel_width = (right - left) / ncols
        pixel_height = (top - bottom) / nrows

        rows, cols = np.indices(band_arr.shape)

        lons = left + cols * pixel_width + (pixel_width / 2)
        lats = top - rows * pixel_height - (pixel_height / 2)

        lons = lons.ravel()
        lats = lats.ravel()
        values = band_arr.ravel()

        df = pd.DataFrame({
            'longitude': lons,
            'latitude': lats,
            'value': values
        })

        df.to_csv(output_csv_path, index=False)


extract_pixel_coords('ndvi.tif','lmao.csv')