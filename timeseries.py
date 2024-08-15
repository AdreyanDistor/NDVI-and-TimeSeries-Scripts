import sys
import os
import argparse
import datetime as date
import logging

# Add directories to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import functions
from bounding_box_functions import *
from ndvi_extraction_functions import *
from ndvi_image_functions import *
from time_series_functions import *
from wkt_functions import *
from log_config import logger, console_handler
def handle_point_timeseries(lat, lon, start_date, end_date, ndvi_dir):
    try:
        time_series_point = ndvi_timeseries_point(lat, lon, start_date, end_date, ndvi_dir)
        file_name = f"{start_date.date()}_to_{end_date.date()}_at_Longitude_{lon}_and_Latitude_{lat}.csv"
        time_series_point.to_csv(file_name, index=False)
        logger.info(f"Point time series saved to {file_name}")
        logger.debug(f"Time series data: {time_series_point}")
    except Exception as e:
        logger.error(f"Error processing point time series: {e}")

def handle_range_timeseries(wkt, start_date, end_date, ndvi_dir):
    try:
        time_series_range = ndvi_timeseries_range(wkt, start_date, end_date, ndvi_dir)
        mbr = wkt_to_bounds(wkt)
        file_name = f"{start_date.date()}_to_{end_date.date()}_at_{mbr}.csv"
        time_series_range.to_csv(file_name, index=False)
        logger.info(f"Range time series saved to {file_name}")
        logger.debug(f"Time series data: {time_series_range}")
    except Exception as e:
        logger.error(f"Error processing range time series: {e}")

def main():
    parser = argparse.ArgumentParser(description='NDVI Image and Time Series Processing')
    parser.add_argument('-i', '--input', metavar='input_directory', type=str, required=True, help='Input directory of NDVI images')
    parser.add_argument('-p', '--point', nargs=2, metavar=('latitude', 'longitude'), type=float, help='Latitude and Longitude for point time series')
    parser.add_argument('-w', '--wkt', metavar='wkt_file', type=str, help='Path to WKT file for range time series')
    parser.add_argument('-s', '--start', metavar='start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('-e', '--end', metavar='end_date', type=str, required=True, help='End date in YYYY-MM-DD format')
    parser.add_argument('-q', '--quiet', action='store_true', help='Turns off Messages until WARNING LEVEL')
    args = parser.parse_args()

    ndvi_dir = args.input
    start_date = date.datetime.strptime(args.start, '%Y-%m-%d')
    end_date = date.datetime.strptime(args.end, '%Y-%m-%d')

    quiet = args.quiet
    if quiet:
        console_handler.setLevel(logging.ERROR)
    else:
        console_handler.setLevel(logging.DEBUG)
    
    if args.point:
        latitude, longitude = args.point
        handle_point_timeseries(latitude, longitude, start_date, end_date, ndvi_dir)

    if args.wkt:
        wkt_path = args.wkt
        if os.path.isfile(wkt_path):
            with open(wkt_path, 'r') as file:
                wkt_string = file.read()
            handle_range_timeseries(wkt_string, start_date, end_date, ndvi_dir)
        else:
            logger.warning(f"The WKT file {wkt_path} does not exist.")
    
    
        

if __name__ == '__main__':
    main()
