# NDVI-and-TimeSeries-Scripts
## Prequisites
- Anaconda
- Python 3

# Installation
## 1. Clone the github repository
    https://github.com/AdreyanDistor/NDVI-and-TimeSeries-Scripts.git

## 2. Run these commands
```
conda install -c conda-forge gdal 
pip install -r requirements.txt  
```
The first line install gdal, the second installs the other libraries
```
    rasterio
    pyproj
    geopandas
    rioxarray
    numpy
    pandas
    shapely
```
# How To Run Scripts
## process_ndvi.py
Used for converting satellite images into compressed ndvi images
```
options:
Convert satellite images to NDVI

options:
  -h, --help            show this help message and exit
  -i input_directory, --input input_directory
                        Enter input directory of dataset
  -o output_directory, --output output_directory
                        Enter output directory for NDVI images
  -q, --quiet           Turns off Messages until WARNING LEVEL
```
*Note*: Make sure the input directory is the one that contains the folders in YYYY-MM-DD format, as this is how it searches for ndvi images.

## timeseries.py
Used for converting satellite images into compressed ndvi images
```
options:
  -h, --help            show this help message and exit

  -i input_directory, --input input_directory
                        Input directory of NDVI images
                        
  -p latitude longitude, --point latitude longitude
                        Latitude and Longitude for point time series

  -w wkt_file, --wkt wkt_file
                        Path to WKT file for range time series

  -s start_date, --start start_date
                        Start date in YYYY-MM-DD format
                        
  -e end_date, --end end_date
                        End date in YYYY-MM-DD format

  -q, --quiet           Turns off Messages until WARNING LEVEL
```
*Note*: Make sure the input directory is the one that contains the NDVI images



