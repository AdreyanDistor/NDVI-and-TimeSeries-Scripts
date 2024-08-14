import sys
import os
import argparse
import logging

# Add directories to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src/old_archive'))

# Import functions
from ndvi_image_functions import *
from log_config import logger, console_handler

def main():
    parser = argparse.ArgumentParser(description='Convert satellite images to NDVI')
    parser.add_argument('-i', '--input', metavar='input_directory', type=str, required=True, help='Enter input directory of dataset')
    parser.add_argument('-o', '--output', metavar='output_directory', type=str, required=True, help='Enter output directory for NDVI images')
    parser.add_argument('-q', '--quiet', action='store_true', help='Turns off Messages until WARNING LEVEL')

    args = parser.parse_args()

    input_directory = args.input
    output_directory = args.output
    quiet = args.quiet

    if quiet:
        console_handler.setLevel(logging.ERROR)
    else:
        console_handler.setLevel(logging.DEBUG)
    
    logger.addHandler(console_handler)

    cpu_count = os.cpu_count()
    dir_queue = initialize_queue(input_directory)
    threads = create_and_start_threads(input_directory, output_directory, dir_queue, num_threads=cpu_count, quality='60')
    wait_for_threads_to_complete(threads)

if __name__ == '__main__':
    main()
