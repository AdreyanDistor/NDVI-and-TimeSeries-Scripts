import logging

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the logging level

# Create a console handler and set level to debug
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

# def some_function():
#     logger.debug('This is a debug message from module1')
#     logger.info('This is an info message from module1')
#     logger.warning('This is a warning message from module1')
#     logger.error('This is an error message from module1')
#     logger.critical('This is a critical message from module1')
