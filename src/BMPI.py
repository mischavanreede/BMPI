#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bitcoin Mining Payout Inspector

@author: Mischa van Reede

x Disconnected two scrapers
x get mining pool btc address from both scrapers 
x update pool_data.json (Add f2pool message to pool_data.json, etc.)
x add binance pool
x check for message match, check for btc address match
x add btc address if missing
- save block data for manual inspection if btc address match, but no message match; So that this message can be added manually.
- Think about settings for blocks index (with variables such as; height, hash, prev hash, pool_name, mining pool btc address)
- Think about settings for mining pool index (name, number of blocks, latest mined block, receiving btc address, sub-mining addresses, share of rewards per mining address) 
- (make abstract scraper class: https://www.youtube.com/watch?v=PDMe3wgAsWg)

x fix logging errors when trying to log the fish symbol and other non ascii symbols.
x update pool_data.json to allow for multiple coinbase tags for the same mining pool (e.g. 'Mined by AntPool' and 'Mined By AntPool' and '/AntPool/') 



"""


# package imports
import logging
from logging.handlers import TimedRotatingFileHandler

from configparser import ConfigParser
import click
import os
import sys

# project imports
from apps.BMPI_functions import BMPIFunctions


   


def initialize_config():
    
    cwd = os.path.abspath(os.path.dirname(sys.argv[0]))
    config_path = cwd + "/config/settings.conf"
    
    assert os.path.exists(config_path)
    config = ConfigParser()
    config.read(config_path)
    return config

def initialize_logger(config): #For logging, look at: https://docs.python.org/3/howto/logging.html  and  https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial
    """
    A method that initializes logging
    # https://docs.python.org/3/howto/logging-cookbook.html#logging-to-multiple-destinations
    https://www.youtube.com/watch?v=jxmzY9soFXg

    Levels:
        DEBUG:      Detailed information, typically of interest only when diagnosing problems.
        INFO:       Confirmation that things are working as expected.
        WARNING:    An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
        ERROR:  	Due to a more serious problem, the software has not been able to perform some function.
        CRITICAL:  	A serious error, indicating that the program itself may be unable to continue running.
        EXCEPTION:  Includes a traceback to mot recent method call
    """
    
   
    bmpi_logger = logging.getLogger("BMPI")
    bmpi_logger.setLevel(logging.DEBUG)
    
    # Set log format
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)-8s | (%(filename)s:%(lineno)s) -- %(message)s')
    formatter.datefmt = '%Y-%m-%d %H:%M:%S'
    
    # Clears existing handlers to prevent duplicating logs on consequent runs
    if (bmpi_logger.hasHandlers()):
        bmpi_logger.handlers.clear()
    
    # Generic log file: Create file handler, Obtains log file location from config (settings.conf)
    file_path = config.get('Logging', 'LOG_FILE_PATH')   
    timed_rotating_file_handler = TimedRotatingFileHandler(filename=file_path, when='h', interval=6, backupCount=50, encoding='utf-8')
    timed_rotating_file_handler.setLevel(logging.DEBUG)
    timed_rotating_file_handler.setFormatter(formatter)
    
    # Error log file:
    error_log_file_path = config.get('Logging', 'ERROR_LOG_FILE_PATH')
    error_handler = logging.FileHandler(filename=error_log_file_path, mode='a', encoding='utf-8')
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(formatter)
     
    
    # Console log: Create stream handler for console output
    # stream_handler = logging.StreamHandler()
    # stream_handler.setLevel(logging.INFO)
    # stream_handler.setFormatter(formatter)
    
    # Add handlers to logger
    bmpi_logger.addHandler(timed_rotating_file_handler)
    bmpi_logger.addHandler(error_handler)
    # bmpi_logger.addHandler(stream_handler)
    
    # Stop logs from propagating to the root logger
    #bmpi_logger.propagate = False
    
    return bmpi_logger


@click.group()
def cli():
    """
    Please select a command to run.

    """
    pass
    

@cli.command()
def gather_scraper_data():
    """
    Gathers block data from implemented scrapers and store it in elasticsearch. 
    """
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        BMPI.gatherAndStoreBlocksFromScrapers()
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        
        
#https://stackoverflow.com/questions/67297248/noninteractive-confirmation-of-eager-options-in-the-python-click-library
@cli.command()
@click.confirmation_option(prompt='Are you sure you want to delete all data from the elasticsearch instance?')
# @click.confirmation_option()
def delete_stored_data():
    '''
    Deletes the data from a specified index in elasticsearch. If 
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        print("Deleting all data from ES.")
        BMPI.removeALLStoredElasticsearchData()
        print("All data deleted.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


if __name__ == '__main__':
    
    config = initialize_config()
    logger = initialize_logger(config)
    
    cli()