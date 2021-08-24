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
@click.option('--start_height', default=None, show_default=True, type=int, help='height of the block to start the run.')
@click.option('--start_hash', default=None, show_default=True, type=str, help='hash of the block to start the run.')
@click.option('--blocks_stored', default=0, show_default=True, type=int, help='# of blocks stored during previous runs.')
@click.option('--blocks_skipped', default=0, show_default=True, type=int, help='# of blocks skipped on previour runs.')
@click.option('--api_conflicts', default=0, show_default=True, type=int, help='# of api_conflics found during previous runs.')
def gather_scraper_data(start_height, start_hash, blocks_stored, blocks_skipped, api_conflicts):
    """
    Gathers block data from implemented scrapers and store it in elasticsearch. 
    If using parameters; make sure start_hash and start_height are from the 
    same block and belong together, this is not checked during runtime.  
    
    """
    if (start_height is None) ^ (start_hash is None):
        logger.error('Please specify both arguments to start at a specific block.')
        logger.error('You can also omit both if you want to start at the latest block.')
        sys.exit(1)
    
    if start_height is not None and start_hash is not None:
        logger.info('Starting the run at a specified hash and height.')
        logger.info('Height: {},  Hash: {}'.format(start_height, start_hash))
    else:
        logger.info('Starting the run at the latest block height and hash.')
    
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        BMPI.gatherAndStoreBlocksFromScrapers(start_height=start_height,
                                              start_hash=start_hash,
                                              blocks_stored=blocks_stored,
                                              blocks_skipped=blocks_skipped,
                                              api_conflicts=api_conflicts)
    except Exception as ex:
        logger.exception("An exception occured during runtime: {}".format(str(ex)))
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        
        
#https://stackoverflow.com/questions/67297248/noninteractive-confirmation-of-eager-options-in-the-python-click-library
@cli.command()
@click.confirmation_option(prompt='Are you sure you want to delete all data from the elasticsearch instance?')
def delete_all_stored_data():
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
        
        
#https://stackoverflow.com/questions/67297248/noninteractive-confirmation-of-eager-options-in-the-python-click-library
@cli.command()
@click.option('--index', default=None, required=True, type=str, help='Deletes the data stored in the given index from elasticsearch instance.')
@click.confirmation_option(prompt='Are you sure you want to delete the data from this index?')
def delete_stored_data(index):
    '''
    Deletes the data from a specified index in elasticsearch.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        print("Deleting data from index {}.".format(index))
        BMPI.removeStoredElasticsearchData(index)
        print("Data deleted.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        
@cli.command()
@click.option('--index', default=None, show_default=True, type=str, help='The index in which the document is stored.')
@click.option('--doc_id', default=None, show_default=True, type=str, help='The id of the document.')        
def delete_doc_by_id(index, doc_id):
    '''
    Deletes a document from an index specified by its document id.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        print("Deleting document {} from index {}".format(doc_id, index))
        BMPI.deleteDocByID(index, doc_id)
        print("Done.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        
@cli.command()
def remove_duplicate_api_conflicts():
    '''
    Removes duplicates from the API conflicts index based on the block_height
    of the stored documents. Keeps one record stored.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        print("Deleting duplicates from the api_conflicts index")
        BMPI.remove_duplicate_api_conflicts()
        print("Done.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


@cli.command()
@click.option('--block_height', default=None, show_default=True, type=int, help='height of the block to gather and store.')
@click.option('--block_hash', default=None, show_default=True, type=str, help='hash of the block at specified height. Correctness of hash is not checked.')
def store_block(block_height, block_hash):
    '''
    Gathers and stores a specified block in Elasticsearch
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        print("Trying to gather and store block: {}  /  {}".format(block_height, block_hash))
        BMPI.gatherAndStoreSpecificBlock(block_height=block_height, block_hash=block_hash)
        print("Done.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


@cli.command()
@click.option('--start_height', default=0, show_default=True, type=int, help='Starting height of the blocks that needs to be deleted.')
@click.option('--end_height', default=0, show_default=True, type=int, help='End height of the last block that needs to be deleted.')
@click.option('--should_delete', is_flag=True)
def delete_stored_blocks_between_heights(start_height, end_height, should_delete):
    '''
    Deletes stored blocks from index "blocks_from_scrapers" between the specified heights. 
    The parameter start_height should be smaller then end_height.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    
    try:
        assert(start_height<=end_height)
        index = "blocks_from_scrapers"
        print("Deleting stored blocks between heights {} and {}".format(start_height, end_height))
        BMPI.deleteStoredBlocksFromElasticsearch(index=index start_height=start_height, end_height=end_height, should_delete=should_delete)
        print("Done.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
    


@cli.command()
def print_scrapers():
    '''
    Print the string representation of the implemented scrapers.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        print("Printing scrapers command issued.")
        BMPI.printScrapers()
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


@cli.command()
@click.confirmation_option(prompt='Are you sure you want to reindex the data to a new index?')
def reindex_blocks():
    '''
    Moving block data to new block index.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        print("Moving block data to updated index.")
        BMPI.reindexBlocksFromScraperData()
        print("Done.")
        
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


def combine_known_pool_data():
    '''
    Fills mining pool data file "combined_data.json" with known mining tags. 
    Uses tag information from three sources:
        - blockchain.com's known-pools file
        - btc.com's known-pools file
        - known-pools file from Sjors Provoost (BTC core developer)
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        pass
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        

if __name__ == '__main__':
    
    config = initialize_config()
    logger = initialize_logger(config)
    
    cli()