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
from apps.utils import Utils


   


def initialize_config():
    
    cwd = os.path.abspath(os.path.dirname(sys.argv[0]))
    config_path = cwd + "/config/settings.conf"
    
    assert os.path.exists(config_path)
    config = ConfigParser()
    config.read(config_path)
    return config

def initialize_logger(config, DEBUG): #For logging, look at: https://docs.python.org/3/howto/logging.html  and  https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial
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
    
    if DEBUG:
        bmpi_logger.setLevel(logging.DEBUG)
    else:
        bmpi_logger.setLevel(logging.INFO)
    # Set log format
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)-8s | (%(filename)s:%(lineno)s) -- %(message)s')
    formatter.datefmt = '%Y-%m-%d %H:%M:%S'
    
    # Clears existing handlers to prevent duplicating logs on consequent runs
    if (bmpi_logger.hasHandlers()):
        bmpi_logger.handlers.clear()
    
    # Generic log file: Create file handler, Obtains log file location from config (settings.conf)
    file_path = config.get('Logging', 'LOG_FILE_PATH')   
    timed_rotating_file_handler = TimedRotatingFileHandler(filename=file_path, when='h', interval=1, backupCount=50, encoding='utf-8')
    
    if DEBUG:
        timed_rotating_file_handler.setLevel(logging.DEBUG)
    else:
        timed_rotating_file_handler.setLevel(logging.INFO)
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
@click.option('--stop_height', default=0, show_default=True, type=int, help='height at which the run is stopped, block at entered height is not stored.')
@click.option('--start_hash', default=None, show_default=True, type=str, help='hash of the block to start the run.')
@click.option('--blocks_stored', default=0, show_default=True, type=int, help='# of blocks stored during previous runs.')
@click.option('--blocks_skipped', default=0, show_default=True, type=int, help='# of blocks skipped on previour runs.')
@click.option('--api_conflicts', default=0, show_default=True, type=int, help='# of api_conflics found during previous runs.')
def gather_scraper_data(start_height, stop_height, start_hash, blocks_stored, blocks_skipped, api_conflicts):
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
                                              stop_height=stop_height,
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
@click.option('--index', default=None, help='Name of index from which to remove the duplicates.')
def remove_duplicates(index):
    '''
    Removes duplicates from the index based on the block_height
    of the stored documents. Keeps one record stored.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    #index = 'skipped_blocks'
    
    try:
        print("Deleting duplicates from the index: {}".format(index))
        BMPI.remove_duplicates(index)
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
        BMPI.gatherSpecificBlock(block_height=block_height, block_hash=block_hash, store_block=True)
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
        BMPI.deleteStoredBlocksFromElasticsearch(index=index, start_height=start_height, end_height=end_height, should_delete=should_delete)
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

@cli.command()
def combine_known_pool_data():
    '''
    Fills mining pool data file "combined_data.json" with known mining tags. 
    Uses tag information from three sources:
        - blockchain.com's known-pools file
        - btc.com's known-pools file
        - known-pools file from 0xB10C (BTC core developer)
        - known-pools file from Sjors Provoost (BTC core developer)
    '''
    try:
        print("Combining known-pools data files.")
        Utils.combineKnownPoolDataFiles(logger=logger)
        print("Done.")
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)

@cli.command()
@click.option('--run_id', default=None, type=str, help='Enter a run_id (str), by default it uses current date and time.')
@click.option('--update_pool_data', is_flag=True, help='Use this flag to update the pools.json with pool payout addresses during analysis.')
@click.option('--start_height', type=int, help='Start height of block to stored block to be analysed.')
@click.option('--end_height', type=int, help='Height of the last block to be analysed.')
def attribute_pool_names(run_id, update_pool_data, start_height, end_height):
    '''
    Reads block data from the blocks_from_scrapers_updated index,
    calls the attribute_blocks module to determine pool name for each block,
    writes results to the block_attributions index.
    '''
    assert(start_height <= end_height)
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        print("Attributing pool names to stored blocks.")
        print("Storing results in seperate index")
        # Stored on server: 0 / 700000
        # Stored at home: 694996 / 699329
        
        BMPI.attributePoolNames(run_id=run_id, 
                                update_my_pool_data=update_pool_data, 
                                start_height=start_height, 
                                end_height=end_height)
        print("Done.")
        
    except Exception as e:
        print("An error occured:")
        print("Error message: {}".format(str(e)))
        sys.exit(1)
        
@cli.command()       
def gather_and_store_skipped_blocks():
    '''
    Try to re-gathers skipped blocks
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        print("Re-gathering skipped blocks.")
        BMPI.gatherMissingBlocksFromScrapers()
        print("Done.")
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)


@cli.command()       
def update_pools_data_json():
    '''
    Loop over gathered blocks to attribute a pool name, update pools.json
    if a new payout address if found for a mining pool.
    Does not store any results in Elasticsearch.
    '''
    BMPI = BMPIFunctions(config=config, logger=logger)
    try:
        print("Start updating pools.json")
        BMPI.updatePoolDataWithPayoutAddressData()
        print("Done.")
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)

@cli.command()    
def test_key_to_address():
    '''
    Test code to transform a public key to a bitcoin address.
    
    Examples:
    pub key 04:
    https://blockstream.info/api/tx/d757ef4fe66da70eb56c20119fa1d221595e43ebebca7712fb741e8dd4ba792b
    https://blockchain.info/rawblock/000000000010cbb69ef733f178bcb186d5ca37cd89f887cb87f27874f16d56b6
    pub key = 04ece4b58a8a0cdc08285110db223a76c6cc0bd409c440b813bff0a0575491c8cc9d4a89459e0d6a3dfb0b88a7878b1a83aea0ba8331d5325fe381125ee8d587dc
    
    pub key 03:
    https://blockchain.info/rawblock/0000000000000001a2ef84f5945f525963ad672bc48a95ea2bf87cea2a1c6117
    https://blockchain.info/rawblock/0000000000000001a2ef84f5945f525963ad672bc48a95ea2bf87cea2a1c6117
    pub_key = 03a05203af00cc042052c88a2b80ddabcacce7c5d4bc4fabf887505db3e4ff7001
    address = 12y2z1dv2q96fnEHUWhnyc5Sjb32p2LPTi
    
    '''
    try:
        print("Transforming pub keys to addresses\n")
        
        pub_key_04 = '04ece4b58a8a0cdc08285110db223a76c6cc0bd409c440b813bff0a0575491c8cc9d4a89459e0d6a3dfb0b88a7878b1a83aea0ba8331d5325fe381125ee8d587dc'
        pub_key_03 = '03a05203af00cc042052c88a2b80ddabcacce7c5d4bc4fabf887505db3e4ff7001'
        pub_key_02 = '022af168cba6edf073a68f91a645d0b4833d549d15377a94472acb8bb58a6a26cd'
        
        print("Transforming a pub key with prefix '04'")
        print("Pub key: {}".format(pub_key_04))
        print(Utils.bitcoin_address_from_pub_key(pub_key=pub_key_04, logger=logger))
        print()
        
        print("Transforming a pub key with prefix '03'")
        print("Pub key: {}".format(pub_key_03))
        print(Utils.bitcoin_address_from_pub_key(pub_key=pub_key_03, logger=logger))
        print()
       
        print("Transforming a pub key with prefix '02'")
        print("Pub key: {}".format(pub_key_02))
        print(Utils.bitcoin_address_from_pub_key(pub_key=pub_key_02, logger=logger))
        print()
        
        
        print("Done.")
    except Exception as ex:
        print("An error occured:")
        print("Error message: {}".format(str(ex)))
        sys.exit(1)
        

if __name__ == '__main__':
    
    config = initialize_config()
    DEBUG = config.getboolean('Constants', 'DEBUG')
    print("Debug is set to: {}".format(DEBUG))
    
    logger = initialize_logger(config, DEBUG)
    
    cli()