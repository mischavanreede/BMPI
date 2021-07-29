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


- TODO check if coinbase transaction has multiple output addresses
- TODO implement try-catch blocks to catch timeout errors

- make a 'mismatch index' in es which keeps track of mismatches in block information from the two API's'

"""


# package imports
import logging
from configparser import ConfigParser
import click

# project imports
from apps.BMPI_functions import BMPIFunctions


   


def initialize_config():
    config = ConfigParser()
    config.read("config/settings.conf")
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

    TODO:   - Add date to log file to prevent them growing too big
            - Perhaps implement log file 'rotation'
            - Put and obtain settings from loggin.conf
            https://stackoverflow.com/questions/15727420/using-logging-in-multiple-modules
    """
    
    # set up logging to file - see previous section for more details
    # logging.basicConfig(level=logging.DEBUG,
    #                     format='%(asctime)s.%(msecs)d | %(name)s | %(module)s-%(lineno)8s | %(levelname)-8s | %(message)s',
    #                     datefmt='%m-%d %H:%M:%S',
    #                     filename=self.config['constants']['LOG_FILE_PATH'],
    #                     filemode='w')
    
    bmpi_logger = logging.getLogger("BMPI")
    bmpi_logger.setLevel(logging.DEBUG)
    
    # Set log format
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)-8s | (%(filename)s:%(lineno)s) -- %(message)s')
    formatter.datefmt = '%Y-%m-%d %H:%M:%S'
    
    # Clears existing handlers to prevent duplicating logs on consequent runs
    if (bmpi_logger.hasHandlers()):
        bmpi_logger.handlers.clear()
    
    # Create file handler, Obtains log file location from config (settings.conf)
    file_path = config.get('Logging', 'LOG_FILE_PATH')
    file_handler = logging.FileHandler(filename=file_path, mode='a',encoding='utf-8')
    file_handler.filemode = 'w'
    file_handler.setFormatter(formatter)
    
    # Create stream handler for console output
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    
    # Add handlers to logger
    bmpi_logger.addHandler(file_handler)
    bmpi_logger.addHandler(stream_handler)
    
    # Stop logs from propagating to the root logger
    #bmpi_logger.propagate = False
    
    return bmpi_logger


# @click.group()
def bmpi(config, logger):
    """
    This will be executed on every application call.

    """
    BMPI = BMPIFunctions(config=config, logger=logger)
    BMPI.gatherAndStoreBlocksFromScrapers()


#https://stackoverflow.com/questions/67297248/noninteractive-confirmation-of-eager-options-in-the-python-click-library
# @bmpi.command()
# @click.option()
# @click.confirmation_option()
def deleteStoredData():
    '''
    

    Returns
    -------
    None.

    '''
    pass


def gatherApiBlockData():
    '''
    Gathers block data from API scrapers and stores data in elasticsearch.
    '''
    pass



def run(self):
    pass
    #BMPI = BMPIFunctions(config=self.config, logger=self.logger)
    #BMPI.runScrapers()
    #BMPI.run()
    




if __name__ == '__main__':
    config = initialize_config()
    logger = initialize_logger(config)
    
    bmpi(config, logger)