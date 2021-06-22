# -*- coding: utf-8 -*-
"""
Bitcoin Mining Payout Inspector

@author: Mischa van Reede
"""


# package imports
import logging 
from configparser import ConfigParser


# project imports
from apps.BMPI_functions import BMPIFunctions



class BMPI():
       
    def __init__(self):
        
        self.config = self.initialize_config()
        self.logger = self.initialize_logger()
            
    
    def initialize_config(self):
        config = ConfigParser()
        config.read("config/settings.conf")
        return config
    
    def initialize_logger(self): #For logging, look at: https://docs.python.org/3/howto/logging.html  and  https://docs.python.org/3/howto/logging.html#logging-advanced-tutorial
        """
        A method that initializes logging # https://docs.python.org/3/howto/logging-cookbook.html#logging-to-multiple-destinations
        https://www.youtube.com/watch?v=jxmzY9soFXg
        
        Levels:
            DEBUG:      Detailed information, typically of interest only when diagnosing problems.
            INFO:       Confirmation that things are working as expected.
            WARNING:    An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
            ERROR:  	Due to a more serious problem, the software has not been able to perform some function.
            CRITICAL:  	A serious error, indicating that the program itself may be unable to continue running.
            EXCEPTION:  Includes a traceback to mot recent method call
        
        Todo:
            add date to log file to prevent them growing too big
            perhaps implement log file 'rotation' 
        
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
        #formatter = logging.Formatter('%(asctime)s | %(name)s | %(module)4s-%(lineno)4s | %(levelname)-8s | %(message)s')
        formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)-8s | (%(filename)s:%(lineno)s) -- %(message)s')
        formatter.datefmt = '%Y-%m-%d %H:%M:%S'
        
        # Clears existing handlers to prevent duplicating logs on consequent runs
        if (bmpi_logger.hasHandlers()):
            bmpi_logger.handlers.clear()
        
        # Create file handler, Obtains log file location from config (settings.conf)
        file_path = self.config.get('Logging', 'LOG_FILE_PATH')
        file_handler = logging.FileHandler(file_path)
        file_handler.filemode = 'w'
        file_handler.setFormatter(formatter)
        
        # Create stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.ERROR)
        stream_handler.setFormatter(formatter)
        
        # Add handlers to logger
        bmpi_logger.addHandler(file_handler)
        bmpi_logger.addHandler(stream_handler)
        
        # Stop logs from propagating to the root logger
        #base_logger.propagate = False
        
        return bmpi_logger

    
    def run(self):
        
        BMPI = BMPIFunctions(config=self.config, logger=self.logger)
        
        BMPI.store_last_n_blocks_in_es(5)
    

if __name__ == '__main__':
    BMPI().run()