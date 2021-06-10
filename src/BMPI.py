# -*- coding: utf-8 -*-
"""
Bitcoin Mining Payout Inspector

@author: Mischa van Reede
"""



import logging 
from configparser import ConfigParser



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
            
        
        """
        
        # set up logging to file - see previous section for more details | TODO: add file and method to logging format: FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s" , https://stackoverflow.com/questions/10973362/python-logging-function-name-file-name-line-number-using-a-single-file

        # logging.basicConfig(level=logging.DEBUG,
        #                     format='%(asctime)s.%(msecs)d | %(name)s | %(module)s-%(lineno)8s | %(levelname)-8s | %(message)s',
        #                     datefmt='%m-%d %H:%M:%S',
        #                     filename=self.config['constants']['LOG_FILE_PATH'],
        #                     filemode='w')
        
        base_logger = logging.getLogger("BMPI")
        base_logger.setLevel(logging.DEBUG)
        
        # Set log format
        formatter = logging.Formatter('%(asctime)s.%(msecs)d | %(name)s | %(module)s-%(lineno)8s | %(levelname)-8s | %(message)s')
        formatter.datefmt = '%Y-%m-%d %H:%M:%S'
        
        # Clears existing handlers to prevent duplicating logs on consequent runs
        if (base_logger.hasHandlers()):
            base_logger.handlers.clear()
        
        # Create file handler, Obtains log file location from config (settings.conf)
        file_handler = logging.FileHandler(self.config['constants']['LOG_FILE_PATH'])
        file_handler.filemode = 'w'
        file_handler.setFormatter(formatter)
        
        # Create stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        
        # Add handlers to logger
        base_logger.addHandler(file_handler)
        base_logger.addHandler(stream_handler)
        
        # Stop logs from propagating to the root logger
        #base_logger.propagate = False
        
        return base_logger

    
    def run(self):
        self.logger.debug("Testing debug")
        self.logger.info("Testing info")
        self.logger.warning("Testing warning")
        self.logger.critical("Testing critical")   
    

if __name__ == '__main__':
    BMPI().run()