# -*- coding: utf-8 -*-

"""


@author: Mischa van Reede
"""

import json
import time
import click

from .elastic import ElasticsearchController
from .API_scrapers.scraper_controller import ScraperController
from .API_scrapers.blockchain_scraper import BlockchainScraper
from .API_scrapers.btc_scraper import BtcScraper
from .utils import Utils



class BMPIFunctions():
    
    
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config

        self.__initialize_modules()

    def __initialize_modules(self):
        
        
        #self.es_controller = ElasticsearchController(config=self.config, logger=self.logger)
        
        self.scraper_controller = ScraperController(config=self.config, logger=self.logger)

        


    
    
    
    def removeStoredElasticsearchData(self):
        pass
    
    
    def gatherAndStoreMissingBlocksFromScrapers(self):
        pass
    
    def gatherAndStoreBlocksFromScrapers(self):
        
        latest_block_hash, latest_height = self.scraper_controller.getLatestBlockHashAndHeight()
        
        block_height = latest_height
        previous_block_hash = latest_block_hash
        
        block_list = []
        API_conflicts = []
        
        while block_height >= 0:
            # on intervals where 1000 blocks have been gathered, 
                # store block_list and API_conflicts in Elasticsearch
            if block_height % 1000 == 0:
                # Store succesfully gathered blocks
                if self.es_controller.bulk_store(records=block_list, index="blocks_from_scrapers"):
                    # bulk store succesfull, emptying list
                    block_list = []
                
                # Store API data conflicts
                if self.es_controller.bulk_store(records=API_conflicts, index="CONFLICT_API_block_data_conflicts"):
                    #bulk store succesfull, emptying list
                    API_conflicts = []
           
            try:
                result = self.scraper_controller.getBlockInfoFromScrapers(previous_block_hash)
                
                if result['status'] == "success":
                    block = result['block']
                    # Add block to list
                    block_list.append(block)
                    # Decrease block_height by 1 and set previous hash
                    previous_block_hash = block['prev_block_hash']
                    block_height -= 1
                
                elif result['status'] == "conflict":
                    result['conflict_entry']['block_height'] = block_height
                    # Add conflict block to list
                    API_conflicts.append(result['conflict_entry'])
                    # Add skipped block to es
                    es_entry = {
                        "block_height": block_height,
                        "block_hash": previous_block_hash
                        }
                    self.es_controller.store(record=es_entry, index="skipped_blocks")
                    
                    # Set variables for next iteration of outer while
                    if result['prev_hash_equal']:
                        previous_block_hash = result['prev_hash']
                        # manually decrease block_height 
                        block_height -= 1
     
                    else:
                        # Keep decreasing until a matching previous hash is found
                        while True:
                            # if prev_hash is not the same, get that the hash at that height from the scrapers
                            previous_height = block_height - 1
                            previous_hash = self.scraper_controller.getBlockHashAtHeight(previous_height)
                            #TODO: Maybe add reason for skipping in the skipped_blocks index.
                            if previous_hash is None:
                                es_entry = {
                                    "block_height": previous_height,
                                    "block_hash": None
                                    }
                                self.es_controller.store(record=es_entry, index="skipped_blocks")
                            
                            if previous_hash is not None:
                                # set height and prev_hash for following itteration of outer while
                                block_height = previous_height
                                previous_block_hash = previous_hash
                                break
  
            except:
                # catch timeout errors and other exceptions
                continue
            
            else: 
                # No exception occurred
                pass
            
            finally:
                pass
                #code that is run after each try
        
                
        
        

    
    def attributePoolNames(self):
        '''
        add click parameter to choose which data to use(API scaper data or node data)
        possible also which pool_data.json to use
        store in seperate indices
        '''
        pass
    
    
    
    
    def runScrapers(self):
        self.scraper_controller.getLastBlocks(n=25)
        print("Done.")
    
    @Utils.printTiming
    def run(self):
        file =  open(file='../pools/pool_data.json', mode='r', encoding='utf-8')
        combined_pool_data_json = json.load(file)
        file.close()
        
        
    
        
        
        
    