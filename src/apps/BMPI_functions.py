# -*- coding: utf-8 -*-

"""


@author: Mischa van Reede
"""

import json

from .elastic import ElasticsearchController
from .API_scrapers.blockchain_scraper import BlockchainScraper



class BMPIFunctions():
    
    
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        self.es_controller = None
        self.blockchain_scraper = None

    def initialize_modules(self):
        
        self.es_controller = ElasticsearchController(config=self.config, logger=self.logger)
        
        self.blockchain_scraper = BlockchainScraper(config=self.config, logger=self.logger)
        
        
    def store_most_recent_block_in_es(self):
        
        self.initialize_modules()
        
        block = self.blockchain_scraper.getLatestBlock()
        #remove tx for now
        
        block.pop('tx')
        
        #self.logger.info("Block/record type: [{}]".format(type(block)))
        block_hash = block['hash']
        
        #store record
        
        self.logger.info("Block [{}] at height [{}] stored succesfully".format(block_hash, block['height']))
        self.es_controller.store(record=block, index_name="first_index")
        
        #query record
        self.logger.info("Quering block with hash [{}] from es instance".format(block_hash))
        
        match = self.es_controller.query_es(index="first_index", query=block_hash)
        
        print("Obtained most recent block from ES instance")
        
        print(json.dumps(match.to_dict(), indent=2))

        
