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
        self.__initialize_modules()

    def __initialize_modules(self):
        
        
        self.es_controller = ElasticsearchController(config=self.config, logger=self.logger)
        
        self.blockchain_scraper = BlockchainScraper(config=self.config, logger=self.logger)
        
        
    def store_most_recent_block_in_es(self):
        
        # Make sure the controller is connected to a running ES instance
        assert(self.es_controller.isConnected())
        
        
        
        block = self.blockchain_scraper.getLatestBlock()
        #remove tx for now
        
        block.pop('tx')
        
        #self.logger.info("Block/record type: [{}]".format(type(block)))
        block_hash = block['hash']
        
        #store record
        
        self.es_controller.store(record=block, index_name="first_index")
        self.logger.info("Block [{}] at height [{}] stored succesfully".format(block_hash, block['height']))
        
        #query data record
        self.logger.info("Quering block with hash [{}] from es instance".format(block_hash))
        result = self.es_controller.query_es(index="first_index", query=block_hash)
        self.logger.info("Got the following result: [{}]".format(result))
        

        print("done")
        

    def store_last_n_blocks_in_es(self, n=1):
        assert (n>0) and self.es_controller.isConnected()
        
        
        blocks = []
        i = n
        latest_block_height = self.blockchain_scraper.getLatestBlockHeight()
        latest_blocks = self.blockchain_scraper.getBlocksAtHeight(block_height=latest_block_height)
        self.logger.debug("Gathering the last {} block(s), starting from height {}".format(n, latest_block_height))
        if len(latest_blocks) != 1:
            # TODO: implement this scenario
            print("Found multiple blocks at current height: {}".format(latest_block_height))
            print("Exiting because this scenario is not coded yet.")
            return
        else:    
            latest_block = latest_blocks[0]
            latest_block = self.blockchain_scraper.pruneUserTransactionsFromBlock(latest_block)
            latest_block['mining_pool_message'] = self.blockchain_scraper.extractBlockMessage(latest_block)
            prev_block_hash = self.blockchain_scraper.extractPrevBlockHash(latest_block)
            blocks.append(latest_block)
            i-=1
            
            while i>0:
                prev_block = self.blockchain_scraper.getBlock(prev_block_hash)    
                prev_block = self.blockchain_scraper.pruneUserTransactionsFromBlock(prev_block)
                prev_block['mining_pool_message'] = self.blockchain_scraper.extractBlockMessage(prev_block)
                prev_block_hash = self.blockchain_scraper.extractPrevBlockHash(prev_block)
                blocks.append(prev_block)
                i-=1
        
        self.logger.info("finished gathering last n={} blocks".format(n))
          # store blocks
        self.es_controller.bulk_store(records=blocks, index_name='blocks')
        print("done")
        return