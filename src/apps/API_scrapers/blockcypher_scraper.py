# -*- coding: utf-8 -*-
"""
A scraper module used to query data from the Blockcypher.com API.

https://www.blockcypher.com/quickstart/


@author: Mischa van Reede

NOTE: The current plan with 2000 daily requests will probably be not enough to query the entire blockchain.
If we take 2 queries per block it would take 690000 / 2000 / 2  = 690 days

https://www.smartbit.com.au/api
"""

from .generic_rest_requests import RestRequests

class BlockcypherScraper(RestRequests):
    
    def __init__(self, config, logger):
        
        self.config = config
        self.logger = logger
        self.base_url = ""
        self.api_token = "5d4331ca8409401fb8425918af44d164"
        self.daily_requests = 2000 # free plan has 2000 requests

        # Initialize RestRequest object from parent class
        super().__init__(config=self.config, logger=self.logger)
        
# ====================================   
#  API query methods 
# ====================================
        
    def getLatestBlock(self):
        pass
    
    def getBlock(self, block_hash):
        pass    
    
    def getTransaction(self, transaction_hash):
        pass        
    
    def getBlocksAtHeight(self, block_height):
        pass
    
    def getAddressInfo(self, bitcoin_address):
        pass
    
# ====================================   
#  Data handler methods
# ====================================

    def getLatestBlockHeight(self):
        pass
  
    def getLatestBlockHash(self):
        pass

    def getCoinbaseTransaction(self, block):
        pass
    
    def extractPrevBlockHash(self, block):
        pass
    
    def extractBlockMessage(self, block):
        pass

    def pruneUserTransactionsFromBlock(self, block):
        pass