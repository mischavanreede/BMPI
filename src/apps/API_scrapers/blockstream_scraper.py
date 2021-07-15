# -*- coding: utf-8 -*-
"""
A scraper module used to query data from the blockstream.com API through the
use of the bloxplorer library.

Documentation
    https://blockstream.info/
    https://pypi.org/project/bloxplorer/
    https://valinsky.me/bloxplorer/

@author: Mischa van Reede

Improvement idea: to avoid duplicate API calls, extract neccesary information in one method 
"""

from bloxplorer import bitcoin_explorer as explorer

from .generic_rest_requests import RestRequests
from ..utils import Utils



class BlockstreamScraper(RestRequests):
    
    def __init__(self, config, logger):
        
        self.config = config
        self.logger = logger

        
        # self.base_url = ""
        # self.api_key = None
        # Initialize RestRequest object from parent class
        # super().__init__(config=self.config, logger=self.logger)
        
    def __str__(self):
        return "Blockstream.info API scraper"
    
    def __repr__(self):
        return "Blockstream.info API scraper"
        
# ====================================   
#  API query methods 
# ====================================
        
    def getLatestBlock(self):
        latest_hash = explorer.blocks.get_last_hash()
        latest_block = explorer.get(latest_hash)
        return latest_block.data       
    
    def getLatestBlockHeight(self):
        latest_height = explorer.blocks.get_last_height()
        return int(latest_height.data)
  
    def getLatestBlockHash(self):
        latest_hash = explorer.blocks.get_last_hash()
        return latest_hash.data
        
    def getBlock(self, block_hash):
        result = explorer.blocks.get(block_hash)
        return result.data
    
    def getBlockTransactions(self, block_hash, page=0):
        result = explorer.blocks.get(block_hash)
        return result.data
    
    def getTransaction(self, transaction_hash):
        result = explorer.tx.get(transaction_hash)        
        return result.data
        
    def getAddressInfo(self, bitcoin_address):
        result = explorer.addr.get(bitcoin_address)
        return result.data
    
    def getBlocksAtHeight(self, block_height):
        hash_at_height = explorer.blocks.get_height(block_height).data
        block = self.getBlock(hash_at_height)
        return block

    def getCoinbaseTransaction(self, block_hash):
        tx_hashes = explorer.blocks.get_txids(block_hash)
        coinbase_tx_hash = tx_hashes.data[0]
        coinbase_tx = explorer.tx.get(coinbase_tx_hash)
        return coinbase_tx.data
    
# ====================================   
#  Data handler methods
# ====================================
          
    
    def __extractBlockHeight(self, block):
        height = block['height']
        return height
    
    def __extractPrevBlockHash(self, block):
        prev_block_hash = block['previousblockhash']
        return prev_block_hash
    
    def __extractBlockTimestamp(self, block):
        return block['timestamp']
                
    def getBlockInformation(self, block_hash):
        """
        Extract various bits of information from a specified block and 
        returns a dictionary.

        Parameters
        ----------
        block_hash : string

        Returns
        -------
        block_information : dict.
        """
        self.logger.debug("Querying Blockstream.info API to obtain information about block: {}".format(block_hash))
        block = self.getBlock(block_hash)
        block_height = self.__extractBlockHeight(block)
        coinbase_tx = self.getCoinbaseTransaction(block_hash)
        coinbase_message = Utils.hexStringToAscii(coinbase_tx['vin'][0]['scriptsig'])
        payout_address = coinbase_tx['vout'][0]['scriptpubkey_address']

        block_information = {
            "block_hash": block_hash,
            "prev_block_hash": self.__extractPrevBlockHash(block),
            "block_height": self.__extractBlockHeight(block),
            "timestamp" : self.__extractBlockTimestamp(block),
            "coinbase_tx_hash": coinbase_tx['txid'],
            "coinbase_message": coinbase_message,
            "pool_name": None,
            "pool_address": payout_address,
            "fee_block_reward": coinbase_tx['vout'][0]['value'] - Utils.getBlockReward(self.__extractBlockHeight(block)), 
            "total_block_reward": coinbase_tx['vout'][0]['value']
            }
        self.logger.debug("Information succesfully obtained from the Blockstream.info API for block at height {} with hash: {}".format(block_height, block_hash))
        return block_information
        
        
        
    