# -*- coding: utf-8 -*-
"""
A scraper module used to query data from the blockstream.com API through the
use of the bloxplorer library.

Documentation
    https://blockstream.info/
    https://pypi.org/project/bloxplorer/
    https://valinsky.me/bloxplorer/

@author: Mischa van Reede

"""

import time

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
    
    def getHashAtHeight(self, block_height):
        result = explorer.blocks.get_height(block_height)
        return result.data
    
    def getBlocksAtHeight(self, block_height):
        hash_at_height = explorer.blocks.get_height(block_height).data
        block = self.getBlock(hash_at_height)
        return block

    def getCoinbaseTransaction(self, block_hash):
        
        max_tries = 3
        self.logger.debug("Obtaining coinbase transaction information.")
        
        for attempt in range(max_tries):
            try:
                tx_hashes = explorer.blocks.get_txids(block_hash)
                coinbase_tx_hash = tx_hashes.data[0]
                coinbase_tx = explorer.tx.get(coinbase_tx_hash)
            except Exception as ex:
                exception_name = type(ex).__name__
                self.logger.error("Found: " + str(ex))
                if 'BlockstreamClientTimeout' in exception_name:
                    #In the event of a Timeout, BlockstreamClientTimeout will be raised.
                    self.logger.info("Encoutered a BlockstreamClientTimeout Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.info("Trying again after 5 seconds.")
                    self.logger.info("Waiting...")
                    time.sleep(5)
                elif 'BlockstreamApiError' in exception_name:
                    # In the event of an API error (e.g. Invalid resource, Bad Request, etc), Bloxplorer will raise BlockstreamApiError.
                    self.logger.info("Encoutered a BlockstreamApiError Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.info("Trying again after 5 seconds.")
                    self.logger.info("Waiting...")
                    time.sleep(5)    
                else: # BlockstreamClientError
                    # For anything else, Bloxplorer will raise a BlockstreamClientError.
                    self.logger.error("Encoutered a generic BlockstreamClientError Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.error("Exception message: {}".format(str(ex)))
                    time.sleep(1)
            else:
                self.logger.debug("Transaction gathered.")
                break
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
    
    def __getPayoutAddressesFromCbTx(self, coinbase_tx):
        payout_addresses = []
        
        for output in coinbase_tx['vout']:
            if 'scriptpubkey_address' in output.keys():
                payout_addresses.append(output['scriptpubkey_address'])
                
            if 'scriptpubkey_type' in output.keys():
                if output['scriptpubkey_type'] == "p2pk":
                    public_key = output["scriptpubkey"][2:-2] # Remove first and last two bytes, they signal script instructions
                    address = Utils.bitcoin_address_from_pub_key(pub_key=str(public_key))
                    payout_addresses.append(address)
                
                
        return payout_addresses
        
    def __getBlockReward(self, coinbase_tx):
        total_reward = 0
        
        for output in coinbase_tx['vout']: 
            #if there is a payout value, unfortunately blockstream doensn't include payout address for scripting type transactions such as "p2sh" or "p2pk"
            if ('value' in output.keys()):
                total_reward += output['value']
        
        if total_reward >= Utils.btcToSats(21*10**6):
            self.logger.error("The total reward should not exceed the max number of bitcoins.")
            return -1
        return total_reward 
                
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
        max_tries = 3
        self.logger.debug("Querying Blockstream.info API to obtain block information.")
        
        for attempt in range(max_tries):
            try:
                self.logger.debug("Gathering block.")
                block = self.getBlock(block_hash)
            except Exception as ex:
                exception_name = type(ex).__name__
                self.logger.error("Found: " + str(exception_name))
                if 'BlockstreamClientTimeout' in exception_name:
                    #In the event of a Timeout, BlockstreamClientTimeout will be raised.
                    self.logger.info("Encoutered a BlockstreamClientTimeout Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.info("Trying again after 5 seconds.")
                    time.sleep(5)
                elif 'BlockstreamApiError' in exception_name:
                    # In the event of an API error (e.g. Invalid resource, Bad Request, etc), Bloxplorer will raise BlockstreamApiError.
                    self.logger.info("Encoutered a BlockstreamApiError Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.info("Trying again after 5 seconds.")
                    time.sleep(5)    
                else: # BlockstreamClientError
                    # For anything else, Bloxplorer will raise a BlockstreamClientError.
                    self.logger.error("Encoutered a generic BlockstreamClientError Exception on try {} out of {}.".format(attempt+1, max_tries))
                    self.logger.error("Exception message: {}".format(str(ex)))
                    time.sleep(1)
            else:
                self.logger.debug("Block gathered.")
                break
        
        if not block:
            self.logger.error("Couldn't retrieve the block: {}".format(block_hash))
            return {}
        
        self.logger.debug("Processing block information.")
        block_height = self.__extractBlockHeight(block)
        coinbase_tx = self.getCoinbaseTransaction(block_hash)
        coinbase_message = Utils.hexStringToAscii(coinbase_tx['vin'][0]['scriptsig'])
        payout_addresses = self.__getPayoutAddressesFromCbTx(coinbase_tx)#coinbase_tx['vout'][0]['scriptpubkey_address']
        block_reward = self.__getBlockReward(coinbase_tx)
        fee = -1
        if block_reward >= 0:
            fee = block_reward - Utils.getBlockReward(self.__extractBlockHeight(block))
            if fee >= Utils.btcToSats(21*10**6):
                self.logger.error("Fee exceeds max number of bitcoins for block {}. Setting value to -1.".format(block_height))
                fee = -1
            
        block_information = {
            "block_hash": block_hash,
            "prev_block_hash": self.__extractPrevBlockHash(block),
            "block_height": block_height,
            "timestamp" : self.__extractBlockTimestamp(block) * 1000,
            "coinbase_tx_hash": coinbase_tx['txid'],
            "coinbase_message": coinbase_message,
            "payout_addresses": payout_addresses,
            "fee_block_reward": fee, 
            "total_block_reward": block_reward
            }
        self.logger.debug("Block information succesfully obtained from the Blockstream.info API.")
        return block_information
        
        
        
    