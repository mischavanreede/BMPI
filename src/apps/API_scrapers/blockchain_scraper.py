# -*- coding: utf-8 -*-
""" 
A class file used to query data from the https://www.blockchain.com/ API.

TODO:  Implement [getCoinbaseTransaction] to only retrieve the first transaction from a block

@author: Mischa van Reede

API Documentation: https://www.blockchain.com/api/blockchain_api

Available API responses:    
    Implemented in this class:
        Single block:               
            https://blockchain.info/rawblock/$block_hash
            Example Block result:
                {
                  "hash": "0000000000000bae09a7a393a8acded75aa67e46cb81f7acaa5ad94f9eacd103",
                  "ver": 1,
                  "prev_block": "00000000000007d0f98d9edca880a6c124e25095712df8952e0439ac7409738a",
                  "mrkl_root": "935aa0ed2e29a4b81e0c995c39e06995ecce7ddbebb26ed32d550a72e8200bf5",
                  "time": 1322131230,
                  "bits": 437129626,
                  "nonce": 2964215930,
                  "n_tx": 22,
                  "size": 9195,
                  "block_index": 818044,
                  "main_chain": true,
                  "height": 154595,
                  "received_time": 1322131301,
                  "relayed_by": "108.60.208.156",
                  "tx": [
                    "--Array of Transactions--"
                  ]
                }
        
        Single Transaction:         
            https://blockchain.info/rawtx/$tx_hash
        Block Height:               
            https://blockchain.info/block-height/$block_height?format=json
            Example result from get request, returns the 'blocks' list:
                result = {
                  "blocks": [
                    "--Array Of Blocks at the specified height--"
                  ]
                }
        Single Address:             https://blockchain.info/rawaddr/$bitcoin_address
            Example Address result:
                {
                  "hash160": "660d4ef3a743e3e696ad990364e555c271ad504b",
                  "address": "1AJbsFZ64EpEfS5UAjAfcUG8pH8Jn3rn1F",
                  "n_tx": 17,
                  "n_unredeemed": 2,
                  "total_received": 1031350000,
                  "total_sent": 931250000,
                  "final_balance": 100100000,
                  "txs": [
                    "--Array of Transactions--"
                  ]
                }
        Latest Block:               https://blockchain.info/latestblock
    
    Not implemented:
        Chart Data:                 https://blockchain.info/charts/$chart-type?format=json
        Multi Address:              https://blockchain.info/multiaddr?active=$address|$address
        Unspent Outputs:            https://blockchain.info/unspent?active=$address
        Balance:                    https://blockchain.info/balance?active=$address
        Unconfirmed Transactions:   https://blockchain.info/unconfirmed-transactions?format=json
        Blocks: (returns 404's):    - https://blockchain.info/blocks/$time_in_milliseconds?format=json
                                    - https://blockchain.info/blocks/$pool_name?format=json      
"""

import time

from .generic_rest_requests import RestRequests
from ..utils import Utils

class BlockchainScraper(RestRequests):
        
    def __init__(self, config, logger):
        self.base_url = "https://blockchain.info/"
        self.api_key = None
        self.config = config
        self.logger = logger
        # Initialize RestRequest object from parent class
        super().__init__(config=self.config, logger=self.logger)
        
    def __str__(self):
        return "Blockchain.com API scraper"
    
    def __repr__(self):
        return "Blockchain.com API scraper"
            
# ====================================   
#  API query methods 
# ====================================

    def getLatestBlock(self):
        """
        Queries the Blockchain.com API to obtain information on the latest mined block.
        It first queries the latestblock API call to obtain the hash_value of the latest block,
        after which it calls getBlock(hash_value) to obtain the full block. This is because 
        the latest block returns a BasicBlock with limited information instead of a full Block structure.
        
        TODO check for orphan blocks at the latest block height
        
        Returns
        -------
        result : <class 'dict'>
            Dictionary containing information on the latest block mined. 
            Note that this is not the complete block information, it cointains a 'BasicBlock' (see API documentation) object.
                
        """
        latest_block_url = self.base_url + "latestblock"
        latest_basic_block = self.get(latest_block_url)
        latest_block_hash = latest_basic_block['hash']
        result = self.getBlock(latest_block_hash)
        return result
    
    def getLatestBlockHeight(self):
        return self.getLatestBlock()['height']
  
    def getLatestBlockHash(self):
        return self.getLatestBlock()['hash']
    
    def getBlock(self, block_hash): 
        """
        Get a block, using a block hash as parameter, from the Blockchain.com API
        
        """
        assert isinstance(block_hash, str) 
        single_block_url = self.base_url + "rawblock/" + block_hash
        result = super().get(single_block_url)
        return result
    
    def getHashAtHeight(self, block_height):
        assert isinstance(block_height, int)
        block_height_url = self.base_url + "block-height/" + str(block_height) + "?format=json"
        result = super().get(block_height_url)
        
        hash_list = []
        for block in result['blocks']:
            hash_list.append(block['hash'])
        
        return hash_list
        
        

    def getBlocksAtHeight(self, block_height):
        """
        Returns a list of Blocks at the specified height.
                    
        """
        assert isinstance(block_height, int)
        block_height_url = self.base_url + "block-height/" + str(block_height) + "?format=json"
        result = super().get(block_height_url)
        return result['blocks']    

    def getTransaction(self, transaction_hash):
        """
        Get a transaction object 

        """
        assert isinstance(transaction_hash, str)
        
        transaction_url = self.base_url + "rawtx/" + transaction_hash
        result = super().get(transaction_url)
        return result
    
    def getAddressInfo(self, bitcoin_address):
        """
        TODO implement limit & offset when there are more than 50 transactions          

        """
        assert isinstance(bitcoin_address, str)
    
        single_address_url = self.base_url + "rawaddr/" + bitcoin_address
        result = super().get(single_address_url)
        return result        
    
# ====================================   
#  Data handler methods 
# ====================================     
        

    def __extractCoinbaseTransaction(self, block):
        return block['tx'][0]
             
    def __extractPrevBlockHash(self, block):
        """
        Extracts the hash of the preceding block from a block specified by
        its block hash.
        """
        return block['prev_block']
    
    def __extractBlockHeight(self, block):
        return block['height']
    
    def __extractBlockTimestamp(self, block):
        return block['time']
    
    
    def __getCoinbaseScriptMessage(self, block):
        """
        Extracts the miners' message from a block and return the hex value 
        Mining data in block dict ->   tx/0/inputs/0/script: 
        """
        return Utils.hexStringToAscii(block['tx'][0]['inputs'][0]['script'])
    
    def __getPayoutAddressesFromCbTx(self, coinbase_tx):
        payout_addresses = []
        
        for output in coinbase_tx['out']:
            if 'addr' in output.keys():
                payout_addresses.append(output['addr'])
        return payout_addresses
        
    
    def __getBlockReward(self, coinbase_tx):
        total_reward = 0
        
        for output in coinbase_tx['out']: #if there is a payout addres and payout value
            if ('addr' in output.keys()) and ('value' in output.keys()):
                total_reward += output['value']
                
        if total_reward >= 21*10**6:
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
        self.logger.debug("Querying Blockchain.com API to obtain block information.")
        max_tries = 3
        
        for attempt in range(max_tries):
            try:
                self.logger.debug("Gathering block.")
                block = self.getBlock(block_hash)
            except Exception as e:
                # For anything else, Bloxplorer will raise a BlockstreamClientError.
                self.logger.error("Encoutered a generic BlockstreamClientError Exception on try {} out of {}.".format(attempt+1, max_tries))
                self.logger.info("Exception message: {}".format(str(e)))
                self.logger.info("Trying again after 5 seconds.")
                time.sleep(5)
            else:
                self.logger.debug("Block gathered.")
                break
        
        if not block:
            self.logger.info("Couldn't retrieve the block. Returning empty dict.")
            return {}
        
        self.logger.debug("Processing block information.")
        block_height = self.__extractBlockHeight(block)
        coinbase_tx = self.__extractCoinbaseTransaction(block)
        coinbase_message = Utils.removeNonAscii(self.__getCoinbaseScriptMessage(block))
        
        #Utils.prettyPrint(coinbase_tx['out'])
        #payout_addresses = coinbase_tx['out'][0]['addr']
        block_reward = self.__getBlockReward(coinbase_tx)
        payout_addresses = self.__getPayoutAddressesFromCbTx(coinbase_tx)
        #self.logger.debug("Total reward: {}, Payout Addresses: [{}]".format(block_reward, payout_addresses))
        
        if block['fee'] >= 21*10**6:
            self.logger.warning("Fee exceeds max number of bitcoins. Setting value to -1.")
            fee = -1
        else:
            fee = block['fee']
            
        block_information = {
            "block_hash": block_hash,
            "prev_block_hash": self.__extractPrevBlockHash(block),
            "block_height": block_height,
            "timestamp" : self.__extractBlockTimestamp(block) * 1000,
            "coinbase_tx_hash": coinbase_tx['hash'],
            "coinbase_message": coinbase_message,
            "payout_addresses": payout_addresses,
            "fee_block_reward": fee, 
            "total_block_reward": block_reward
            }
        self.logger.debug("Block information succesfully obtained from the Blockchain.info API.")
        return block_information


        

    
    



