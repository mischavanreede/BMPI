# -*- coding: utf-8 -*-
""" 
A class file used to query data from the https://www.blockchain.com/ API.

Documentation: https://www.blockchain.com/api/blockchain_api



https://blockchain.info/rawblock/0000000000000bae09a7a393a8acded75aa67e46cb81f7acaa5ad94f9eacd103


    
    

@author: Mischa van Reede



Possible API urls:
    
    Source: https://www.blockchain.com/api/blockchain_api
    
    Implemented in this class:
        
        Single block:               https://blockchain.info/rawblock/$block_hash
        Single Transaction:         https://blockchain.info/rawtx/$tx_hash

        Block Height:               https://blockchain.info/block-height/$block_height?format=json
        Single Address:             https://blockchain.info/rawaddr/$bitcoin_address
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

import json
import time


from .generic_rest_requests import RestRequests


class BlockchainScraper(RestRequests):
        
    def __init__(self, config, logger):
        
        
        self.base_url = "https://blockchain.info/"
        self.api_key = None
        self.timeout = 5 #DefaultTimeout is 10 seconds
        self.config = config
        self.logger = logger
        super().__init__(config=self.config, logger=self.logger) # Initialize RestRequest object from parent class
                
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
            Note that this is not the complete block information, for example, it does not contain  
            Example:
                
        """
        latest_block_url = self.base_url + "latestblock"
        latest_basic_block = self.get(latest_block_url)
        latest_block_hash = latest_basic_block['hash']
        result = self.getBlock(latest_block_hash)
        
        return result
    
    def getBlock(self, block_hash): 
        
        """
        Parameters
        ----------
        block_hash : TYPE
            DESCRIPTION.

        Returns
        -------
        result : TYPE
            DESCRIPTION.
            
            Example result:
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

        """
        assert isinstance(block_hash, str)
        
        single_block_url = self.base_url + "rawblock/" + block_hash
        result = self.get(single_block_url)
        return result

    def getTransaction(self, transaction_hash):
        """
        

        Parameters
        ----------
        transaction_hash : str
            hash value of a particular transaction on the bitcoin blockchain.

        Returns
        -------
        result : TYPE
            DESCRIPTION.
            
            EXAMPLE result:
                {
                  "hash": "b6f6991d03df0e2e04dafffcd6bc418aac66049e2cd74b80f14ac86db1e3f0da",
                  "ver": 1,
                  "vin_sz": 1,
                  "vout_sz": 2,
                  "lock_time": "Unavailable",
                  "size": 258,
                  "relayed_by": "64.179.201.80",
                  "block_height": 12200,
                  "tx_index": "12563028",
                  "inputs": [
                    {
                      "prev_out": {
                        "hash": "a3e2bcc9a5f776112497a32b05f4b9e5b2405ed9",
                        "value": "100000000",
                        "tx_index": "12554260",
                        "n": "2"
                      },
                      "script": "76a914641ad5051edd97029a003fe9efb29359fcee409d88ac"
                    }
                  ],
                  "out": [
                    {
                      "value": "98000000",
                      "hash": "29d6a3540acfa0a950bef2bfdc75cd51c24390fd",
                      "script": "76a914641ad5051edd97029a003fe9efb29359fcee409d88ac"
                    },
                    {
                      "value": "2000000",
                      "hash": "17b5038a413f5c5ee288caa64cfab35a0c01914e",
                      "script": "76a914641ad5051edd97029a003fe9efb29359fcee409d88ac"
                    }
                  ]
                }

        """
        assert isinstance(transaction_hash, str)
        
        transaction_url = self.base_url + "rawtx/" + transaction_hash
        result = self.get(transaction_url)
        return result
    
    
    def getBlocksAtHeight(self, block_height):
        """
        
        Parameters
        ----------
        block_height : TYPE
            DESCRIPTION.

        Returns
        -------
        result : TYPE
            DESCRIPTION.
            
            Example result:
                {
                  "blocks": [
                    "--Array Of Blocks at the specified height--"
                  ]
                }

        """
        assert isinstance(block_height, int)
        
        block_height_url = self.base_url + "block-height/" + str(block_height) + "?format=json"
        result = self.get(block_height_url)
        return result
    
    def getAddressInfo(self, bitcoin_address):
        """
        TODO implement limit & offset when there are more than 50 transactions

        Parameters
        ----------
        bitcoin_address : TYPE
            DESCRIPTION.

        Returns
        -------
        result : TYPE
            DESCRIPTION.
            
            Example result:
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
        """
        assert isinstance(bitcoin_address, str)
    
        single_address_url = self.base_url + "rawaddr/" + bitcoin_address
        result = self.get(single_address_url)
        return result
          
    
# ====================================   
#  Data handler methods 
# ====================================     
        

    def getLatestBlockHeight(self):
        return self.getLatestBlock()['height']
  
    def getLatestBlockHash(self):
        return self.getLatestBlock()['hash']
  
    
    def hexStringToAscii(self, hex_string):
        return bytes.fromhex(hex_string).decode('ascii', 'ignore')
        
    
    def extractBlockMessage(self, block):
        """
        Mining data in block dict ->   tx/0/inputs/0/script: 

        Parameters
        ----------
        block : TYPE
            DESCRIPTION.

        Returns
        -------
        message : TYPE
            DESCRIPTION.

        """
        message = block['tx'][0]['inputs'][0]['script']
        return message

    def formatObjectToJson(self, data_object, indent=4):
        return json.dumps(data_object, indent)



a = BlockchainScraper(None, None)


# latest_block = a.getLatestBlock()

# msg = a.extractBlockMessage(latest_block)


# latest_block.pop('tx')

# print(latest_block)
# print(msg)

# json_obj = json.dumps(result, indent=4)
# print(json_obj)

    
    



