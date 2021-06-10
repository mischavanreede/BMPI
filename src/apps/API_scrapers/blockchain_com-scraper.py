# -*- coding: utf-8 -*-
""" 
A class file used to query data from the https://www.blockchain.com/ API.

Blockchain.com API: https://github.com/blockchain/api-v1-client-python

https://github.com/blockchain/api-v1-client-python/blob/master/docs/blockexplorer.md

https://blockchain.info/rawblock/0000000000000bae09a7a393a8acded75aa67e46cb81f7acaa5ad94f9eacd103

Mining data in ->   tx/0/inputs/0/script: 
    
    https://www.nylas.com/blog/use-python-requests-module-rest-apis/

@author: Mischa van Reede


https://www.blockchain.com/api/blockchain_api
Expample urls:
    - https://blockchain.info/latestblock
    - https://blockchain.info/rawblock/$block_hash
    - https://blockchain.info/rawtx/$tx_hash
    


"""


from generic_rest_requests import RestRequests


class BlockchainScraper(RestRequests):
        
    def __init__(self, config, logger):
        
        
        self.base_url = "https://blockchain.info/"
        self.api_key = None
        self.timeout = 5 #DefaultTimeout is 10 seconds
        self.config = config
        self.logger = logger
        super().__init__(config=self.config, logger=self.logger) # Initialize RestRequest object from parent class
                
        
    def getLatestBlock(self):
        """
        Queries the Blockchain.com API to obtain information on the latest mined block..         
        
        Returns
        -------
        result : dict
            Dictionary containing information on the latest block mined.
            Example:
               {
                  "hash": "0000000000000538200a48202ca6340e983646ca088c7618ae82d68e0c76ef5a",
                  "time": 1325794737,
                  "block_index": 841841,
                  "height": 160778,
                  "txIndexes": [
                    13950369,
                    13950510,
                    13951472
                  ]
            } 
        """
        latest_block_url = self.base_url + "latestblock"
        result = self.get(latest_block_url)
        return result
    
    def getLatestBlockHeight(self):
        return self.getLatestBlock()['height']
  
    def getLatestBlockHash(self):
        return self.getLatestBlock()['hash']
  
    
    def getBlockFromHash(self, block_hash):
        assert isinstance(block_hash, str)
        single_block_url = self.base_url + "rawblock/" + block_hash
        result = self.get(single_block_url)
        return result
    
    
    def getGenesisBlock(self):
        genesis_hash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
        return self.getBlockFromHash(genesis_hash)
    
    def extractBlockMessage(self, block_hash):
        block = self.getBlockFromHash(block_hash)
        message = block['tx'][0]['inputs'][0]['script']
        return message

    

a = BlockchainScraper(None, None)

# genesis_message = bytes.fromhex(a.getGenesisBlock()['tx'][0]['inputs'][0]['script']).decode(encoding="utf-8", errors='ignore')

latest_hash = a.getLatestBlockHash()
message = a.extractBlockMessage(latest_hash)
print(message)
print(bytes.fromhex(message).decode('ascii', 'ignore'))


# print(message)
# print(bytes.fromhex(message).decode(encoding="utf-8", errors='ignore'))

# print(a.getLatestBlock())

