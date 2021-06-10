# -*- coding: utf-8 -*-
"""
Blockchain.com API library documentation: https://github.com/blockchain/api-v1-client-python

"""



from blockchain import blockexplorer
import json
import codecs

class BlockchainScraperLib():
    
    def __init__(self, config, logger):
        self.config = config
        self.logger =  logger
        self.blockexplorer = blockexplorer
    
    
    def getLatestBlock(self):
        
        latest_block = blockexplorer.get_latest_block()
        block_hash = latest_block.hash
        
        block = blockexplorer.get_block(block_hash)
        
        msg = block.transactions[0].inputs[0].script_sig
        
        decoded = codecs.decode(msg, "hex").decode('ascii', errors="ignore")

        print(decoded)
   
    
    def getGenisisBlock(self):
        block = blockexplorer.get_block("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
        
        msg = block.transactions[0].inputs[0].script_sig
        
        decoded = codecs.decode(msg, "hex").decode('ascii', errors="ignore")
        # block = json.loads(block)
        
        # print(my_block)
        print(decoded)

        
        

logger = None
config = None

b = BlockchainScraperLib(logger, config)

#b.getGenisisBlock()
b.getLatestBlock()