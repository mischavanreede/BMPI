# -*- coding: utf-8 -*-
"""
A controller module that gathers and compares data from 
the various API scraper modules that have been implemented.

TODO: add logger information to various scraper modules

Scraper modules:
    - BlockchainScraper() [blockchain_scraper.py], 
        status=CONNECTED,
    - BlockStreamScraper() [blockstream_scraper.py], 
        status=CONNECTED,
        uses bloxplorer library.
    - BlockCypherScraper() [blockcypher_scraper.py], 
        status=NOT USED, use limited to 2000 request per day.
    - BtcScraper() [btc_scraper.py], 
        status=NOT USED, limits number of requests to +- 10 per minute.
    
@author: Mischa van Reede
"""

from .blockchain_scraper import BlockchainScraper
from .blockstream_scraper import BlockstreamScraper
#from .blockcypher_scraper import BlockcypherScraper
#from .btc_scraper import BtcScraper


import time
import json
from itertools import groupby


class ScraperController:
    
    
    def __init__(self, config, logger):

        self.config = config
        self.logger = logger        
        
        # Initialize API scrapers        
        self.BlockchainScraper = BlockchainScraper(config, logger)
        self.BlockstreamScraper = BlockstreamScraper(config, logger)             
         #self.BlockcypherScraper = BlockcypherScraper(config, logger)
         #self.BtcScraper = BtcScraper(config, logger) 
        
        self.scrapers = [self.BlockchainScraper, self.BlockstreamScraper]
        
        # Load pool data in local variable
        # with open(file="../pools/pool_data.json", mode='r', encoding='utf-8') as f:
        #     self.pool_data_json = json.load(f)
         
        self.block_conflicts = {
            "conflicting_pool_name_attribution":[
                    {
                        "block_hash": "0_DummyBlockhash",
                        "block_height": "DummyHeight",
                        "payout_address_pool_name": "DummyAddressPoolName",
                        "coinbase_tag_pool_name": "DummyTagPoolName",
                        "block": "DummyBlock"
                    }         
                ],
            "conflicting_API_information":[
                    {
                        "block_hash": "0_DummyBlockhash",
                        "gathered_data": [
                                {   "scraper": "Dummy name of scraper",
                                     "block": "DummyBlock"
                                }
                        ]
                    }
                ],
            "multiple_coinbase_tag_matches":[
                    {
                        "block_hash": "0_DummyBlockhash",
                        "block_height": "DummyHeight",
                        "coinbase_tag_pool_names": ["DummyTagPoolNames"],
                        "block": "DummyBlock"
                        
                    }
                ]
            }
    
    def all_equal(self, iterable):
        """
        Checks if all objects in an iterable (e.g. list) are equal.
        Used in this class to compare blocks
        """
        g = groupby(iterable)
        return next(g, True) and not next(g, False)
    
    def __addConflictingData(self, conflict_type, conflict_entry):
        self.logger.info("Conflict found! Conflict type: {}".format(conflict_type))
                
        if conflict_type == "conflicting_API_information":
            # This occurs when data retrieved from the implemented API's does not match
            self.block_conflicts['conflicting_API_information'].append(conflict_entry)
        
        elif conflict_type == "conflicting_pool_name_attribution":
            # This occurs when 
            self.block_conflicts['conflicting_pool_name_attribution'].append(conflict_entry)
            
        elif conflict_type == "multiple_coinbase_tag_matches":
            self.block_conflicts['multiple_coinbase_tag_matches'].append(conflict_entry)
        
        self.logger.info("Conflicting data added to local variable (self.block_conflicts), TODO: write to file")

    
    def __constructConflictingApiInformationDataEntry(self, block_hash, block_info_list):
        entry = {
            "block_hash": block_hash,
            "block_height": None,
            "gathered_data": [
                {
                "scraper": str(scraper),
                "block": block
                } 
                for scraper, block in zip(self.scrapers, block_info_list)]
            }
        return entry
        
    
    def __constructConflictingPoolNameAttributionDataEntry(self, block, tag_match_pool_name, address_match_pool_name):
        block_hash = block['block_hash']
        block_height = block['block_height']                      
        return {
            "block_hash": block_hash,
            "block_height": block_height,
            "payout_address_pool_name": address_match_pool_name,
            "coinbase_tag_pool_name": tag_match_pool_name,
            "block": block
            }
    
    def __constructMultipleCoinbaseTagMatchesDataEntry(self, block, match_list):
        return {
            "block_hash": block['block_hash'],
            "block_height": block['block_height'],
            "coinbase_tag_pool_names": match_list,
            "block": block
            }
    
    def __updatePoolDataJSON(self, pool_name, payout_address):
        entry = {'name': pool_name}
        
        # Update local class variable
        self.pool_data_json['payout_addresses'][payout_address] = entry
        
        # Update pool_data.json
        with open(file="../../pools/pool_data.json", mode='r', encoding='utf-8') as jsonFile:
            data = json.load(jsonFile)
        # Update data
        data['payout_addresses'][payout_address] = entry
        # Then write data with new entries to file
        with open(file="../../pools/pool_data.json", mode='w', encoding='utf-8') as jsonFile:
            json.dump(data, jsonFile, indent=4, sort_keys=True)
        
        # Also update the merged file
        self.__updateMergedPoolDataJSON(pool_name, payout_address)
        
          
    def __updateMergedPoolDataJSON(self, pool_name, payout_address):
        self.logger.debug("Adding payout address [{}] to pool [{}]".format(payout_address, pool_name))
        # Update pool_data_merged.json
        with open(file="../pools/pool_data_merged.json", mode='r', encoding='utf-8') as jsonFile:
            data = json.load(jsonFile)
        # Update data
        data['mining_pools'][pool_name]['pool_addresses'].append(payout_address)
        # Then write data with new entries to file
        with open(file="../pools/pool_data.json", mode='w', encoding='utf-8') as jsonFile:
            json.dump(data, jsonFile, indent=4, sort_keys=True)

    
    def getBlockHashAtHeight(self, height):
        self.logger.debug("Obtaining hash at height [{}] from both scrapers".format(height))
        # Hash from Blockstream.info scraper, returns single value
        blockstream_hash = self.BlockstreamScraper.getHashAtHeight(height)
        self.logger.debug("Blockstream scraper returned: {}".format(blockstream_hash))
        
        # Hash from Blockchain.com scraper, returns list of hashes
        blockchain_hash = self.BlockchainScraper.getHashAtHeight(height)
        self.logger.debug("Blockchain.com scraper returned: {}".format(blockchain_hash))
        
        if len(blockchain_hash) > 1:
            self.logger.debug("Blockchain.com returned multiple hashes at height: {}".format(height))
        
        if blockstream_hash in blockchain_hash:
            self.logger.debug("Found a matching hash.")
            self.logger.debug("Assuming that the matching hash is the right one.")
            return blockstream_hash
        
        self.logger.warning("No matching hash found at block height: {}".format(height))
        return None                    
   
    def getLatestBlockHashAndHeight(self):
        latest_hash_list = []
        latest_height_list = []
        self.logger.debug("Obtaining latest block hash and height from all implemented scrapers.")
        for scraper in self.scrapers:
            latest_hash_list.append(scraper.getLatestBlockHash())
            latest_height_list.append(scraper.getLatestBlockHeight())

        
        if self.all_equal(latest_hash_list):
            self.logger.debug("Obtained hashes are all equal.")
            if self.all_equal(latest_height_list):
                self.logger.debug("Obtained block heights are all equal")
                return latest_hash_list[0], latest_height_list[0]

        self.logger.error("Obtained block heights do not match. Please check the logs.")
        self.logger.error("Gathered the following information: {}".format(zip(self.scrapers, latest_hash_list)))
        raise Exception("Mismatch in latest hash and or latest height of block.")

    
    def getBlockInfoFromScrapers(self, block_hash):
        block_info_list = []
        self.logger.debug("Gathering block with hash: {}".format(block_hash))
        #Get block from scrapers
        for scraper in self.scrapers:
            block = scraper.getBlockInformation(block_hash)
            block_info_list.append(block)
        
        
        self.logger.debug("Blocks succesfully gathered.")
        # Compare blocks
        if self.all_equal(block_info_list): # Check if blocks are equal
            # All blocks are equal, so returning the first one
            self.logger.debug("Obtained blocks are equal.")
            block_to_return = block_info_list[0]
            return {"status": "success",
                    "block": block_to_return}
        
        else: # Blocks are not equal
            self.logger.warning("The obtained block data is not equal (Conflicting API information).")
            self.logger.debug("Conflict found in block: {}".format(block_hash))

            prev_hashes = []
            for scraper, block in zip(self.scrapers, block_info_list):
                prev_hashes.append(block['prev_block_hash'])
                 
            conflict_entry = self.__constructConflictingApiInformationDataEntry(block_hash, block_info_list)
                
            if self.all_equal(prev_hashes):
                self.logger.info("Previous hashes are equal.")
                
                return {"status": "conflict",
                        "prev_hash_equal": True,
                        "prev_hash": prev_hashes[0],
                        "conflict_entry": conflict_entry}
            else:
                self.logger.info("Couldn't determine prev_hash. Returning conflict data.")
                return {"status": "conflict",
                        "prev_hash_equal": False,
                        "prev_hash": None,
                        "conflict_entry": conflict_entry}
