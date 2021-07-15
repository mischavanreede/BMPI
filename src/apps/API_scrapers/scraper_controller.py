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
        with open(file="../pools/pool_data.json", mode='r', encoding='utf-8') as f:
            self.pool_data_json = json.load(f)
         
        self.conflict_encoutered = False
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
            "gathered_data": [
                {
                "scraper": scraper,
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


    def getLatestBlockHeight(self):
        latest_height_list = []
        self.logger.debug("Obtianing latest block height from all implemented scrapers.")
        for scraper in self.scrapers:
            latest_height_list.append(scraper.getLatestBlockHeight())
        if self.all_equal(latest_height_list):
            self.logger.debug("Obtained block heights are all equal, returning latest height.")
            return latest_height_list[0]
        else:
            self.logger.warning("Obtained block heights do not match. Please check the logs")
            self.logger.warning("Gathered the following information: {}".format([list(zip(self.scrapers, latest_height_list))]))
            self.logger.debug("Returning the lowest height number to continue.")
            return min(latest_height_list)
    
    def getLatestBlockHash(self):
        latest_hash_list = []
        self.logger.debug("Obtianing latest block hash from all implemented scrapers.")
        for scraper in self.scrapers:
            latest_hash_list.append(scraper.getLatestBlockHash())
        if self.all_equal(latest_hash_list):
            self.logger.debug("Obtained hashes are all equal, returning latest hash.")
            return latest_hash_list[0]
        else:
            self.logger.warning("Obtained block heights do not match. Please check the logs")
            self.logger.warning("Gathered the following information: {}".format(zip(self.scrapers, latest_hash_list)))
            self.logger.warning("Returning the first hash to continue for now.")
            return latest_hash_list[0]

    
    def getBlockInfoListFromScrapers(self, block_hash):
        block_info_list = []
        #Get block from scrapers
        for scraper in self.scrapers:
            block = scraper.getBlockInformation(block_hash)
            block_info_list.append(block)
        
        # Compare blocks
        if self.all_equal(block_info_list): # Check if blocks are equal
            # All blocks are equal, so returning the first one
            self.logger.debug("Obtained blocks with hash [{}] are equal.".format(block_hash))
            block_to_return = block_info_list[0]
            self.logger.debug("Attributing mining pool to block: {}".format(block_hash))
            self.attributePoolNameToBlock(block_to_return)
            return block_to_return
        
        else: # Blocks are not equal
            self.logger.warning("Conflict in data obtained from scrapers. The obtained block data is not equal. Please inspect the logs.")
            self.logger.warning("Found a mismatch in the blocks with hash [{}]:".format(block_hash))
            self.conflict_encoutered = True

            prev_hashes = []
            for scraper, block in zip(self.scrapers, block_info_list):
                self.attributePoolNameToBlock(block)
                prev_hashes.append(block['prev_block_hash'])
                 
            conflict_entry = self.__constructConflictingApiInformationDataEntry(block_hash, block_info_list)
            self.__addConflictingData(conflict_type="conflicting_API_information", conflict_entry=conflict_entry)
                
            if self.all_equal(prev_hashes):
                self.logger.info("Previous hashes are equal.")
                self.logger.info("Returning first block becasue prev_hashes are equal.")
                self.logger.warning("Please inspect the logs (conflicting_API_information) for block: {}".format(block_hash))
                return block_info_list[0]
            else:
                self.logger.exception("Conflict in a prev_hash field of blocks. Couldn't determine which block to return.")
                raise Exception("Conflict in a prev_hash field of blocks. Couldn't determine which block to return.")

    
    def getLastBlocks(self, n=1):
        previous_hash = self.getLatestBlockHash()
        latest_height = self.getLatestBlockHeight()
        blocks = []
        #times = []
        self.logger.info("Gathering the last n={} blocks, starting from height={} and block hash: {}".format(n, latest_height, previous_hash))
        for i in range(n):
            self.logger.debug("")
            self.logger.info("Gathering block at height: {}".format(latest_height-i))
            self.logger.debug("Gathering block at with hash: {}".format(previous_hash))
            #tic = time.perf_counter()
            block_info = self.getBlockInfoListFromScrapers(previous_hash)
            #toc = time.perf_counter()
            #times.append(round(toc - tic, 4))
            blocks.append(block_info)
            previous_hash = block_info['prev_block_hash']
        # self.logger.info("Printing results to terminal:")
        # for block in blocks:
        #     print("Block at height: {}".format(block['block_height']))
        #     Utils.prettyPrint(block)
        
        #print("Average time per block request: {}".format(sum(times)/len(times)))
             
    
    def attributePoolNameToBlock(self, block):
        pool_data_updated = False
        explicitly_find_tag_match = True

        payout_address = block['pool_address']
        coinbase_message = block['coinbase_message']
        
        # TODO: combine various pool_data sources
        # TODO: implement check for multiple output addresses
        # TODO: add logger info
        
        if payout_address in self.pool_data_json['payout_addresses']:
            address_match_pool_name = self.pool_data_json['payout_addresses'][payout_address]['name']
                        
            # Check if the miner uses it's pool tag
            if explicitly_find_tag_match:
                for coinbase_tag, pool_info in self.pool_data_json['coinbase_tags'].items():
                    if coinbase_tag in coinbase_message and address_match_pool_name == pool_info['name']:
                        # pool_name is the same for address and tag match "everything is ok"
                        self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
                        block['pool_name'] = address_match_pool_name
                        return
                    
                    elif coinbase_tag in coinbase_message and address_match_pool_name != pool_info['name']:
                        self.conflict_encoutered = True
                        tag_match_pool_name = pool_info['name']
                        self.logger.info("Naming conflict occured in attributing pool name to block: {}".format(block['block_hash']))
                        self.logger.debug("Found matches; address_match_pool_name={} , tag_match_pool_name={}".format(address_match_pool_name, tag_match_pool_name))
                        self.logger.debug("Coinbase message = {}".format(block['coinbase_message']))
                        self.logger.info("Found conflicting matches for pool_names. Please inspect the logs")
                        self.logger.debug("Saving conflicts to (conflicting_pool_name_attribution) entry in conflict JSON")
                        self.logger.debug("Attributing payout_address match as this match takes precedence.")
        
                        
                        conflict_entry = self.__constructConflictingPoolNameAttributionDataEntry(block, tag_match_pool_name, address_match_pool_name)
                        self.__addConflictingData(conflict_type="conflicting_pool_name_attribution", conflict_entry=conflict_entry)
                        self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
                        block['pool_name'] = address_match_pool_name
                        return

                self.logger.debug("Found a matching payout address (match={}), but no matching coinbase tag.".format(address_match_pool_name))
                self.logger.debug("Saving block with message for manual inspection")
                self.logger.debug("Coinbase message = {}".format(block['coinbase_message']))
                #TODO: safe blocks in seperate json file
                self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
                block['pool_name'] = address_match_pool_name
                return
      
        # No quick address match found, 
        # trying to find matching coinbase tag for the coinbase message.
        # If match is found update pool_data JSON's with payout address.
        else:
            tag_match = False
            for coinbase_tag, pool_info in self.pool_data_json['coinbase_tags'].items():
                
                # Keep track of multiple tag matches
                match_list = []
                
                if coinbase_tag in coinbase_message:
                    tag_match_pool_name = pool_info['name']
                    match_list.append(tag_match_pool_name)
                    
            if tag_match:
                if self.all_equal(match_list):
                    tag_match_pool_name = match_list[0]
                    block['pool_name'] = tag_match_pool_name
                    self.__updatePoolDataJSON(tag_match_pool_name, block['pool_address'])
                    return
                else:
                    self.logger.warning("Naming conflict! Multiple matching tags found in coinbase message.")
                    self.logger.debug("Unsure to which pool to attribute this block. Attributing to 'Unknown'.")
                    self.logger.debug("Matches found: {}".format(set(match_list)))
                    self.conflict_encoutered = True
                    conflict_entry = self.__constructMultipleCoinbaseTagMatchesDataEntry(block, match_list)
                    self.__addConflictingData(conflict_type='multiple_coinbase_tag_matches', conflict_entry=conflict_entry)
        # No matches found, setting pool_name to unknown.
        self.logger.debug("Block attributed to: Unknown")
        block['pool_name'] = "Unknown"
        return        