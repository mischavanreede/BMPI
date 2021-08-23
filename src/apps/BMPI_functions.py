# -*- coding: utf-8 -*-

"""


@author: Mischa van Reede
"""

import json
import time
import sys
import click

from .elastic import ElasticsearchController, ElasticsearchIndexes
from .API_scrapers.scraper_controller import ScraperController
from .API_scrapers.blockchain_scraper import BlockchainScraper
from .API_scrapers.btc_scraper import BtcScraper
from .utils import Utils



class BMPIFunctions():
    
    
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        
        self.block_list = []
        self.skipped_blocks_list = []
        self.API_conflicts = []
        
        # Init modules
        self.__initialize_modules()

    def __initialize_modules(self):
        
        
        self.es_controller = ElasticsearchController(config=self.config, logger=self.logger)
        
        self.scraper_controller = ScraperController(config=self.config, logger=self.logger)

        

    
    def removeALLStoredElasticsearchData(self):
        self.logger.info("Removing all data from elasticsearch.")
        for index_name in ElasticsearchIndexes.INDEX_NAMES:
            self.es_controller.delete_index(index_name)
        self.logger.info("All data removed.")
        
    
    
    def removeStoredElasticsearchData(self, index):
        self.logger.info("Removing the all data in index: {}".format(index))
        self.es_controller.delete_index(index)
        self.logger.info("Data removed.")
    
    
    
    def gatherAndStoreMissingBlocksFromScrapers(self):
        pass
    
    def gatherAndStoreBlocksFromScrapers(self, start_hash=None,
                                         start_height=None,
                                         blocks_stored=0,
                                         blocks_skipped=0,
                                         api_conflicts=0):
        '''
        This method is used to gather relevant block data from the implemented
        blockchain web service API scrapers. It traverses the entire blockchain
        starting at the latest block_height.

        '''
        if start_hash is not None and start_height is not None:
            block_height = start_height
            block_hash = start_hash
        
        else:
            latest_block_hash, latest_height = self.scraper_controller.getLatestBlockHashAndHeight()        
            block_height = latest_height
            block_hash = latest_block_hash
        
        block_store_interval = 100
        forced_stopped = False
        
        total_blocks_stored = blocks_stored # 0 if not set
        total_blocks_skipped = blocks_skipped # 0 if not set
        total_number_of_api_conflicts = api_conflicts # 0 if not set
        
        while block_height >= 0:
            succesfully_gathered_block = False
            exception_encoutered = False
            conflict_encountered = False
            
            # Store blocks when %block_store_interval blocks are gathered.
            if (block_height % block_store_interval == 0) or forced_stopped:
                
                total_blocks_stored += len(self.block_list)
                total_blocks_skipped += len(self.skipped_blocks_list)
                total_number_of_api_conflicts += len(self.API_conflicts) 
                
                self.performInterimBlockStorage()
                
                self.logger.info("Total number of blocks successfully stored: {}".format(total_blocks_stored))
                self.logger.info("Total number of blocks skipped: {}".format(total_blocks_skipped))
                self.logger.info("Total number of blocks API conflicts: {}".format(total_number_of_api_conflicts))
                
            if forced_stopped:
                sys.exit()
          
            try: # Gathering new block
                self.logger.info("Gathering block at height: {}".format(block_height))
                result = self.scraper_controller.getBlockInfoFromScrapers(block_hash)
                #self.logger.debug("Found block: {}".format(result))
                
            except KeyboardInterrupt:
                forced_stopped = True
                self.logger.info("Gracefully stopping application.")
                continue
            
            except Exception as e:
                # Catch timeout exc and other exceptions
                self.logger.warning("Exception encountered.")
                exception_encoutered = True
                exception_type = type(e).__name__
                self.logger.info("Type of exception: {}".format(exception_type))
                self.logger.debug(str(e))
                skipped_blocks_entry = {
                        "block_height": block_height,
                        "block_hash": block_hash,
                        "reason_for_skipping": "Exception encountered: {}".format(exception_type)
                        }
                self.skipped_blocks_list.append(skipped_blocks_entry)
            
            else: # No exception found and some result is returned.
                succesfully_gathered_block = (result['status'] == "success")
                conflict_encountered = (result['status'] == "conflict")
                
            # finally:
            #     #code that is run after each try
            
            if succesfully_gathered_block:
                # Set variables (block_hash and block_height) for next iteration of outer while
                block = result['block']
                self.block_list.append(block)
                block_hash = block['prev_block_hash']
                block_height -= 1
                self.logger.debug("Block succesfully gathered.\n")
                continue # continue with next iteration
            
            if exception_encoutered:
                block_hash, block_height = self.findValidPrecedingHashAndHeight(block_height)
                self.logger.debug("Block hash found for next iteration.\n")
                continue 
                
            if conflict_encountered:
                self.logger.info("Conflict encountered.")
                # Set block height to conflict entry
                result['conflict_entry']['block_height'] = block_height
                # Add conflict block to list
                self.API_conflicts.append(result['conflict_entry'])
                # Add skipped block
                skipped_blocks_entry = {
                    "block_height": block_height,
                    "block_hash": block_hash,
                    "reason_for_skipping": "Conflicting API information."
                    }
                self.skipped_blocks_list.append(skipped_blocks_entry)
               
                # Set variables (block_hash and block_height) for next iteration of outer while
                if result['prev_hash_equal']:
                    block_hash = result['prev_hash']
                    block_height -= 1
                    self.logger.debug("Block hash found for next iteration.\n")
                    continue # continue with next iteration
                else:     
                    block_hash, block_height = self.findValidPrecedingHashAndHeight(block_height)
                    self.logger.debug("Block hash found for next iteration.\n")
                    continue # 4.
            self.logger.error("Code should not reach this part.")
        # End of outer while    
        self.logger.info("END of loop: Crawling API's for block data complete.")    
    

    def performInterimBlockStorage(self):
        # Store succesfully gathered blocks
        if self.es_controller.bulk_store(records=self.block_list, index_name="blocks_from_scrapers"):
            self.logger.info("Succesfully stored last {} blocks".format(len(self.block_list)))
            self.block_list.clear()
        else:
            self.logger.error("Failed to store blocks in es. Please check the logs.")
        # Store skipped blocks
        if len(self.skipped_blocks_list) > 0:
            if self.es_controller.bulk_store(records=self.skipped_blocks_list, index_name="skipped_blocks"):
                self.logger.info("Succesfully stored last {} skipped blocks".format(len(self.skipped_blocks_list)))
                self.skipped_blocks_list.clear()
            else:
                self.logger.error("Failed to store skipped blocks heights in es. Please check the logs.")
        # Store API data conflicts
        if len(self.API_conflicts) > 0:
            if self.es_controller.bulk_store(records=self.API_conflicts, index_name="api_block_data_conflicts"):
                self.logger.info("Succesfully stored last {} API data conflicts".format(len(self.API_conflicts)))
                self.API_conflicts.clear()
            else:
                self.logger.error("Failed to store skipped blocks heights in es. Please check the logs.")           

    def findValidPrecedingHashAndHeight(self, block_height):
        # 1. Keep decreasing block_height until a matching previous hash is found
        # 2. set block_hash to this previous hash
        # 3. set height to this previous height
        # 4. go to next iteration             
        while True:
            # 1.
            previous_height = block_height - 1
            self.logger.debug("Trying to gather hash at block_height: {}".format(previous_height))
            try:
                previous_hash = self.scraper_controller.getBlockHashAtHeight(previous_height)
                
            except Exception as e: 
                exception_type = type(e).__name__
                self.logger.error("Exception encountered: {}".format(exception_type))
                self.logger.warning("This exception was found while retrieving the block_hash at height: {}".format(previous_height))
                self.logger.warning("Waiting 30 seconds before trying again.")
                time.sleep(30)
                previous_height += 1
                continue # Stay in while true loop

            if previous_hash is None:
                self.logger.debug("Couldn't find hash at this height.")
                self.logger.debug("Saving this height in skipped_blocks list.")
                skipped_blocks_entry = {
                    "block_height": previous_height,
                    "block_hash": None,
                    "reason_for_skipping": "API's return different values for prev_hash"
                    }
                self.skipped_blocks_list.append(skipped_blocks_entry)
            
            if previous_hash is not None:
                # 2. + 3.
                self.logger.debug("Found a hash at this height")
                block_height = previous_height
                block_hash = previous_hash
                break # break out while true loop
        return block_hash, block_height
    
    
    def gatherAndStoreSpecificBlock(self, block_height=None, block_hash=None):
        
        if block_height is None or block_hash is None:
            self.logger.error("Please specify a valid block height and block hash.")
        
        succesfully_gathered_block = False
        exception_encoutered = False
               
        try: # Gathering new block
            self.logger.info("Gathering block at height: {}".format(block_height))
            result = self.scraper_controller.getBlockInfoFromScrapers(block_hash)
            self.logger.debug("Found block: {}".format(result))
                           
        except Exception as e:
            # Catch timeout exc and other exceptions
            self.logger.warning("Exception encountered.")
            exception_encoutered = True
            exception_type = type(e).__name__
            self.logger.info("Type of exception: {}".format(exception_type))
            self.logger.debug(str(e))
            skipped_blocks_entry = {
                    "block_height": block_height,
                    "block_hash": block_hash,
                    "reason_for_skipping": "Exception encountered: {}".format(exception_type)
                    }
            self.skipped_blocks_list.append(skipped_blocks_entry)
        
        else: # No exception found and some result is returned.
            succesfully_gathered_block = (result['status'] == "success")
            conflict_encountered = (result['status'] == "conflict")
                
        if succesfully_gathered_block:
            # Set variables (block_hash and block_height) for next iteration of outer while
            block = result['block']
            self.block_list.append(block)
            self.logger.debug("Block succesfully gathered.\n")
            
        if exception_encoutered:
            self.logger.error("Please check the logs to see what happened.")
            sys.exit(1)
                
        if conflict_encountered:
            self.logger.info("Conflict encountered.")
            # Set block height to conflict entry
            result['conflict_entry']['block_height'] = block_height
            # Add conflict block to list
            self.API_conflicts.append(result['conflict_entry'])
            # Add skipped block
            skipped_blocks_entry = {
                "block_height": block_height,
                "block_hash": block_hash,
                "reason_for_skipping": "Conflicting API information."
                }
            self.skipped_blocks_list.append(skipped_blocks_entry)
        
        self.logger.debug("Storing results in ES.")
        self.performInterimBlockStorage()
        self.logger.debug("Results stored.")
        
    

    def attributePoolNames(self):
        '''
        add click parameter to choose which data to use(API scaper data or node data)
        possible also which pool_data.json to use
        store in seperate indices
        '''
        pass
        # def attributePoolNameToBlock(self, block):
        #     pool_data_updated = False
        #     explicitly_find_tag_match = True
    
        #     payout_address = block['pool_address']
        #     coinbase_message = block['coinbase_message']
            
            
            
        #     # TODO: combine various pool_data sources
        #     # TODO: implement check for multiple output addresses
        #     # TODO: add logger info
            
        #     if payout_address in self.pool_data_json['payout_addresses']:
        #         address_match_pool_name = self.pool_data_json['payout_addresses'][payout_address]['name']
        #         self.logger.debug("Found a matching payout address (match={})".format(address_match_pool_name))
        #         self.logger.debug("Payout address = {}".format(payout_address))
                            
        #         # Check if the miner uses it's pool tag
        #         if explicitly_find_tag_match:
        #             for coinbase_tag, pool_info in self.pool_data_json['coinbase_tags'].items():
        #                 if coinbase_tag in coinbase_message and address_match_pool_name == pool_info['name']:
        #                     # pool_name is the same for address and tag match "everything is ok"
        #                     self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
        #                     block['pool_name'] = address_match_pool_name
        #                     return
                        
        #                 elif coinbase_tag in coinbase_message and address_match_pool_name != pool_info['name']:
        #                     self.conflict_encoutered = True
        #                     tag_match_pool_name = pool_info['name']
        #                     self.logger.info("Naming conflict occured in attributing pool name to block: {}".format(block['block_hash']))
        #                     self.logger.debug("Coinbase message = {}".format(block['coinbase_message']))
        #                     self.logger.info("Found conflicting matches for pool_names. Please inspect the logs")
        #                     self.logger.debug("Found matches; address_match_pool_name={} , tag_match_pool_name={}".format(address_match_pool_name, tag_match_pool_name))
        #                     self.logger.debug("Saving conflicts to (conflicting_pool_name_attribution) entry in conflict JSON")
        #                     self.logger.debug("Attributing payout_address match as this match takes precedence.")
            
                            
        #                     conflict_entry = self.__constructConflictingPoolNameAttributionDataEntry(block, tag_match_pool_name, address_match_pool_name)
        #                     self.__addConflictingData(conflict_type="conflicting_pool_name_attribution", conflict_entry=conflict_entry)
        #                     self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
        #                     block['pool_name'] = address_match_pool_name
        #                     return
    
        #             self.logger.debug("Found a matching payout address (match={}), but no matching coinbase tag.".format(address_match_pool_name))
        #             self.logger.debug("Coinbase message = {}".format(block['coinbase_message']))
        #             self.logger.debug("Saving block with message for manual inspection")
        #             #TODO: safe blocks in seperate json file
        #             self.logger.debug("Block attributed to: {}".format(address_match_pool_name))
        #             block['pool_name'] = address_match_pool_name
        #             return
          
        #     # No quick address match found, 
        #     # trying to find matching coinbase tag for the coinbase message.
        #     # If match is found update pool_data JSON's with payout address.
        #     else:
        #         tag_match = False
        #         for coinbase_tag, pool_info in self.pool_data_json['coinbase_tags'].items():
        #             # Keep track of multiple tag matches
        #             match_list = []
                    
        #             if coinbase_tag in coinbase_message:
        #                 tag_match_pool_name = pool_info['name']
        #                 match_list.append(tag_match_pool_name)
                        
        #         if tag_match:
        #             if self.all_equal(match_list):
        #                 tag_match_pool_name = match_list[0]
        #                 block['pool_name'] = tag_match_pool_name
        #                 self.__updatePoolDataJSON(tag_match_pool_name, block['pool_address'])
        #                 return
        #             else:
        #                 self.logger.warning("Naming conflict! Multiple matching tags found in coinbase message.")
        #                 self.logger.debug("Unsure to which pool to attribute this block. Attributing to 'Unknown'.")
        #                 self.logger.debug("Matches found: {}".format(set(match_list)))
        #                 self.conflict_encoutered = True
        #                 conflict_entry = self.__constructMultipleCoinbaseTagMatchesDataEntry(block, match_list)
        #                 self.__addConflictingData(conflict_type='multiple_coinbase_tag_matches', conflict_entry=conflict_entry)
        #     # No matches found, setting pool_name to unknown.
        #     self.logger.debug("Block attributed to: Unknown")
        #     block['pool_name'] = "Unknown"
        #     return        
    
    
    def combineKnownPoolDataFiles(self):
        # Keep the weird characters of F2Pool in mind,
        # maybe change encoding when opening files.
        self.logger.info("Combining pool data into combined_data.json")
        self.logger.debug("Loading json file data.")
        
        with open('../../known-pools/data/blockchain-com_data.json', mode='r') as blockchain_com_file:
            blockchain_com_json = json.load(blockchain_com_file)
            
        with open('../../known-pools/data/btc-com_data.json', mode='r') as btc_com_file:
            btc_com_json = json.load(btc_com_file)
            
        with open('../../known-pools/data/0xB10c_data.json', mode='r') as B10c_file:
            B10c_json = json.load(B10c_file)
            
        with open('../../known-pools/data/sjors_data.json', mode='r') as sjors_file:
            sjors_json = json.load(sjors_file)
            
        self.logger.debug("File data loaded.")
        
        json_data = [blockchain_com_json, btc_com_json, B10c_json, sjors_json]
        
        
        # Not finished
          
        pass
    
    def deleteStoredBlocksFromElasticsearch(self, start_height, end_height, should_delete=False):
        '''
        Deletes blocks between start_height and end_height that are stored in
        the index {blocks_from_scrapers} in the elasticsearch instance. 
        '''
        self.logger.info("Deleting documents between heights {} and {}".format(start_height, end_height))
        total_mismatch_list = []
        for height in range(start_height, end_height+1, 1):
            mismatches = []
            query = "block_height:{}".format(height)
            # get stored docs from es
            results = self.es_controller.query_es(index='blocks_from_scrapers', query=query)
            self.logger.info("Found {} matches for block at height {}".format(len(results), height))
            self.logger.debug("Looping over results.")
            
            for result in results:
                doc_id = result["_id"]
                height_from_doc = result["_source"]["block_height"]
                self.logger.debug("Handling document: {} at height {}. Height in doc: {}".format(doc_id, height, height_from_doc))
                if height_from_doc == height:
                    #height matches, delete doc
                    self.logger.debug("Height matches.")
                    doc_type= "_doc"
                    index_name = "blocks_from_scrapers"
                    if should_delete:
                        self.es_controller.delete_doc(index=index_name, doc_type=doc_type, id=doc_id)
                else:
                    self.logger.warning("Height doesn't match. Not deleting this document.")
                    mismatches.append((height, doc_id))
            self.logger.info("Found {} mismatches in height field for results at height {}.".format(len(mismatches), height))
            total_mismatch_list.extend(mismatches)
        self.logger.info("Done deleting documents.")
        self.logger.info("Found {} mismatches in total".format(len(total_mismatch_list)))
        self.logger.debug("Printing mismatches: {}".format(total_mismatch_list))
    
    
    def reindexBlocksFromScraperData(self):
        '''
            Should reindex existing docs in blocks_from_scrapers index.
        '''
        pass
        
    
    def printScrapers(self):
        
        self.logger.info("Printing scrapers for testing purposes.")
        for scraper in self.scraper_controller.scrapers:
            self.logger.debug("Found scraper: {}".format(repr(scraper)))
            print(repr(scraper))
            print("Done.")
            self.logger.debug("Done printing scrapers.")
        
        
    
    def runScrapers(self):
        self.scraper_controller.getLastBlocks(n=25)
        print("Done.")
    
    @Utils.printTiming
    def run(self):
        pass
        
        
    
        
        
        
    