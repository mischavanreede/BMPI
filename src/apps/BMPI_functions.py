# -*- coding: utf-8 -*-

"""


@author: Mischa van Reede
"""

import json
import time
import sys

from datetime import datetime

from .elastic import ElasticsearchController, ElasticsearchIndexes
from .attribute_blocks import BlockAnalyser
from .API_scrapers.scraper_controller import ScraperController
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
        
        self.block_analyser = None

    
    def removeALLStoredElasticsearchData(self):
        self.logger.info("Removing all data from elasticsearch.")
        for index_name in ElasticsearchIndexes.INDEX_NAMES:
            self.es_controller.delete_index(index_name)
        self.logger.info("All data removed.")
    
        
    def remove_duplicate_api_conflicts(self):
        api_index = "api_block_data_conflicts"
        self.logger.debug("Removing duplicates from {}".format(api_index))
        #gather list of all blockheights
        results = self.es_controller.query_all_docs(index=api_index)
        self.logger.debug("Returned a total of {} results".format(len(results)))
        
        #Keep track of heights, use set to avoid duplicates:
        height_set = set()
               
        for doc in results:
            height = doc['block_height']
            height_set.add(height)
        
        self.logger.debug('Found a total of {} unique heights'.format(len(height_set)))
        
        for height in height_set:
        #for each blockheight
            self.logger.debug("Handling height: {}".format(height))
            # construct query
            height_query = "block_height:{}".format(height)
            # delete duplicate method from elastic.py
            self.es_controller.remove_all_but_one_by_query(index=api_index, query=height_query)
        self.logger.debug("Done deleting duplicate api conflicts.")
    
    def removeStoredElasticsearchData(self, index):
        self.logger.info("Removing the all data in index: {}".format(index))
        self.es_controller.delete_index(index)
        self.logger.info("Data removed.")
    

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
        if self.es_controller.bulk_store(records=self.block_list, index_name="blocks_from_scrapers_updated"):
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
    
    
    def gatherSpecificBlock(self, block_height=None, block_hash=None, store_block=True):
        
        if block_height is None or block_hash is None:
            self.logger.error("Please specify a valid block height and block hash.")
        
        succesfully_gathered_block = False
        exception_encoutered = False
               
        try: # Gathering new block
            self.logger.info("Gathering block at height: {}".format(block_height))
            result = self.scraper_controller.getBlockInfoFromScrapers(block_hash)
            #self.logger.debug("Found block: {}".format(result))
                           
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
            
            if store_block:
                self.logger.debug("Storing results in ES.")
                self.performInterimBlockStorage()
                self.logger.debug("Results stored.")
            
            return True
            
        if exception_encoutered:
            self.logger.warning("Exception encountered. Please check the logs to see what happened.")
            
            if store_block:
                self.logger.debug("Storing results in ES.")
                self.performInterimBlockStorage()
                self.logger.debug("Results stored.")
            
            return False
                            
        if conflict_encountered:
            self.logger.warning("Conflict encountered.")
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
            
            if store_block:
                self.logger.debug("Storing results in ES.")
                self.performInterimBlockStorage()
                self.logger.debug("Results stored.")
                
            return False
        
    @Utils.printTiming
    def gatherMissingBlocksFromScrapers(self):
   
        skipped_index = "skipped_blocks"
        
        skip_reasons = set()
        # Get records of skipped blocks
        #skipped_blocks = self.es_controller.query_all_docs(index=skipped_index)
        skipped_blocks = self.es_controller.query_all_docs_with_metadata(index=skipped_index)
        #skipped_blocks = self.es_controller.query_es(query="*", index=skipped_index)
        
        # Reverse list to start at lower block heights
        
        skipped_blocks.reverse()
        
        for record in skipped_blocks:
            # Add reason for skipping this block to set of reasons
            skip_reasons.add(record["data"]["reason_for_skipping"])
        
        self.logger.info("Found a total of {} reasons for skipping blocks.".format(len(skip_reasons)))
        self.logger.info("Namely: {}".format(list(skip_reasons)))
        
        # total_skipped_blocks = len(skipped_blocks) 
        # print("first block:")
        # Utils.prettyPrint(skipped_blocks[0])
        # print("last block:")
        # Utils.prettyPrint(skipped_blocks[total_skipped_blocks-1])
        
        success = 0
        failed = 0
        
        
        for record in skipped_blocks:
            # Trying to re-gather skipped block
            if (self.gatherSpecificBlock(block_height=record["data"]["block_height"],
                                              block_hash=record["data"]["block_hash"],
                                              store_block=(True))):
                self.logger.info("Successfully re-gathered block: {}".format(record["data"]["block_height"]))
                success += 1
   
            else:
                self.logger.warning("Failed to re-gather block: {}".format(record["data"]["block_height"]))
                failed += 1
            
            # Delete old record from skipped blocks index
            self.deleteDocByID(index=skipped_index, 
                                doc_id=record["id"])   
          
        self.logger.info("Re-gathering complete. Successfull: {},  Failed: {}".format(success, failed))
        return

    @Utils.printTiming
    def attributePoolNames(self, run_id=None, update_my_pool_data=False, start_height=None, end_height=None):
        '''
        Reads block data from the blocks_from_scrapers_updated index,
        calls the attribute_blocks module to determine pool name for each block,
        writes results to the block_attributions index.
        '''
        self.block_analyser = BlockAnalyser(config=self.config, logger=self.logger)
        if not run_id:
            run_id = datetime.now().strftime("%b/%m/%Y %H:%M:%S")               
        block_data_index = "blocks_from_scrapers_updated"
        name_attribution_index = "block_attributions"
        skipped_heights = []
        
        assert(run_id and start_height and end_height)
        self.logger.info("Starting attribution algorithm with following variables:")
        self.logger.info("Run_id: {}, start_height: {}, end_height: {}, update_my_pool_data: {}".format(run_id, start_height, end_height, update_my_pool_data))
        
        for height in range(start_height, end_height+1):
            self.logger.info("Attributing pool name to block: {}".format(height))
            # get block from elasticsearch
            query = "block_height:{}".format(height)
            
            results = None
            for attempt in range(3):
                results = self.es_controller.query_es(index=block_data_index, query=query)
                if results:
                    break
                else:
                    time.sleep(3)
            if results is None:
                self.logger.warning("Couldn't get a response from the ES instance for block height: {}".format(height))
                skipped_heights.append(height)
                continue # Go to next height in outer loop
            
            self.logger.debug("Found {} results for height {}".format(len(results), height))
            if len(results) == 0:
                # No result found with matching block height, go to next height
                skipped_heights.append(height)
                self.logger.info("No stored block found for height: {}".format(height))
                continue
                
            elif len(results) > 1:
                block = None
                for doc in results:
                    if doc["_source"]['block_height'] == height:
                        block = doc["_source"]
                        break 
                if block == None:
                    # No result found with exact matching block height
                    self.logger.info("No stored block found for height: {}".format(height))
                    continue
            else:
                block = results[0]["_source"]
            
            # extract block information
            block_hash = block["block_hash"]
            block_height = block["block_height"]
            time_stamp = block["timestamp"]
            coinbase_message = block["coinbase_message"]
            payout_addresses = list(block["payout_addresses"])
            fee_reward = block["fee_block_reward"]
            total_reward = block["total_block_reward"]
            
            # attribute pool name based on block information
            results = self.block_analyser.AttributePoolName(coinbase_message=coinbase_message, 
                                                            payout_addresses=payout_addresses,
                                                            update_my_pools_json=update_my_pool_data)
            
            # construct a results document to be stored in index block_attributions
            data_entry = {
                "run_id": run_id,
                "block_height": block_height,
                "block_hash": block_hash,
                "timestamp" : time_stamp,
                "coinbase_message": coinbase_message,
                "payout_addresses": list(payout_addresses),
                "fee_block_reward": fee_reward,
                "total_block_reward": total_reward,
                "0xB10C_results": results["0xB10C"],
                "0xB10C_attribution": results["0xB10C"]["pool_name"],
                "Blockchain_com_results": results["Blockchain_com"],
                "Blockchain_com_attribution": results["Blockchain_com"]["pool_name"],
                "BTC_com_results": results["BTC_com"],
                "BTC_com_attribution": results["BTC_com"]["pool_name"],
                "My_results": results["My_attribution"],
                "My_attribution": results["My_attribution"]["pool_name"]
                }
            # store result document
            for attempt in range(3):
                if self.es_controller.store(record=data_entry, index_name=name_attribution_index):
                    self.logger.debug("Result stores successfully.")
                    break
                else:
                    self.logger.warning("Storing of results failed. Trying again.")
                    time.sleep(3)
            
                self.logger.warning("Couldnt store results of block at height: {}".format(height))
                skipped_heights.append(height)
            self.logger.debug("Continuing with the next block_height.\n")
        
        # Done, finishing up
        self.logger.info("Block attribution complete.")
        self.logger.info("Skipped a total of {} blocks".format(len(skipped_heights)))
        
        if len(skipped_heights) > 0:
            self.logger.info("Writing skipped heights to skipped_heights_log.json file")
            entry = {
                str(run_id) : skipped_heights
                }
            current_path = Utils.getCurrentPath()
            with open(current_path + '/../logs/skipped_heights.json', mode='w', encoding='utf-8') as outfile:
                json.dump(entry, outfile, indent=4)
                self.logger.debug("Skipped heights saved to log file 'skipped_heights.json'")


    
    def deleteStoredBlocksFromElasticsearch(self, index, start_height, end_height, should_delete=False):
        '''
        Deletes blocks between start_height and end_height that are stored in
        the provided index in the elasticsearch instance. 
        '''
        self.logger.info("Deleting documents between heights {} and {}".format(start_height, end_height))
        total_mismatch_list = []
        for height in range(start_height, end_height+1, 1):
            mismatches = []
            query = "block_height:{}".format(height)
            # get stored docs from es
            results = self.es_controller.query_es(index=index, query=query)
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
                    index_name = "blocks_from_scrapers_updated"
                    if should_delete:
                        self.es_controller.delete_doc(index=index_name, doc_type=doc_type, doc_id=doc_id)
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
        Reindex existing block data from blocks_from_scrapers to updated
        blocks_from_scrapers_updated index.
        '''
        
        old_index = "blocks_from_scrapers"
        new_index = "blocks_from_scrapers_updated"
        
        self.logger.info("Reindexing data from {} to {}".format(old_index, new_index))
        self.es_controller.reindex_data(source_index=old_index, dest_index=new_index)
        self.logger.info("Reindexing complete.")
        
        
    def printScrapers(self):
        self.logger.info("Printing scrapers for testing purposes.")
        for scraper in self.scraper_controller.scrapers:
            self.logger.debug("Found scraper: {}".format(repr(scraper)))
            print(repr(scraper))
            print("Done.")
            self.logger.debug("Done printing scrapers.")
        
    
    def deleteDocByID(self, index, doc_id):
        self.logger.info("Deleting document [{}] from index [{}]".format(doc_id, index))
        self.es_controller.delete_doc(index=index, doc_type="_doc", doc_id=doc_id)
        self.logger.info("Document deleted.")
        
        

        
    
        
        
        
    