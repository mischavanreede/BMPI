# -*- coding: utf-8 -*-
"""


@author: Mischa van Reede
"""

import json

class BlockAnalyser():
    
      
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        
        # Load json file data into memory
        
        # Known pools data base path
        base_path = "../../known-pools/"
    
        # External known pools file data, sources are in known-pools/sources
        B10c_known_pools_file_path =            base_path + "data/0xB10c.json"
        blockchain_com_known_pools_file_path =  base_path + "data/blockchain-com.json"
        btc_com_known_pools_file_path =         base_path + "data/btc-com.json"
        sjors_known_pools_file_path =           base_path + "data/sjors.json"
    
        # My pool data
        my_known_pools_file_path =  base_path + "pools.json"
        
        B10c_data = {
                "name": "0xB10c",
                "data": self.__load_json(B10c_known_pools_file_path)
            }
        blockchain_data = {
                "name": "Blockchain.com",
                "data": self.__load_json(blockchain_com_known_pools_file_path)
            }
        btc_data = {
                "name": "BTC.com",
                "data": self.__load_json(btc_com_known_pools_file_path)
            }
        sjors_data = {
                "name": "Sjors Provoost",
                "data": self.__load_json(sjors_known_pools_file_path)
            }
        my_pool_data = {
                "name": "My Data",
                "data": self.__load_json(my_known_pools_file_path)
            }
        
        self.known_pool_data_list = [B10c_data, blockchain_data, btc_data, sjors_data, my_pool_data]
        
        
        
    def __load_json(self, file_path):
        self.logger.debug("Trying to load [{}] file into memory.".format(file_path))  
        try:
            with open(file_path, mode='r') as file:
                data = json.load(file)
            self.logger.debug("File loaded, returning data.")
            return data
        except IOError:
            self.logger.error("Failed to load file.")
            return None
        
            
        
        
    
    def AttributePoolName(self, coinbase_message):
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
    