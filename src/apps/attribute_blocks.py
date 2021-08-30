# -*- coding: utf-8 -*-
"""


@author: Mischa van Reede
"""

import json
import Utils

class BlockAnalyser():
    
      
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        
        # Load json file data into memory
        
        # Known pools data base path
        base_path = "../../known-pools/"
    
        # External known pools file data, sources are in known-pools/sources
        B10C_known_pools_file_path =            base_path + "data/0xB10C.json"
        blockchain_com_known_pools_file_path =  base_path + "data/blockchain-com.json"
        btc_com_known_pools_file_path =         base_path + "data/btc-com.json"
        sjors_known_pools_file_path =           base_path + "data/sjors.json"
    
        # My pool data1
        my_known_pools_file_path =  base_path + "pools.json"
        
        B10C_data = {
                "name": "0xB10C",
                "data": self.__load_json(B10C_known_pools_file_path)
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
        
        # All data from external sources in a list
        self.external_pool_data_list = [B10C_data, blockchain_data, btc_data, sjors_data]
        self.B10C_data = B10C_data
        self.blockchain_data = blockchain_data
        self.btc_data = btc_data
        self.sjors_data = sjors_data
        # My known-pools data kept in seperate variable for updating.
        self.my_pool_data = my_pool_data
        
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
        
    def __update_my_pool_data(self, entry_key, record, record_type):

        my_data_updated = False
 
        if record_type == "payout_addresses":
            
            if entry_key not in self.my_pool_data["payout_addresses"]:
                self.logger.debug("Adding payout address [{}] to my_pool_data.".format(entry_key))
                self.my_pool_data["payout_addresses"][entry_key] = record
                my_data_updated = True
                
            else:
                self.logger.debug("Entry {} already exists".format(entry_key))
                return
            
        elif record_type == "coinbase_tags":
            
            if entry_key not in self.my_pool_data["coinbase_tags"]:
                self.logger.debug("Adding coinbase_tag {} to my_pool_data.".format(entry_key))
                self.my_pool_data["coinbase_tags"][entry_key] = record
                my_data_updated = True
                
            else:
                self.logger.debug("Entry {} already exists".format(entry_key))
                return
            
        else:
            self.logger.error("Record type not supported: {}".format(record_type))
            return
               
        if my_data_updated:
            self.logger.debug("Writing changes to known-pools/pools.json file")
            current_path = Utils.getCurrentPath()
            with open(current_path + '/../../known-pools/pools.json', mode='w', encoding='utf-8') as outfile:
                json.dump(self.my_pool_data, outfile, sort_keys=True, indent=4)
                self.logger.debug("known-pools/pools.json file updated.")
        return
    
    
    def getPoolNameFromData(self, coinbase_message, payout_addresses, pool_data, update_data):
        
        pool_name_attribution = {
            "pool_name": None,
            "multiple_matches": False,
            "payout_addresses_matches": None,
            "coinbase_tag_matches": None
            }
        
        payout_address_matches = set()
        coinbase_tag_matches = set()
        
        found_address_match = False
        found_tag_match = False
        
        # Find matches
        for payout_address in payout_addresses:
            
            if payout_address in pool_data['payout_addresses']:
                found_address_match = True
                pool_name = pool_data["payout_addresses"][payout_address]["name"]
                payout_address_matches.add(pool_name)
        
        for tag in pool_data["coinbase_tags"]:
            
            if tag in coinbase_message:
                found_tag_match = True
                pool_name = pool_data["coinbase_tags"][tag]["name"]
                coinbase_tag_matches.add(pool_name)
                
        # Handle matches
        if not found_address_match and not found_tag_match:
            # No match found, pool name "Unknown"
            pool_name_attribution["pool_name"] = "Unknown"
            pool_name_attribution["payout_addresses_matches"] = []
            pool_name_attribution["coinbase_tag_matches"] = []
            return pool_name_attribution
               
        if found_address_match and not found_tag_match:
            name_matches = len(payout_address_matches)
            if name_matches == 1:
                pool_name_attribution["pool_name"] = list(payout_address_matches)[0]
                pool_name_attribution["payout_addresses_matches"] = list(payout_address_matches)
                pool_name_attribution["coinbase_tag_matches"] = []
                
            else:
                # Multiple names matches found
                pool_name_attribution["pool_name"] = "Unknown"
                pool_name_attribution["multiple_matches"] = True,
                pool_name_attribution["payout_addresses_matches"] = list(payout_address_matches)
                pool_name_attribution["coinbase_tag_matches"] = []
            return pool_name_attribution
        
        if not found_address_match and found_tag_match:
            name_matches = len(coinbase_tag_matches)
            name = list(coinbase_tag_matches)[0]
            if name_matches == 1:
                pool_name_attribution["pool_name"] = name
                pool_name_attribution["payout_addresses_matches"] = list(coinbase_tag_matches)
                pool_name_attribution["coinbase_tag_matches"] = []
                
                # If data is to be updated and there is only 1 payout address, 
                # -> add this address to my_data and local file.
                if update_data and len(payout_addresses) == 1:
                    payout_address = payout_addresses[0]
                    self.__update_my_pool_data(entry_key=payout_address, 
                                               record= {
                                                   "name": name
                                                }, 
                                               record_type='payout_addresses')
            else:
                # Multiple names matches found
                pool_name_attribution["pool_name"] = "Unknown"
                pool_name_attribution["multiple_matches"] = True,
                pool_name_attribution["payout_addresses_matches"] = []
                pool_name_attribution["coinbase_tag_matches"] = list(list(coinbase_tag_matches))    
            return pool_name_attribution
        
        if found_address_match and found_tag_match:
            address_matches = len(payout_address_matches)
            #tag_matches = len(payout_address_matches)
            if address_matches == 1:
                pool_name_attribution["pool_name"] = list(payout_address_matches)[0]
                pool_name_attribution["payout_addresses_matches"] = list(payout_address_matches)
                pool_name_attribution["coinbase_tag_matches"] = list(coinbase_tag_matches)  
            else:
                # Multiple names matches found
                pool_name_attribution["pool_name"] = "Unknown"
                pool_name_attribution["multiple_matches"] = True,
                pool_name_attribution["payout_addresses_matches"] = list(payout_address_matches)
                pool_name_attribution["coinbase_tag_matches"] = list(coinbase_tag_matches)
            return pool_name_attribution

        # First check for payout address match
        
        
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
        

        
    
    def AttributePoolName(self, coinbase_message, payout_addresses):
        
        results = {
            "coinbase_message": coinbase_message,
            "payout_addresses": payout_addresses,
            "0xB10C": self.getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.B10C_data,
                                               update_data = False),
            
            "Blockchain.com": self.getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.blockchain_data,
                                               update_data = False),
            
            "BTC.com": self.getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.btc_data,
                                               update_data = False),
            
            "Sjors Provoost": self.getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.sjors_data,
                                               update_data = False),
            
            "My Attribution": self.getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.my_pool_data,
                                               update_data = True)
            }
        
        return results

      
    