# -*- coding: utf-8 -*-
"""


@author: Mischa van Reede
"""

import json
from .utils import Utils

class BlockAnalyser():
    
      
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config
        
        # Load json file data into memory
        
        # Known pools data base path
        base_path = "../known-pools/"
    
        # External known pools file data, sources are in known-pools/sources
        B10C_known_pools_file_path =            base_path + "data/0xB10C.json"
        blockchain_com_known_pools_file_path =  base_path + "data/blockchain-com.json"
        btc_com_known_pools_file_path =         base_path + "data/btc-com.json"
    
        # My pool data1
        my_pools_file_path =  base_path + "updated_pools.json"
        my_updated_pools_file_path =  base_path + "updated_pools.json"
        
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
        my_pool_data = {
                "name": "My Data",
                "data": self.__load_json(my_pools_file_path)
            }
        my_pool_data_updated = {
                "name": "My Updated Data",
                "data": self.__load_json(my_updated_pools_file_path)
            }
        
        # All data from external sources
        self.B10C_data = B10C_data
        self.blockchain_data = blockchain_data
        self.btc_data = btc_data

        # My known-pools data.
        self.my_pool_data = my_pool_data
        self.my_pool_data_updated = my_pool_data_updated
        
        self.logger.info("BlockAnalyser object initialized.")
        
    def __load_json(self, file_path):
        self.logger.debug("Trying to load [{}] file into memory.".format(file_path))  
        try:
            with open(file_path, mode='r', encoding="utf8") as file:
                data = json.load(file)
            self.logger.debug("File loaded, returning data.")
            return data
        except IOError:
            self.logger.error("Failed to load file.")
            return None
        
    def __update_my_pool_data(self, entry_key, record, record_type):

        my_data_updated = False
 
        if record_type == "payout_addresses":
            
            if entry_key not in self.my_pool_data['data']["payout_addresses"]:
                self.logger.info("Adding payout address [{}] to pools.json for pool {}.".format(entry_key, record['name']))
                self.my_pool_data['data']["payout_addresses"][entry_key] = record
                my_data_updated = True
                
            else:
                self.logger.debug("Entry {} already exists".format(entry_key))
                return
            
        elif record_type == "coinbase_tags":
            
            if entry_key not in self.my_pool_data['data']["coinbase_tags"]:
                self.logger.info("Adding coinbase_tag {} to my_pool_data.".format(entry_key))
                self.my_pool_data['data']["coinbase_tags"][entry_key] = record
                my_data_updated = True
                
            else:
                self.logger.debug("Entry {} already exists".format(entry_key))
                return
            
        else:
            self.logger.error("Record type not supported: {}".format(record_type))
            return
               
        if my_data_updated:
            self.logger.info("Writing changes to known-pools/pools.json file")
            current_path = Utils.getCurrentPath()
            with open(current_path + '/../known-pools/pools.json', mode='w', encoding='utf-8') as outfile:
                json.dump(self.my_pool_data['data'], outfile, sort_keys=True, indent=4)
                self.logger.info("known-pools/pools.json file updated.")
        return
    
    def __getPoolNameFromData(self, coinbase_message, payout_addresses, pool_data, update_data):
        
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
        
        self.logger.debug("Finding pool name using data from: {}".format(pool_data['name']))
        pool_data = pool_data['data']
        
        # Find matches
        #self.logger.debug("Looking for payout address matches.")
        for payout_address in payout_addresses:
            if payout_address in pool_data['payout_addresses']:
                found_address_match = True
                pool_name = pool_data["payout_addresses"][payout_address]["name"]
                payout_address_matches.add(pool_name)
        
        #self.logger.debug("Looking for coinbase tag matches.")
        for tag in pool_data["coinbase_tags"]:
            
            if tag in coinbase_message:
                found_tag_match = True
                pool_name = pool_data["coinbase_tags"][tag]["name"]
                coinbase_tag_matches.add(pool_name)
                
        # Handle matches
        if not found_address_match and not found_tag_match:
            # No match found, pool name "Unknown"
            self.logger.debug("Found no matches.")
            pool_name_attribution["pool_name"] = "Unknown"
            pool_name_attribution["payout_addresses_matches"] = []
            pool_name_attribution["coinbase_tag_matches"] = []
            return pool_name_attribution
               
        if found_address_match and not found_tag_match:
            self.logger.debug("Found payout address match(es), found no tag matches")
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
            self.logger.debug("Found no payout address matches, found tag match(es)")
            name_matches = len(coinbase_tag_matches)
            name = list(coinbase_tag_matches)[0]
            if name_matches == 1:
                pool_name_attribution["pool_name"] = name
                pool_name_attribution["payout_addresses_matches"] = []
                pool_name_attribution["coinbase_tag_matches"] = list(coinbase_tag_matches)
                
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
            self.logger.debug("Found payout address and tag match(es)")
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
     
    
    def updateMyPoolData(self, coinbase_message, payout_addresses, update_data):
        # Wrapped function call to call this method from outside and use only my_pool_data variable
        # Should update pools.json if new payout address is encoutered and update_data is True.
        _ = self.__getPoolNameFromData(coinbase_message, payout_addresses, self.my_pool_data, update_data)
        return     
    
    def AttributePoolName(self, coinbase_message, payout_addresses):
        self.logger.debug("Attributing pool name using various pool_data files.")
        results = {
            "coinbase_message": coinbase_message,
            "payout_addresses": payout_addresses,
            "0xB10C": self.__getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.B10C_data,
                                               update_data = False),
            
            "Blockchain_com": self.__getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.blockchain_data,
                                               update_data = False),
            
            "BTC_com": self.__getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.btc_data,
                                               update_data = False),
                        
            "My_initial_attribution": self.__getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.my_pool_data,
                                               update_data = False),
            
            "My_updated_attribution": self.__getPoolNameFromData(coinbase_message = coinbase_message,
                                               payout_addresses = payout_addresses,
                                               pool_data = self.my_pool_data_updated,
                                               update_data = False)
            
            }
        self.logger.debug("Attribution complete.")
        return results

      
    