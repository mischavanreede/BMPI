# -*- coding: utf-8 -*-
"""
This file contains a utility class with methods that are used by the various modules of this application.

@author: Mischa van Reede
"""

import os
import sys
import json

from functools import wraps
from time import time


class Utils():
    

    def getCurrentPath():
        """
            get the current working path
        """
        return os.path.abspath(os.path.dirname(sys.argv[0]))
    
    
    def hexStringToAscii(hex_string):
        return bytes.fromhex(hex_string).decode('ascii', 'ignore')
    
   
    def btcToSats(bitcoin):
       # 1 btc = 100000000 sats
       return int(bitcoin*100000000)
   
    def getBlockReward(block_height):
       """
       returns the block reward in satoshis for any given height.
       https://en.bitcoin.it/wiki/Controlled_supply
       
       Works for blocks_heights up to 1680000.
       
       WORKS UNTIL APPROXIMATELY 2036.

       """
       
       if block_height < 210000:
           return Utils.btcToSats(50)
       elif block_height < 420000:
           return Utils.btcToSats(25)
       elif block_height < 630000:
           return Utils.btcToSats(12.5)
       elif block_height < 840000:
           return Utils.btcToSats(6.25)
       elif block_height < 1050000:
           return Utils.btcToSats(3.125)
       elif block_height < 1260000:
           return Utils.btcToSats(1.5625)
       elif block_height < 1470000:
           return Utils.btcToSats(0.78125)
       elif block_height < 1680000:
           return Utils.btcToSats(0.390625)
       else:
           print("Please update the getBlockReward method")
           print("Works for blocks_heights up to 1680000.")
           return -1
    
    
    def addAddressToPoolData(bitcoin_address, pool_name):
        pool_data_path = '../pools/pool_data.json'
        with open(file=pool_data_path, mode='r+', encoding='utf-8') as file:
            # load file into a dict
            file_data = json.load(file)
            # add address to pool data entry
            file_data['mining_pools'][pool_name]['pool_addresses'].append(bitcoin_address)
            # set file pointer to start of file
            file.seek(0)
            # write to file
            json.dump(file_data, file, indent=4)
    
    # def addCoinbaseTagToPoolData(coinbase_tag, pool_name):
    #     pool_data_path = '../pools/new_pool_data.json'
    #     with open(pool_data_path, 'r+') as file:
    #         # load file into a dict
    #         file_data = json.load(file)
    #         # add coinbase tag to pool data entry
            
            
            
    #         #file_data['mining_pools'][pool_name]['pool_addresses'].append(bitcoin_address)
            
            
            
    #         # set file pointer to start of file
    #         file.seek(0)
    #         # write to file
    #         json.dump(file_data, file, indent=4)
        
    
    def getPoolName(coinbase_message, payout_address, logger):
        f = open(file="../pools/pool_data.json", mode='r', encoding='utf-8')
        pool_data_json = json.load(f)
        f.close()
        tag_match = False
        address_match = False
        tag_match_name_list = []
        address_match_name_list = []
        
        logger.debug("Trying to find matches for:")
        logger.debug("Payout address: {}".format(payout_address))
        logger.debug("Coinbase message: {}".format(coinbase_message))
        
        # loop over pool data and keep track of matches
        for name in pool_data_json['mining_pools']:
            
            #look for coinbase tag match
            for coinbase_tag in pool_data_json['mining_pools'][name]['coinbase_tags']:          

                # If match is found
                if coinbase_tag in coinbase_message:
                    logger.debug("Found a matching coinbase tag!")
                    logger.debug("Match found for pool: {}".format(name))
                    tag_match = True
                    tag_match_name_list.append(name)
                    # break out of coinbase_tag loop when match is found, 
                    # no need to try and match multiple tags from same pool.
                    break 
            
            #look for payout address match
            pool_payout_addresses = pool_data_json['mining_pools'][name]['pool_addresses']

            if payout_address in pool_payout_addresses:
                logger.debug("Found a matching pool payout address!")
                logger.debug("Match found for pool: {}".format(name))
                address_match = True
                address_match_name_list.append(name)
        
        if tag_match and address_match:
            # Two matches, returning pool_name derived from addres match, 
            # no further action needed.
            if len(address_match_name_list) == 1:
                return address_match_name_list[0]
            else:
                logger.warning("Multiple pool_name matches found")
                logger.warning("tag_match matches: {}".format(tag_match_name_list))
                logger.warning("address_match matches: {}".format(address_match_name_list))
                logger.warning("Please make sure there are no duplicate addresses in pool_data.json")
                logger.warning("Returning first entry for now.")
            return address_match_name_list[0]
        
        elif tag_match and not address_match:
            # Tag matches a pool message, but address doesn't, 
            # add address to known payout addresses and return pool_name
            # derived from the tag match
            if len(tag_match_name_list) == 1:
                logger.info("Found a new address for pool: {}".format(tag_match_name_list[0]))
                Utils.addAddressToPoolData(payout_address, tag_match_name_list[0])
                return tag_match_name_list[0]
            else:
                logger.warning("Multiple pool_name matches found in coinbase message: {}".format(coinbase_message))
                logger.warning("Matches found: {}".format(tag_match_name_list))
                logger.warning("Not sure to which pool to add new address.")
                logger.warning("Returning first entry for now.")
            return tag_match_name_list[0]
            
        elif not tag_match and address_match:
            # No tag match, but address does match,  
            # This means a (potential) new message is in the block
            # add message to logs for manual inspection, return pool_name
            if len(address_match_name_list) == 1:
                logger.debug("Did not find a match with pool tags.")
                logger.debug("Found match with the payout address of pool: [{}]".format(address_match_name_list[0]))
                return address_match_name_list[0]
            else:
                logger.warning("Multiple pool_name matches found: {}".format(address_match_name_list))
                logger.warning("Please make sure there are no duplicate addresses in pool_data.json")
                logger.warning("Returning first entry for now.")
            return address_match_name_list[0]
    
        else: # no tag_match and no address_match
            logger.debug("No matches found. Returning pool_name: Unknown")
            return "Unknown"       
            
    def prettyPrint(dictionary):
        print(json.dumps(dictionary, indent=2))
        return 

    def prettyFormat(dictionary):
        return json.dumps(dictionary, indent=2)
    
    def removeNonAscii(string):
        encoded_string = string.encode("ascii", "ignore")
        decode_string = encoded_string.decode()
        return(decode_string)
    
    def populatePoolDataJSON(logger):
        with open(file='../pools/pool_data_merged.json', mode='r', encoding='utf-8') as file:
            combined_pool_data_json = json.load(file)
            
            data = {
                "coinbase_tags" : {},
                "payout_addresses" : {}
            }

            for pool_name in combined_pool_data_json['mining_pools']:
                #populate coinbase tags key
                for tag in combined_pool_data_json['mining_pools'][pool_name]['coinbase_tags']:
                    try: 
                        if data['coinbase_tags'][tag]:
                            logger.warning("coinbase_tag already present.")
                            logger.warning("{} :: name: {}".format(data['coinbase_tags'][tag], data['coinbase_tags'][tag]['name']))        
                    except KeyError:
                        logger.info("Adding new tag to JSON: {}".format(tag))
                        data['coinbase_tags'][tag] = {'name' : pool_name}
                            
                #populate payout address key
                for address in combined_pool_data_json['mining_pools'][pool_name]['pool_addresses']:
                    try: 
                        if data['coinbase_tags'][address]:
                            logger.warning("payout_address already present.")
                            logger.warning("{}, {}".format(data['payout_addresses'][address], data['payout_addresses'][address]['name']))       
                    except KeyError:
                        logger.info("Adding new address to JSON: {}".format(address))
                        data['payout_addresses'][address] = {'name' : pool_name}
                        
            #write to new datafile
            with open('../pools/pool_data.json', 'w', encoding='utf-8') as outfile:
                json.dump(data, outfile, indent=4, sort_keys=True)
        print("Done populating pool_data.json")
    
    

    def printTiming(f):
        """
        Decorator used to measure time of a method f.
        Usage: Add @Utils.printTiming in front of a method.
        """
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time()
            result = f(*args, **kwargs)
            end = time()
            print('Elapsed time: {}'.format(end-start))
            return result
        return wrapper
   
    # def updateCoinbaseTags():
    #     #open new file
    #     new_file = open(file='../pools/pool_data.json', mode='r+' , encoding='utf-8')
    #     new_file_data = json.load(new_file)
    #     #open old file
    #     old_file = open(file='../pools/old_pool_data.json', mode='r' , encoding='utf-8')
    #     old_file_data = json.load(old_file)
        
    #     # empty coinbase tags
    #     for name in new_file_data['mining_pools']:
    #         #set coinbase tag to empty list
    #         new_file_data['mining_pools'][name]['coinbase_tags'] = []
        
    #     # Fill coinbase list of new file with tags
    #     for coinbase_tag in old_file_data['coinbase_tags']:
    #         pool_name = old_file_data['coinbase_tags'][coinbase_tag]['name']
    #         new_file_data['mining_pools'][pool_name]['coinbase_tags'].append(coinbase_tag)
        
    #     # set file pointer to start of file
    #     new_file.seek(0)
    #     # write to data to new file
    #     json.dump(new_file_data, new_file, indent=4)
    #     #close files
    #     new_file.close()
    #     old_file.close()

        
    # def invertPoolDataJson():
    #     f = open('../pools/pool_data.json', encoding='utf-8')
    #     json_file = json.load(f)
        
    #     data = {"mining_pools": {}}        
        
    #     for coinbase_tag in json_file['coinbase_tags']:
    #         pool_name = json_file['coinbase_tags'][coinbase_tag]['name']
    #         pool_addresses = []            
            
    #         for btc_address in json_file['payout_addresses']:
    #             if pool_name == json_file['payout_addresses'][btc_address]['name']:
    #                 pool_addresses.append(btc_address)
            
    #         entry = {
    #             pool_name: {
    #                 "coinbase_tag": coinbase_tag,
    #                 "link": json_file['coinbase_tags'][coinbase_tag]['link'],
    #                 "pool_addresses": pool_addresses
    #                 } 
    #             }
    #         data['mining_pools'].update(entry)        
    #     f.close()
    #     with open('../pools/new_pool_data.json', 'w') as outfile:
    #         json.dump(data, outfile, indent=4, sort_keys=True)
        
    #     print("Inverting json complete.")
        
        
        