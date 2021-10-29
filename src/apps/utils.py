# -*- coding: utf-8 -*-
"""
This file contains a utility class with methods that are used by the various modules of this application.

@author: Mischa van Reede
"""

import os
import sys
import json

import hashlib
import base58

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
    
    
    def hash160(hex_str):
        sha = hashlib.sha256()
        rip = hashlib.new('ripemd160')
        sha.update(hex_str)
        rip.update( sha.digest() )
        #print ( "key_hash = \t" + rip.hexdigest() )
        return rip.hexdigest()  # .hexdigest() is hex ASCII
    
    def bitcoin_address_from_pub_key(pub_key, logger):
        # https://gist.github.com/circulosmeos/ef6497fd3344c2c2508b92bb9831173f
        # https://en.bitcoin.it/wiki/Protocol_documentation#Addresses
        assert( isinstance(pub_key, str))
        #compress_pubkey = False
        
        prefix = pub_key[:2]
        
        if prefix == '02':
            hex_str = bytearray.fromhex(pub_key)
        elif prefix == '03':
            hex_str = bytearray.fromhex(pub_key)
        elif prefix == '04':
            hex_str = bytearray.fromhex(pub_key)
        else:
            logger.warning("Public key {} does not contain the right prefix".format(pub_key))
            return
        
        
        # if (compress_pubkey):
        #     if (ord(bytearray.fromhex(pub_key[-2:])) % 2 == 0):
        #         pub_key_compressed = '02'
        #     else:
        #         pub_key_compressed = '03'
        #     pub_key_compressed += pub_key[2:66]
        #     hex_str = bytearray.fromhex(pub_key_compressed)
            

        # Obtain key:
        key_hash = '00' + Utils.hash160(hex_str)
        
        # Obtain signature:
        sha = hashlib.sha256()
        sha.update( bytearray.fromhex(key_hash) )
        checksum = sha.digest()
        sha = hashlib.sha256()
        sha.update(checksum)
        checksum = sha.hexdigest()[0:8]
        
        #print ( "checksum = \t" + sha.hexdigest() )
        #print ( "key_hash + checksum = \t" + key_hash + ' ' + checksum )
        #print ( "bitcoin address = \t" + (base58.b58encode( bytes(bytearray.fromhex(key_hash + checksum)) )).decode('utf-8') )
        return (base58.b58encode( bytes(bytearray.fromhex(key_hash + checksum)) )).decode('utf-8')

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
    
    def combineKnownPoolDataFiles(logger):
        # Keep the weird characters of F2Pool in mind,
        # maybe change encoding when opening files.
        logger.info("Combining pool data into combined_data.json")
        logger.debug("Loading json file data.")
        logger.debug("Current path: {}".format(Utils.getCurrentPath()))
        
        cur_path = os.path.dirname(__file__)
        
        with open(cur_path + '/../../known-pools/data/blockchain-com.json', mode='r', encoding='utf-8') as blockchain_com_file:
            blockchain_com_json = json.load(blockchain_com_file)
            
        with open(cur_path + '/../../known-pools/data/btc-com.json', mode='r', encoding='utf-8') as btc_com_file:
            btc_com_json = json.load(btc_com_file)
            
        with open(cur_path + '/../../known-pools/data/0xB10c.json', mode='r', encoding='utf-8') as B10c_file:
            B10c_json = json.load(B10c_file)
            
        with open(cur_path + '/../../known-pools/data/sjors.json', mode='r', encoding='utf-8') as sjors_file:
            sjors_json = json.load(sjors_file)
            
        logger.debug("File data loaded.")
        json_data = [blockchain_com_json, btc_com_json, B10c_json, sjors_json]
        my_json = {
            "coinbase_tags" : {
                },
            "payout_addresses" : {
                }
            }
        # Add coinbase tags to my_json
        logger.debug("Adding coinbase tags.")
        for data in json_data:
            for cb_tag in data['coinbase_tags']:

                if cb_tag not in my_json['coinbase_tags']:
                    my_json['coinbase_tags'][cb_tag] = data['coinbase_tags'][cb_tag]
                else:
                    #tag is already in my_json, check if pool name matches
                    name1 = my_json['coinbase_tags'][cb_tag]['name']
                    name2 = data['coinbase_tags'][cb_tag]['name']
                    if not name1 == name2:
                        #if the name is not equal
                        logger.error("Couldn't match names for tag: {}".format(cb_tag))
                        logger.info("Name 1: {} , Name 2: {}".format(name1, name2))
                        # Remove entry from my_json
                        my_json['coinbase_tags'].pop(cb_tag)
                    #Else if they match, continue
        
        logger.info("Finished adding coinbase tags.")
       
        # Add addresses to my_json if they appear in all files
        logger.info("Adding pool addresses") 
        addr_intersection = [addr for addr in json_data[0]['payout_addresses'] if all([addr in d['payout_addresses'] for d in json_data[1:]])]
           
        logger.info("Found {} matching addresses".format(len(addr_intersection)))
        logger.info("Found the following addresses in every jsonfile: {}".format(addr_intersection))
        # Check if pool names are equal
        for addr in addr_intersection:
            pool_names = []
            for data in json_data:
                pool_names.append(data['payout_addresses'][addr]['name'])
            # If names are equal add record to my_json
            if len(set(pool_names)) == 1:
                my_json['payout_addresses'][addr] = json_data[0]['payout_addresses'][addr]
            else:
                logger.error("Found multiple pool names for addr: {}".format(addr))
                logger.info("Names: {}".format(pool_names))       
        logger.info("Finished adding pool payout addresses.")
        
        logger.info("Writing data to file.")
        with open(cur_path + '/../../known-pools/pools.json', mode='w', encoding='utf-8') as outfile:
            json.dump(my_json, outfile, sort_keys=True, indent=4)
        logger.info("Done writing data to file.")
   
    def load_json_file(file_path, logger):
        logger.debug("Trying to load [{}] file into memory.".format(file_path))  
        try:
            with open(file_path, mode='r', encoding="utf8") as file:
                data = json.load(file)
            logger.debug("File loaded, returning data.")
            return data
        except IOError:
            logger.error("Failed to load file.")
            return None
        