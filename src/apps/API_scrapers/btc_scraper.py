# -*- coding: utf-8 -*-
"""
A scraper module used to query data from the btc.com API.

@author: Mischa van Reede

Documentation link: https://btc.com/api-doc#API



Typical API json response, data field contains requested information:
  
    {
    "data": ...,
    "err_code": 0,
    "err_no": 0,
    "err_msg": null,
    "status": null
    }
    
TODO: when returning results, check if response was successfull 
(i.e. 
 "err_code": 200,
    "err_no": 200,
    "err_msg": "sucess",
    "status": "sucess"
 )

Response Types:
    
    Block:
        Block {
            height: int
            version: int
            mrkl_root: string
            curr_max_timestamp: int
            timestamp: int
            bits: int
            nonce: int
            hash: string
            prev_block_hash: string, null if not exists
            next_block_hash: string, null if not exists
            size: int
            pool_difficulty: int
            difficulty: int
            tx_count: int
            reward_block: int
            reward_fees: int
            created_at: int
            confirmations: int
            extras: {
                pool_name: string
                pool_link: string
                relayed_by: string
            }
        }
    Transaction:  (optional parameter: ?verbose= [1|2|3])
        Transaction {
            block_height: int
            block_time: int
            created_at: int
            fee: int
            hash: string
            inputs: [
                {
                    "prev_addresses": Array<String>
                    "prev_position": int
                    "prev_tx_hash": string
                    "prev_value": int
                    "script_asm": string, only when requesting specific transaction with verbose=2|3
                    "script_hex": string, only when requesting specific transaction with verbose=2|3
                    "sequence": int
                },
            ],
            inputs_count: int
            inputs_value: int
            is_coinbase: boolean
            lock_time: int
            outputs: [
                {
                    addresses: Array<String>
                    value: int
                }
            ],
            outputs_count: int
            outputs_value: int
            size: int
            version: int
        }
    Address:
        {
            address: string
            received: int
            sent: int
            balance: int
            tx_count: int
            unconfirmed_tx_count: int
            unconfirmed_received: int
            unconfirmed_sent: int
            unspent_tx_count: int
        }
        


-----
get specific block: https://chain.api.btc.com/v3/block/689034

all tx in block: https://chain.api.btc.com/v3/block/689034/tx

specific tx with script: https://chain.api.btc.com/v3/tx/7823fb18366d7b09afc87770b9f3fb6fee55288ef840e1fc9d8dc01d53eeff4e?verbose=3
"""

from .generic_rest_requests import RestRequests
from ..utils import Utils

class BtcScraper(RestRequests):
    
    def __init__(self, config, logger):
        
        self.config = config
        self.logger = logger
        self.base_url = "https://chain.api.btc.com/v3/"
        self.api_key = None
        
        
        # Initialize RestRequest object from parent class
        super().__init__(config=self.config, logger=self.logger)
        super().change_request_delay(delay=6)

# ====================================   
#  API query methods 
# ====================================
        
    def getLatestBlock(self):
        """
        Queries the BTC.COM API to obtain information on the latest mined block.
        The response is a json Block object as specified by the class documentation. 
        
        TODO check for orphan blocks at the latest block height
        
        Returns
        -------
        result : <class 'dict'>
            Dictionary containing information on the latest block mined. 
        """
        latest_block_url = self.base_url + "block/latest"
        result = self.get(latest_block_url)
        return result['data']
        
        
    def getBlock(self, block_hash):
        """
        Queries the BTC.COM API to obtain block information.

        Parameters
        ----------
        block_hash : string

        Returns
        -------
            Returns Block object as specified in class documentation
        """
        block_url = self.base_url + "block/" + block_hash
        result = self.get(block_url)
        return result['data'] 
    
    def getBlocksAtHeight(self, block_height):
        """
        Queries the BTC.COM API to obtain the block at a block_height.
        TODO: check for multiple blocks?
        Example: https://chain.api.btc.com/v3/block/1

        Parameters
        ----------
        block_height : int

        Returns
        -------
            Returns Block object as specified in class documentation
        """
        block_url = self.base_url + "block/" + block_height
        result = self.get(block_url)
        return result['data']
    
    def getBlockTransactions(self, block_hash, page=0):
        """
        BTC.COM API returns transactions in paginated form. 
                
        Parameters
        ----------
        block_hash : string
        page : int, optional

        Returns
        -------
        TYPE
            By default it returns the first(=0) page, with 10 transactions.
            For more transactions from a specific block, itterate over the pages.
            Example response: https://chain.api.btc.com/v3/block/latest/tx?page=0
        """
        block_transactions_url = self.base_url + "block/" + block_hash + "/tx"
        result = self.get(block_transactions_url)
        return result['data']
    
    def getTransaction(self, transaction_hash, verbose=3):
        transaction_url = self.base_url + "tx/" + transaction_hash + "?verbose=" + verbose
        result = self.get(transaction_url)
        return result['data']
    
    def getAddressInfo(self, bitcoin_address):
        address_url = self.base_url + "address/" + bitcoin_address
        result = self.get(address_url)
        return result['data']

# ====================================   
#  Data handler methods
# ====================================

    def getLatestBlockHeight(self):
        latest_block = self.getLatestBlock()
        height = latest_block['height']
        return height
  
    def getLatestBlockHash(self):
        latest_block = self.getLatestBlock()
        height = latest_block['hash']
        return height
    
    def getPrevBlockHash(self, block_hash):
        block = self.getBlock(block_hash)
        prev_block_hash = block['prev_block_hash']
        return prev_block_hash

    def getCoinbaseTransaction(self, block_hash):
        """
        Queries the BTC.COM API to get the coinbase transaction for a specific
        block.

        Parameters
        ----------
        block_hash : string

        Returns
        -------
        coinbase_transaction : Transaction object (dict)
        """
        # Get first tx hash from the block
        coinbase_transaction_hash = self.getBlockTransactions(block_hash)['list'][0]['hash']
        # Query tx again because this result cointains script msg info
        coinbase_transaction = self.getTransaction(coinbase_transaction_hash, verbose=3)
        return coinbase_transaction
    
    def getMiningPoolName(self, block_hash):
        
        # Obtain pool_name from block data (this API includes this information)
        pool_name_from_API = self.getBlock(block_hash)['extras']['pool_name']
        # Obtain pool_name from script in coinbase transaction
        pool_name_from_script = Utils.getMiningPoolNameFromHexScript(self.getCoinbaseTransaction(block_hash)['inputs'][0]['script_hex'])
        # Compare
        
        if self.poolNamesAreEqual(pool_name_from_API, pool_name_from_script):
            return pool_name_from_API
        
        else:
            self.logger.error("Pool name mismatch in BTC.COM API module.")
            self.logger.error("Pool names do not match: [{}] =/= [{}]".format(pool_name_from_API, pool_name_from_script))
            self.logger.error("Unsure which value to return.")
            self.logger.error("For now returning poolname from cb tx script: [{}]".format(pool_name_from_script))
            return pool_name_from_script

    def pruneUserTransactionsFromBlock(self, block):
        pass
    
    def poolNamesAreEqual(name1, name2):
        return name1 == name2