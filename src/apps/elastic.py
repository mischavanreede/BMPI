# -*- coding: utf-8 -*-
"""
A class file used to interact with the elasticsearch instance.

@author: Mischa van Reede


https://www.youtube.com/watch?v=ma3BC8aPBfE

https://www.youtube.com/watch?v=wGDQq1ax7uI

https://www.youtube.com/watch?v=hzRP48OQsxE

Elasticsearch mapping information: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html

"""

#ES stores json documents.


import sys
import time
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search




class ElasticsearchController():
    
    
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.es_connection = self.__connect_elasticsearch()
        
        self.__create_all_indices()
        
        
    def __connect_elasticsearch(self):
        """
        Connects to an Elasticsearch instance using the specified values from the config file.

        Returns
        -------
        es : Elasticsearch Object connected to running ES instance

        """        
        es_host = self.config.get('Elasticsearch', 'elasticsearch_host')
        es_port = self.config.get('Elasticsearch', 'elasticsearch_port')
        credentials = {'http_auth': (self.config.get('Elasticsearch', 'user'), self.config.get('Elasticsearch', 'password'))}
                
        es_connection = Elasticsearch([{'host': es_host, 'port': es_port}], **credentials)
        
        if es_connection.ping():
            self.logger.debug(
                "Connected to Elasticsearch instance at: {host}/{port}".format(host=es_host,port=es_port))       
        else:
            self.logger.error(
                "Could not connect to Elasticsearch at: {host}/{port}".format(host=es_host,port=es_port))  
            sys.exit(1)
            
        return es_connection
    
    
    def __create_all_indices(self):
        """
        Attempts to create all specified indices in the elasticsearch instance. Skips indices that already exist.
    
        Returns
        -------
        None.

        """
        index_names = ElasticsearchIndexes.INDEX_NAMES
        self.logger.info("Checking if the following indices need to be created: {}".format(index_names))
        
        for index in index_names:
            # Create needed databases/indices
            if not self.__index_exists(index):
                body = {**ElasticsearchIndexes.SETTINGS, **ElasticsearchIndexes.MAPPINGS[index]}
                self.__create_index(index_name=index, index_body=body)
                continue
            self.logger.info("Index {} already exists.".format(index))
        self.logger.info("All indices have been created.")
    
        
    
    #============================================
    # Indices
    #============================================    
    
    def __create_index(self, index_name, index_body):
        """
            Create index and mapping for index Crawl Peer Info

        Returns
        -------
        created : Boolean
            Return True if index was succesfully created, or when the index already existed.

        """
        try:
            # Ignore 400 means to ignore "Index Already Exist" error.
            self.logger.info("Creating index: [{}]".format(index_name))
            self.es_connection.indices.create(index=index_name, body=index_body) # self.es.indices.create(index=index_name, ignore=400, body=settings)
            #self.es_connection.indices.create(index=index_name)
            self.logger.info("Index created: [{}]".format(index_name))

        except Exception as e:
            self.logger.error("Error on index creation : {}".format(str(e)))
        finally:
            return
    
    def __index_exists(self, index_name):
        # Check whether or not the specified index exists in the ES instance.
               
        try:
            if self.es_connection.indices.exists(index=index_name):
                #self.logger.debug("Index with name [{}] exists.".format(index_name))
                return True
            self.logger.info("Index with name [{}] was not found.".format(index_name))
            return False
        except elasticsearch.exceptions.NotFoundError as e:
            self.logger.info("Index with name [{}] was not found.".format(index_name))
            self.logger.error("Error message: {}".format(e))
            return False
        except Exception as e:
            self.logger.error("An error has occurred: {}".format(e))
        
        
    def delete_index(self, index_name):
        # Delete specified index from ES instance, maybe add { ignore=[400, 404] }  as argument if there are errors.
        self.logger.info("Trying to delete index [{}]".format(index_name))
        if self.__index_exists(index_name):
            try: 
                self.es_connection.indices.delete(index=index_name)
                self.logger.info("Index with name [{}] deleted".format(index_name))
                return
            except Exception as e:
                self.logger.error("An error has occurred: {}".format(e))
        else:
            self.logger.debug("The index [{}] does not exist".format(index_name))
            return
    
    #============================================
    # Methods for interacting with the instance
    #============================================ 
    def store(self, record, index_name):
        """
            Store a data record in a specified index index

        Parameters
        ----------
        record : dict / json object
            Data to be stored in the Elasticsearch Database.
        index_name : string
            Name of the index under which the data record should be stored.
            
        Returns
        -------
        is_stored : Boolean
            True if record is stored succesfully.
        """
        assert(self.__index_exists(index_name))
                    
        try_count = 0
        max_tries = 5
        while True:
            try:
                self.logger.debug("Storing object in index: {}".format(index_name))
                self.es_connection.index(index=index_name, body=record)
                #self.logger.debug("Storing object: {}".format(outcome))
                is_stored = True
                break
            except Exception as ex:
                self.logger.error("An error occurred when storing the document: {}".format(str(ex)))
                #self.logger.error("Error for record : {}".format(record))
                
                time.sleep(1)
                try_count += 1
                self.logger.info("Trying to store data again. Attempt {} out of {}".format(try_count, max_tries))
                if try_count >= 5:
                    self.logger.error("All {} attemps to store the record failed. Skip trying to store this record".format(max_tries))
                    is_stored = False
                    break
        return is_stored
    
    def bulk_store(self, records, index_name):
        """
        Stores many records in one go.

        """
        assert(self.__index_exists(index_name))
        
        action = [
            {
                "_index": index_name,
                "_source": record
            }
            for record in records
        ]
        
        try_count = 0
        max_tries = 5
        while True:
            try:
                self.logger.info("Storing [{}] records in index: [{}]".format(len(records), index_name))
                elasticsearch.helpers.bulk(self.es_connection, actions=action)
                is_stored = True
                self.logger.debug("Storing records was succesful")
                break
            except Exception as ex:
                self.logger.error("An error occurred when storing the documents: {}".format(str(ex)))
                time.sleep(1)
                try_count += 1
                self.logger.info("Trying to store data again. Attempt {} out of {}".format(try_count, max_tries))
                if try_count >= max_tries:
                    self.logger.error("All {} attemps to store the record failed.".format(max_tries))
                    is_stored = False
                    break
        return is_stored
    
    
    def remove_all_but_one_by_query(self, index, query):
        self.logger.debug("Deleting duplicate records from index {} that match with the following query: \"{}\"".format(index, query))
        
        search_context = Search(using=self.es_connection, index=index)
        s = search_context.query('query_string', query=query)
        # Count all results
        total = s.count()
        # Set size to total count-1 to delete all but one docs
        if total > 1:
            self.logger.debug("Found {} documents".format(total))
            single_result = next(s.scan()).to_dict()
            # Delete by query
            self.logger.debug("Deleting all documents")
            response = s.delete()
            self.logger.debug("Delete response: {}".format(response))
            self.logger.info("Documents successfully deleted.")
            self.logger.debug("Reuploading single document.")
            self.store(single_result, index)
            self.logger.debug("Done storing document.")
        else:
            self.logger.debug("Only 1 document returned with query: \"{}\"".format(query))
  

    def query_all_docs(self, index):
        self.logger.debug("Querying index {} to obtain all records using .scan()".format(index))
        query = "*"
        search_context = Search(using=self.es_connection, index=index)
        s = search_context.query('query_string', query=query)
        self.logger.debug("Found a total of {} records".format(s.count()))
        results = [doc.to_dict() for doc in s.scan()]
        return results
    
    
    def query_all_docs_with_metadata(self, index):
        self.logger.debug("Querying index {} to obtain all records using .scan()".format(index))
        query = "*"
        search_context = Search(using=self.es_connection, index=index)
        s = search_context.query('query_string', query=query)
        self.logger.debug("Found a total of {} records".format(s.count()))
        results = []
        for doc in s.scan():
            result = doc.meta.to_dict()
            result["data"] = doc.to_dict()
            results.append(result)
        
        #results = [doc.meta.to_dict().update({"_source": doc.to_dict()}) for doc in s.scan()]
        if len(results) > 0:
            self.logger.debug("Document structure (dict keys): {}".format(results[0].keys()))
        else:
            self.logger.dubug("No results returned.")
        return results
    
    def query_es(self, index=None, query=None, max_results=0):
        self.logger.debug("Querying ES instance.")
        search_context = Search(using=self.es_connection, index=index)
        s = search_context.query('query_string', query=query)
        
        if max_results >= 0:
            #count total search results
            total = s.count()
        else:
            total = max_results
        self.logger.debug("Returning up to {} results".format(total))
        #set upper limit for results
        s = s[0:total]
        response = s.execute()
        if response.success():
            # Response in panda DF
            #df = pd.DataFrame((document.to_dict() for document in s.scan()))
            #return df
            # Response in list
            #return response['hits']['hits']
            # Complete response
            self.logger.debug("ES Query [{}] on index [{}] successful. Returning response object.".format(query, index))
            self.logger.debug("Found a total of {} results".format(response.hits.total))
            return response['hits']['hits']
        
        self.logger.error("ES Query failed. Returning None.")
        return None
    
    def delete_doc(self, index, doc_type, doc_id):
        """
        Delete document with doc_id from specified index.

        """
        self.logger.debug("Deleting doc with id {} from index {}".format(doc_id, index))
        self.es_connection.delete(index=index, id=doc_id, doc_type=doc_type)
        self.logger.debug("Doc deleted.")
    
    
    def reindex_data(self, source_index, dest_index):
        assert(self.__index_exists(source_index) and self.__index_exists(dest_index))
        self.logger.info("Reindexing data from index [{}] to [{}]".format(source_index, dest_index))
        result = self.es_connection.reindex({
            "source": {"index": source_index},
            "dest": {"index": dest_index}
            }, wait_for_completion=True, request_timeout=300)
        self.logger.debug("Result:  {}".format(result))
        
        if result['total'] and result['took'] and not result['timed_out']:
            self.logger.debug("Seems reindex was successfull. You can now delete index [{}]".format(source_index))
        else:
            self.logger.debug("Something went wrong.")
            
       
    def isConnected(self):
        connected = False
        self.logger.debug("Checking if es_connection is connected to an Elasticsearch instance.")
        if self.es_connection.ping():
            self.logger.debug("The object es_connection is connected to an Elasticsearch instance.")
            connected = True
            return connected
        self.logger.error("There is no connection to the elasticsearch instance.")
        self.logger.error("Is the elasticsearch instance running?")
        return connected
    


class ElasticsearchIndexes():
    
    INDEX_NAMES = ["blocks_from_scrapers_updated", 
                   "skipped_blocks", 
                   "api_block_data_conflicts", 
                   "block_attributions"]
    
    SETTINGS = {
            "settings" : {
    	        "number_of_shards": 5,
    	        "number_of_replicas": 0
            }
        }
    # 0 index replicas because there is only 1 ES node
    #https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html

    MAPPINGS = {
        # "blocks_from_scrapers": {
        #     "mappings": {
        #         "properties": {
        #             "block_hash": {"type": "text"},
        #             "prev_block_hash": {"type": "text"},
        #             "block_height": {"type": "integer"},
        #             "timestamp" : {"type": "date"},
        #             "coinbase_tx_hash": {"type": "text"},
        #             "coinbase_message": {"type": "text"},
        #             "payout_addresses": {"type": "text"},
        #             "fee_block_reward": {"type": "integer"}, 
        #             "total_block_reward": {"type": "integer"}
        #             } 
        #         }
        #     },
        "blocks_from_scrapers_updated": {
            "mappings": {
                "properties": {
                    "block_hash": {"type": "text"},
                    "prev_block_hash": {"type": "text"},
                    "block_height": {"type": "integer"},
                    "timestamp" : {"type": "date"},
                    "coinbase_tx_hash": {"type": "text"},
                    "coinbase_message": {"type": "text"},
                    "payout_addresses": {"type": "text"},
                    "fee_block_reward": {"type": "long"}, 
                    "total_block_reward": {"type": "long"}
                    } 
                }
            },
        "skipped_blocks": {
            "mappings": {
                "properties": {
                     "block_height": {"type": "integer"},
                     "block_hash": {"type": "text"},
                     "reason_for_skipping": {"type": "text"}
                    }                
                }
            },
        "api_block_data_conflicts": {
            "mappings": {
                "properties": {
                    "block_height": {"type": "integer"},
                    "block_hash": {"type": "text"},
                    "gathered_information": {
                            "type": "nested",
                            "properties": {
                                    "scraper": {"type": "text"},
                                    "gathered_block": {"type": "flattened"}
                                }       
                        }
                    }                
                }
            },
        "block_attributions": {
            "mappings": {
                "properties": {
                    "run_id": {
                        "type": "text"},
                    "block_height": {
                        "type": "integer"},
                    "block_hash": {
                        "type": "text"},
                    "timestamp" : {
                        "type": "date"},
                    "coinbase_message": {
                        "type": "text"},
                    "payout_addresses": {
                        "type": "text"},
                    "fee_block_reward": {
                        "type": "long"}, 
                    "total_block_reward": {
                        "type": "long"},           
                    "0xB10C_results": {
                        "type": "nested",
                        "properties": {
                            "pool_name": {"type": "text" },
                            "multiple_matches": {"type": "boolean"},
                            "payout_addresses_matches": {"type": "text"},
                            "coinbase_tag_matches": {"type": "text"}        
                            }       
                        },
                    "0xB10C_attribution" : {
                        "type": "keyword"
                        },
                    "Blockchain_com_results": {
                        "type": "nested",
                        "properties": {
                            "pool_name": {"type": "text" },
                            "multiple_matches": {"type": "boolean"},
                            "payout_addresses_matches": {"type": "text"},
                            "coinbase_tag_matches": {"type": "text"}        
                            }       
                        },
                    "Blockchain_com_attribution" : {
                        "type": "keyword"
                        },
                    "BTC_com_results": {
                        "type": "nested",
                        "properties": {
                            "pool_name": {"type": "text" },
                            "multiple_matches": {"type": "boolean"},
                            "payout_addresses_matches": {"type": "text"},
                            "coinbase_tag_matches": {"type": "text"}        
                            }       
                        },
                    "BTC_com_attribution" : {
                        "type": "keyword"
                        },
                    "My_inital_results": {
                        "type": "nested",
                        "properties": {
                            "pool_name": {"type": "text" },
                            "multiple_matches": {"type": "boolean"},
                            "payout_addresses_matches": {"type": "text"},
                            "coinbase_tag_matches": {"type": "text"}        
                            }       
                        },
                    "My_initial_attribution" : {
                        "type": "keyword"
                        },
                    "My_updated_results": {
                        "type": "nested",
                        "properties": {
                            "pool_name": {"type": "text" },
                            "multiple_matches": {"type": "boolean"},
                            "payout_addresses_matches": {"type": "text"},
                            "coinbase_tag_matches": {"type": "text"}        
                            }       
                        },
                    "My_updated_attribution" : {
                        "type": "keyword"
                        }                  
                    }                
                }
            }


        }
    

    
    