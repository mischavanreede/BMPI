# -*- coding: utf-8 -*-
"""
A class file used to interact with the elasticsearch instance.

@author: Mischa van Reede


https://www.youtube.com/watch?v=ma3BC8aPBfE

https://www.youtube.com/watch?v=wGDQq1ax7uI

https://www.youtube.com/watch?v=hzRP48OQsxE

"""

#ES stores json documents.


import sys
import time
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Search

import pandas as pd



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
        TODO: Add settings for each index
    
        Returns
        -------
        None.

        """
        index_names = ["first_index"]
        self.logger.info("Checking if the following indices need to be created: {}".format(index_names))
        
        for index in index_names:
            # Create needed databases/indices
            if not self.__index_exists(index):
                self.__create_index(index_name=index, settings = None)        
        self.logger.info("All indices have been created.")
    
    #============================================
    # Methods for interacting with the instance
    #============================================ 
    def store(self, record, index_name, doc_type=None):
        """
            Store a data record in a specified index index

        Parameters
        ----------
        record : dict / json object
            Data to be stored in the Elasticsearch Database.
        index_name : string
            Name of the index under which the data record should be stored.
        doc_type : string
            Optional name that specifies the type of data record

        Returns
        -------
        is_stored : Boolean
            True if record is stored succesfully.
        """            
        try_count = 0
        max_tries = 5
        while True:
            try:
                outcome = self.es_connection.index(index=index_name, doc_type=doc_type, body=record)
                self.logger.debug("Storing object : {}".format(outcome))
                is_stored = True
                break
            except Exception as ex:
                self.logger.error("Error on storing data : {}".format(str(ex)))
                self.logger.error("Error for index : {}".format(index_name))
                self.logger.error("Error for record : {}".format(record))
                
                time.sleep(1)
                try_count += 1
                self.logger.info("Trying to store data again. Attempt {} out of {}".format(try_count, max_tries))
                if try_count >= 5:
                    self.logger.error("All {} attemps to store the record failed. Skip trying to store this record".format(max_tries))
                    is_stored = False
                    break
        return is_stored
    
    def bulk_store(self, records, index_name, doc_type):
        """
        Stores many records in one go.

        Parameters
        ----------
        records : TYPE
            DESCRIPTION.
        index_name : TYPE
            DESCRIPTION.
        doc_type : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        pass

    
    
    def query_es(self, index, query, doc_type):
        search_context = Search(using=self.es_connection, index=index, doc_type=doc_type)
        s = search_context.query('query_string', query=query)
        response = s.execute()
        if response.success():
            # Response in panda DF
            #df = pd.DataFrame((document.to_dict() for document in s.scan()))
            #return df
            # Response in list
            #return response['hits']['hits']
            # Complete response
            self.logger.debug("ES Query [{}] on index [{}] sucessful. Returning response object.".format(query, index))
            return response['hits']['hits']
        
        self.logger.error("ES Query failed. Returning None.")
        return None

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
    
    #============================================
    # Indices
    #============================================    
    
    def __create_index(self, index_name, settings):
        """
            Create index and mapping for index Crawl Peer Info

        Returns
        -------
        created : Boolean
            Return True if index was succesfully created, or when the index already existed.

        """
        try:
            # Ignore 400 means to ignore "Index Already Exist" error.
            self.es_connection.indices.create(index=index_name, ignore=400) # self.es.indices.create(index=index_name, ignore=400, body=settings)
            self.logger.debug("Created index: [{}]".format(index_name))

        except Exception as e:
            self.logger.error("Error on index creation : {}".format(str(e)))
        finally:
            return
    
    def __index_exists(self, index_name):
        # Check whether or not the specified index exists in the ES instance.
               
        try:
            if self.es_connection.indices.exists(index=index_name):
                self.logger.debug("Index with name [{}] already exists.".format(index_name))
                return True
            self.logger.info("Index with name [{}] was not found.".format(index_name))
            return False
        except elasticsearch.exceptions.NotFoundError as e:
            self.logger.info("Index with name [{}] was not found.".format(index_name))
            self.logger.error("Error message: {}".format(e))
            return False
        except Exception as e:
            self.logger.exception("An error has occurred: {}".format(e))
        
        
    def __delete_database_index(self, index_name):
        # Delete specified index from ES instance, maybe add { ignore=[400, 404] }  as argument if there are errors.
        self.logger.info("Deleting index [{}]".format(index_name))
        self.es_connection.indices.delete(index=index_name)
        self.logger.info("Index with name [{}] deleted".format(index_name))
    
    