# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 15:09:53 2021

@author: Mischa van Reede

https://www.nylas.com/blog/use-python-requests-module-rest-apis/

TODO: Handle various errors: https://dev.btc.com/docs/js#api-authentication


"""

import requests   #Documentation: https://pypi.org/project/requests/ ; https://docs.python-requests.org/en/master/user/quickstart/#passing-parameters-in-urls
import time


class RestRequests:
    
    """
    A generic class that can issue REST requests.
    
    Only implemented GET requests for now.
    
    """
    
    def __init__(self, config, logger):
        
        self.config = config
        self.logger = logger
        self.timeout = 10                  # Response timeout
        self.request_delay = 0.1      # Time delay between GET requests


    def change_timeout(self, timeout):
        """
        Changes the timeout value that is passed as parameter in the REST requests.

        Parameters
        ----------
        timeout : Integer
            timeout for REST requests in seconds.

        Returns
        -------
        None.

        """
        self.logger.debug("Change te timeout to : [{}]".format(timeout))
        self.timeout = timeout
        
    def change_request_delay(self, delay):
        """
        Changes the timeout value that is passed as parameter in the REST requests.

        Parameters
        ----------
        delay : integer
            Time delay between GET requests
        """
        self.logger.debug("Change te timeout to : [{}]".format(delay))
        self.request_delay = delay
        
        
    def get(self, url, params=None):
        """
        Makes a GET request to the specified url. Allows for optional parameters to be passed.
    
        Parameters
        ----------
        url : string
            The URL to query.
        params : dict, optional.
             Example:
            payload = {'user_name': 'admin', 'password': 'password'}
            r = requests.get('http://httpbin.org/get', params=payload). 
            The default is None.

        Returns
        -------
        result : json
            Returns results of the GET request in json format.

        """
        try:

            if params:
                r = requests.get(url, params=params, timeout=self.timeout)
            else:
                r = requests.get(url, timeout=self.timeout)

            r.raise_for_status()

            if r.status_code == 200:
                # self.logger.debug("Received response from url : [{}]".format(url))
                time.sleep(self.request_delay)
                return r.json()
            else:
                print("Error: received no response from url : [{}]".format(url))
                return None

        except requests.exceptions.HTTPError as errh:
            print("Error: HTTP Error : [{}]".format(errh))
            return None
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting : [{}]".format(errc))
            return None
        except requests.exceptions.Timeout as errt:
            print("Error Timeout : [{}]".format(errt))
            return None
        except requests.exceptions.RequestException as err:
            print("Error: Something gets wrong : [{}]".format(err))
            return None