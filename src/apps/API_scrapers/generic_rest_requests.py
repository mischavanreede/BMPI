# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 15:09:53 2021

@author: Dukadu
"""

import requests   #Documentation: https://pypi.org/project/requests/ ; https://docs.python-requests.org/en/master/user/quickstart/#passing-parameters-in-urls



class RestRequests:
    
    """
    A generic class that can issue REST requests.
    
    """
    
    def __init__(self, config, logger):
        
        self.config = config
        self.logger = logger
        self.timeout = 3        #Default timeout is set to 3 seconds


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
                self.logger.debug("Received response from url : [{}]".format(url))
                result = r.json()
                return result
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