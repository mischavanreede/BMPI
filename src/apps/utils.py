# -*- coding: utf-8 -*-
"""
This file contains a utility class with methods that are used by the various modules of this application.

@author: Mischa van Reede
"""

import os
import sys


class Utils():

    def getCurrentPath():
        """
            get the current working path
        """
        return os.path.abspath(os.path.dirname(sys.argv[0]))
    
    
    
    def epochToDateString(epoch_timestamp):
        """
        converts epoch timestamp to date/time string in format

        Parameters
        ----------
        epoch_timestamp : int
            DESCRIPTION.

        Returns
        -------
        None.

        """
        pass
