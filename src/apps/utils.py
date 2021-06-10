# -*- coding: utf-8 -*-
"""
Created on Thu Jun 10 11:11:13 2021

@author: Dukadu
"""

import os
import sys




def getCurrentPath():
    """
        get the current working path
    """
    return os.path.abspath(os.path.dirname(sys.argv[0]))