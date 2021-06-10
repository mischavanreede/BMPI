# -*- coding: utf-8 -*-
"""
This file contains utility methods that are used by the various modules of this application.

@author: Mischa van Reede
"""

import os
import sys




def getCurrentPath():
    """
        get the current working path
    """
    return os.path.abspath(os.path.dirname(sys.argv[0]))