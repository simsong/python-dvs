#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request
import logging
import warnings
import pytest

"""
Test programs for the dvsserver both without and with the bottle server.
"""

# Get 'dvs' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DVS_DEMO_FILE = 'dvs_demo.txt'
DVS_DEMO_PATH = os.path.join(os.path.dirname(__file__), DVS_DEMO_FILE)

from dvs.dvs_constants import *
import dvs

def test_get_file_update():
    assert os.path.exists(DVS_DEMO_PATH)
    update = dvs.get_file_update(DVS_DEMO_PATH)
    assert update[FILENAME]==DVS_DEMO_FILE
    assert update[DIRNAME]==os.path.dirname(__file__)
    assert update[ETAG]=='3ae9e58a7b9960539bfc8598c206ace3'
    assert update[HEXHASH]=='666d6346e4bf5534c205d842567e0fbe82866ba3'
    assert update[METADATA][ST_SIZE]==118
    
