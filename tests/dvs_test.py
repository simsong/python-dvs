#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request
import logging
import warnings
import pytest
import time

"""
Test programs for dvs client, both locally and to the sever
"""


# Get 'dvs' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DVS_DEMO_FILE = 'dvs_demo.txt'
DVS_DEMO_PATH = os.path.join(os.path.dirname(__file__), DVS_DEMO_FILE)

from dvs.dvs_constants import *
import dvs
import dvs.dvs
import dvs.helpers

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH
from dvs.dvs_constants import *


def test_get_file_update():
    warnings.filterwarnings("ignore", module="bottle")
    assert os.path.exists(DVS_DEMO_PATH)
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    assert update[FILENAME]==DVS_DEMO_FILE
    assert update[DIRNAME]==os.path.dirname(__file__)
    assert update[ETAG]=='3ae9e58a7b9960539bfc8598c206ace3'
    assert update[HEXHASH]=='666d6346e4bf5534c205d842567e0fbe82866ba3'
    assert update[METADATA][ST_SIZE]==118
    

def test_do_register():
    """Register one of the test files and then see the last time it was used on this system."""
    warnings.filterwarnings("ignore",module="urllib3.connectionpool")
    message = f"This is message {int(time.time())}"
    dvs.dvs.do_register([DVS_DEMO_PATH],message)
