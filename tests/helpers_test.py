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
Test programs for the installed dvsserver.
"""

# Get 'dvs' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import dvs
import dvs.dvs_server

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH
from dvs.dvs_constants import *
from dvs.helpers import *


from helpers import *



def test_comma_args():
    assert comma_args(0)==""
    assert comma_args(1)=="%s"
    assert comma_args(2)=="%s,%s"
    assert comma_args(3)=="%s,%s,%s"

def test_is_hexadecimal():
    assert is_hexadecimal("")==False
    assert is_hexadecimal("Z")==False
    assert is_hexadecimal("0")==True
    assert is_hexadecimal("12345abcd")==True
    assert is_hexadecimal("Z12345abcd")==False

