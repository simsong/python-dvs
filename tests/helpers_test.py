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

# Get 'dvs' and 'ctools' into the path
from os.path import dirname,abspath
sys.path.append( dirname(dirname(abspath(__file__))))
import dvs
from dvs.dvs_helpers import *


# add the local directory to the path so that we can run this from python or from py.test
# from the current directory or the parent director
# sys.path.append( dirname( __file__ ))



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
