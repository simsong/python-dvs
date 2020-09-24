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
from os.path import dirname,abspath
sys.path.append( dirname(dirname(abspath(__file__ ) )))

import dvs                      # ../dvs/

# for testing, put "." into the local path
sys.path.append( dirname( __file__ ))
import dvs_test_constants
from dvs_test_constants import DVS_DEMO_FILE,DVS_DEMO_PATH

print(DVS_DEMO_FILE, DVS_DEMO_PATH)
