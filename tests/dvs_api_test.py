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

