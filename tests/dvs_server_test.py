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

import dvs
import dvs.dvs_server

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH

@pytest.fixture
def dbwriter_auth():
    try:
        import webmaint
    except ImportError:
        warnings.warn("Cannot run first_test without webmaint")
        yield None
    yield webmaint.get_auth_dbwriter()

def test_do_file_update(dbwriter_auth):
    if dbwriter_auth is None:
        warnings.warn("Cannot run without webmaint")
        return
    
    update = dvs.get_file_update(DVS_DEMO_PATH)
    dvs.dvs_server.do_update(dbwriter_auth, update)
