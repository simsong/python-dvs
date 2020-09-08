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
Test programs for the dvs_server.py program.
"""

# Get 'dvs' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import dvs.helpers
import dvs.dvs_server

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH
from dvs.dvs_constants import *


@pytest.fixture
def dbwriter_auth():
    warnings.filterwarnings("ignore", module="bottle")
    try:
        import webmaint
    except ImportError:
        warnings.warn("Cannot run first_test without webmaint")
        yield None
    yield webmaint.get_auth_dbwriter()

def test_do_file_update(dbwriter_auth):
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    if dbwriter_auth is None:
        warnings.warn("Cannot run without webmaint")
        return
    
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    dvs.dvs_server.do_update(dbwriter_auth, update)

def test_do_note(dbwriter_auth):
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    note = f"This is note {int(time.time())}"
    dvs.dvs_server.add_note(dbwriter_auth, hexhash=update[HEXHASH], author=os.environ['USER'], note=note)

    notes = dvs.dvs_server.get_notes(dbwriter_auth, update[HEXHASH])
    for n in notes:
        if n['note']==note:
            return True
    raise RuntimeError(f"Could not find note '{note}' in '{notes}'")

        
