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

def test_store_objects(dbwriter_auth):
    """Make three objects, store them, and see if we can get them back."""
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    obj1 = {MESSAGE:time.asctime()}
    obj2 = {MESSAGE:time.asctime()+"test"}
    url3 = "https://www.census.gov/"
    objects = dvs.helpers.objects_dict([obj1,obj2,url3])
    dvs.dvs_server.store_objects(dbwriter_auth, objects)

    # Now verify that they were stored
    retr = dvs.dvs_server.get_objects(dbwriter_auth,list(objects.keys()))
    vals = list(retr.values())
    
    assert len(vals)==3
    assert obj1 in vals
    assert obj2 in vals
    assert url3 in vals

    
def test_do_file_update(dbwriter_auth):
    """Store a file update for a single file in the database"""
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    if dbwriter_auth is None:
        warnings.warn("Cannot run without webmaint")
        return
    
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    # make sure the update object is stored
    dvs.dvs_server.store_objects(dbwriter_auth, dvs.helpers.objects_dict([update])) 
    # Do it
    dvs.dvs_server.do_update(dbwriter_auth, update)


def test_store_commit(dbwriter_auth):
    """Store a file update for a single file in the database"""
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    if dbwriter_auth is None:
        warnings.warn("Cannot run without webmaint")
        return
    
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    objects = dvs.helpers.objects_dict([update])

    # make sure the update object is stored
    dvs.dvs_server.store_objects(dbwriter_auth,objects)
    # Do it
    commit = {BEFORE:list(objects.keys())}
    dvs.dvs_server.store_commit(dbwriter_auth, commit)


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

        
