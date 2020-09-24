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
from os.path import dirname,abspath
sys.path.append( dirname(dirname(abspath(__file__))))
import dvs

import dvs.dvs_helpers
try:
    import dvs.dvs_server
    DVS_SERVER=True
except ModuleNotFoundError:
    DVS_SERVER=False
    warnings.warn("DVS server tests skipped")

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH
from dvs.dvs_constants import *


@pytest.fixture
def dbwriter_auth():
    if DVS_SERVER==False:
        warnings.warn("DVS Server not available")
        yield None
    warnings.filterwarnings("ignore", module="bottle")
    try:
        import webmaint
    except ImportError:
        warnings.warn("Cannot run first_test without webmaint")
        yield None
    yield webmaint.get_auth_dbwriter()

def test_store_objects(dbwriter_auth):
    """Make three objects, store them, and see if we can get them back."""
    if DVS_SERVER==False:
        warnings.warn("DVS Server not available")
        yield None
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    obj1 = {MESSAGE:time.asctime()}
    obj2 = {MESSAGE:time.asctime()+"test"}
    url3 = "https://www.census.gov/"
    objects = dvs.dvs_helpers.objects_dict([obj1,obj2,url3])
    dvs.dvs_server.store_objects(dbwriter_auth, objects)

    # Now verify that they were stored
    retr = dvs.dvs_server.get_objects(dbwriter_auth,list(objects.keys()))
    vals = list(retr.values())

    assert len(vals)==3
    assert obj1 in vals
    assert obj2 in vals
    assert url3 in vals


def test_store_commit(dbwriter_auth):
    """Store a file update for a single file in the database"""
    if DVS_SERVER==False:
        warnings.warn("DVS Server not available")
        yield None
    warnings.filterwarnings("ignore", module="pymysql.cursors")
    warnings.filterwarnings("ignore", module="bottle")
    if dbwriter_auth is None:
        warnings.warn("Cannot run without webmaint")
        return

    update = dvs.dvs_helpers.get_file_observation_with_hash(DVS_DEMO_PATH)
    objects = dvs.dvs_helpers.objects_dict([update])

    # make sure the update object is stored
    dvs.dvs_server.store_objects(dbwriter_auth,objects)
    # Do it
    commit = {BEFORE:list(objects.keys())}
    dvs.dvs_server.store_commit(dbwriter_auth, commit)
