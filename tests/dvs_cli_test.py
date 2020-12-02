#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request
import logging
import warnings
import pytest
import time
import json
import subprocess

"""
Test programs for dvs client, both locally and to the sever
"""

# Get 'dvs' into the path
from os.path import dirname,abspath

sys.path.append( dirname(dirname(abspath(__file__))))
import dvs
import dvs.dvs_helpers as dvs_helpers
from dvs.dvs_constants import *


# Grab the dvs cli functions
import cli.dvsc as dvs_cli

# put the local directory in the path so we can import this
sys.path.append( dirname( __file__ ))
from dvs_test_constants import DVS_DEMO_PATH,S3LOC1


@pytest.fixture
def s3loc():
    """Copy a file to s3 and register it to see if we can work with legacy S3 files.
    Then copy a file to s3 with our s3 copy routine"""
    subprocess.call(['aws','s3','cp',DVS_DEMO_PATH,S3LOC1])
    yield S3LOC1

def test_do_search(s3loc):
    """Search the file that was just registered and see if its hash is present"""
    hashes = dvs_helpers.hash_file( DVS_DEMO_PATH )

    dc = dvs.DVS(verify=DEFAULT_VERIFY)
    print(dc)

    commit = dvs_cli.do_cp( dc, DVS_DEMO_PATH, s3loc)

    searches = dvs_cli.do_search(dc, [hashes[SHA1]], debug=True)
    for search in searches:
        for result in search[RESULTS]:
            if (result[OBJECT][FILENAME] == os.path.basename(DVS_DEMO_PATH)  and
                result[OBJECT][FILE_HASHES][SHA1] == hashes[SHA1]):
                logging.info("Found %s", hashes)
                return
    warnings.warn("Searching for %s did not find result in:\n%s"
                  % (do_commit,json.dumps(searches,indent=4,default=str)))
    raise FileNotFoundError()
