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
import dvs.dvs_helpers as dvs_helpers
from dvs.dvs_constants import *


# Grab the dvs cli functions
import cli.dvsc as dvs_cli

# put the local directory in the path so we can import this
sys.path.append( dirname( __file__ ))
from dvs_test_constants import DVS_DEMO_FILE,DVS_DEMO_PATH,S3LOC1


def test_get_file_observation_with_hash():
    warnings.filterwarnings("ignore", module="bottle")
    assert os.path.exists(DVS_DEMO_PATH)
    update = dvs_cli.get_file_observation_with_hash(DVS_DEMO_PATH)
    assert update[FILENAME]==DVS_DEMO_FILE
    assert update[DIRNAME]==os.path.dirname(__file__)
    assert update[FILE_HASHES][SHA1]=='666d6346e4bf5534c205d842567e0fbe82866ba3'
    assert update[FILE_METADATA][ST_SIZE]==118

@pytest.fixture
def do_commit():
    """Register one of the test files."""
    dvs_cli.set_debug_endpoints("~garfi303adm/html/")
    warnings.filterwarnings("ignore",module="urllib3.connectionpool")
    commit = {COMMIT_MESSAGE:f"This is message {int(time.time())}"}
    dvs_cli.do_commit(commit,[DVS_DEMO_PATH])
    yield DVS_DEMO_PATH

@pytest.fixture
def do_s3commit():
    """Copy a file to s3 and register it to see if we can work with legacy S3 files.
    Then copy a file to s3 with our s3 copy routine"""
    subprocess.call(['aws','s3','cp',DVS_DEMO_PATH,S3LOC1])
    commit = {COMMIT_MESSAGE:f"This is an S3 message {int(time.time())}"}
    dvs_cli.do_commit(commit,[S3LOC1])
    yield S3LOC1

def test_do_search(do_commit,do_s3commit):
    """Search the file that was just registered and see if its hash is present"""
    hashes = dvs_helpers.hash_file( do_commit )

    searches = dvs_cli.do_search([do_commit], debug=True)
    for search in searches:
        for result in search[RESULTS]:
            if (result[OBJECT][FILENAME] == os.path.basename(do_commit)  and
                result[OBJECT][FILE_HASHES][SHA1] == hashes[SHA1]):
                logging.info("Found %s", do_commit)
                return
    warnings.warn("Searching for %s did not find result in:\n%s"
                  % (do_commit,json.dumps(searches,indent=4,default=str)))
    raise FileNotFoundError()
