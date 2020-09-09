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
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DVS_DEMO_FILE = 'dvs_demo.txt'
DVS_DEMO_PATH = os.path.join(os.path.dirname(__file__), DVS_DEMO_FILE)

S3LOC1 = os.environ['DAS_S3ROOT'] + '/tmp/demofile1.txt'
warnings.warn("S3LOC1 %s" % S3LOC1)

from dvs.dvs_constants import *
import dvs
import dvs.dvs
import dvs.helpers

# local directory:
from dvs_test import DVS_DEMO_FILE,DVS_DEMO_PATH
from dvs.dvs_constants import *


def test_get_file_update():
    warnings.filterwarnings("ignore", module="bottle")
    assert os.path.exists(DVS_DEMO_PATH)
    update = dvs.helpers.get_file_update(DVS_DEMO_PATH)
    assert update[FILENAME]==DVS_DEMO_FILE
    assert update[DIRNAME]==os.path.dirname(__file__)
    assert update[HEXHASH]=='666d6346e4bf5534c205d842567e0fbe82866ba3'
    assert update[METADATA][ST_SIZE]==118
    

@pytest.fixture
def do_register():
    """Register one of the test files and then see the last time it was used on this system."""
    dvs.dvs.set_debug_endpoints("~garfi303adm/html/")
    warnings.filterwarnings("ignore",module="urllib3.connectionpool")
    note = f"This is note {int(time.time())}"
    dvs.dvs.do_register([DVS_DEMO_PATH],note=note)
    yield DVS_DEMO_PATH

@pytest.fixture
def do_s3register():
    """Copy a file to s3 and register it to see if we can work with legacy S3 files. Then copy a file to s3 with our s3 copy routine"""
    subprocess.call(['aws','s3','cp',DVS_DEMO_PATH,S3LOC1])
    s3note = f"This is an S3 note {int(time.time())}"
    dvs.dvs.do_register([S3LOC1],note=s3note)
    yield S3LOC1

def test_do_search(do_register,do_s3register):
    """Search the file that was just registered and see if its hash is present"""
    hexhash = dvs.helpers.hash_file( do_register )[HEXHASH]
    
    searches = dvs.dvs.do_search([do_register], debug=True)
    for search in searches:
        for result in search[RESULTS]:
            if (result.get(FILENAME,None) == os.path.basename(do_register)  and
                result.get(HEXHASH,None)  == hexhash):
                logging.info("Found %s", do_register)
                return 
    warnings.warn("Searching for %s did not find result in:\n%s" % (do_register,json.dumps(searches,indent=4,default=str)))
    raise FileNotFoundError()

