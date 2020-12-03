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
Test DVS filesets. A fileset is a commit that has a set of inputs, outputs, or methods, but nothing else.
It's basically a fanout system.
"""

# for testing, put both "." and ".." into the local path

from os.path import dirname,abspath

for ADD_DIR in [ dirname( abspath( __file__ )),
                 dirname( dirname( abspath( __file__ ))) ]:
    if ADD_DIR not in sys.path:
        sys.path.append(ADD_DIR)

import dvs
import dvs_test_constants
from dvs_test_constants import DVS_DEMO_PATH
print(DVS_DEMO_PATH)

import tempfile

INFILES = 3
FILENAME_TEMPLATE = "{name}_file_{num}"

def make_tempfile(d,name,num):
    with open(os.path.join(d,FILENAME_TEMPLATE.format(name=name,num=num))) as f:
        print(f"Temporary file created at {time.localtime()}",file=f)
        return f.name

def test_dvs_fileset():
    with tempfile.TemporaryDirectory() as d:

        # Make a few input files, create a fileset, make a few more files, make a file set, and cat them all to the output
        dc = dvs.DVS()
        dc.set_message("test_dvs_fileset in py.test")
        dc.set_author(os.getenv("USER"))
        dc.add_git_commit(dc.COMMIT_METHOD, auto=True)

        d2 = dvs.DVS()

        for num in range(1,4):
            d2.add_local_paths(dc.COMMIT_BEFORE, make_tempfile(d, "infile", num))
        dc.add_child(dc.COMMIT_BEFORE, d2)

        d3 = dvs.DVS()
        for num in range(1,4):
            d2.add_local_paths(dc.COMMIT_AFTER, make_tempfile(d, "outfile", num))
        dc.add_child(dc.COMMIT_AFTER, d3)

        dc.commit()


    pass
