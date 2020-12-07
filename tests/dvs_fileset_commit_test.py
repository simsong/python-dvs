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
FILENAME_TEMPLATE = "{name}_file_{num}.txt"

def make_local_tempfile(d, name, num, extra):
    with open(os.path.join(d,FILENAME_TEMPLATE.format(name=name,num=num)),"w") as f:
        print(f"Temporary file created at {time.localtime()}",file=f)
        if extra:
            BUF="X"*(1024*1024)
            while extra>0:
                extra -= f.write(BUF[0:extra])
        return f.name

def do_dvs_fileset(infile_count, sub_infile_count, sub_outfile_count, extra, check_server):
    with tempfile.TemporaryDirectory() as d:
        # Make a few input files, create a fileset, make a few more files, make a file set, and cat them all to the output
        dc = dvs.DVS()
        dc.set_message("test_dvs_fileset in py.test")
        dc.set_author(os.getenv("USER"))
        dc.add_git_commit(dc.COMMIT_METHOD, auto=True)
        dc.set_attribute(dc.ATTRIBUTE_EPHEMERAL)

        # Test add_local_path with multiple objects
        if infile_count:
            dc.add_local_paths(dc.COMMIT_BEFORE, [make_local_tempfile(d, "infile", num, extra) for num in range(1, infile_count) ] )

        # Test lots of individual adds, each with an object
        if sub_infile_count:
            d2 = dvs.DVS()
            for num in range(1, sub_infile_count+1):
                d2.add_local_paths(dc.COMMIT_BEFORE,[ make_local_tempfile(d, "sub-infile", num, extra)])
            dc.add_child(dc.COMMIT_BEFORE, d2)

        if sub_outfile_count:
            d3 = dvs.DVS()
            for num in range(1, sub_outfile_count+1):
                d3.add_local_paths(dc.COMMIT_AFTER, [ make_local_tempfile(d, "sub-outfile", num, extra)])
            dc.add_child(dc.COMMIT_AFTER, d3)

        dc.commit()

def test_dvs_fileset():
    # Simple tranaction with 4 inputs and 4 outputs and sub-commits
    do_dvs_fileset(4, 4, 0, True, False)
    # Transaction with 1000 inputs and 1 output


if __name__=="__main__":
    # Run with requests logging
    import requests
    import logging
    logging.basicCOnfig(level=logging.DEBUG)
    test_dvs_fileset()
