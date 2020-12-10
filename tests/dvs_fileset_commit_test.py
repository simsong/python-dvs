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

import tempfile
from os.path import dirname,abspath

for ADD_DIR in [ dirname( abspath( __file__ )),
                 dirname( dirname( abspath( __file__ ))) ]:
    if ADD_DIR not in sys.path:
        sys.path.append(ADD_DIR)

import dvs
import dvs_test_constants

from dvs_test_constants import DVS_DEMO_PATH

INFILES = 3
FILENAME_TEMPLATE = "{name}_file_{num}.txt"

def make_local_tempfile(prefix, name, num, filesize):
    with open(os.path.join(prefix, FILENAME_TEMPLATE.format(name=name,num=num)),"w") as f:
        msg = f"Temporary file created at {time.localtime()}\n"
        f.write(msg)
        filesize -= len(msg)
        while filesize > 0:
            BUF="X"*(1024*1024)
            filesize -= f.write(BUF[0:filesize])
        return f.name


def do_dvs_fileset(infile_count, sub_infile_count, sub_outfile_count, filesize = 4096, check_server=False, options=set()):
    with tempfile.TemporaryDirectory() as prefix:
        # Make a few input files, create a fileset, make a few more files, make a file set, and cat them all to the output
        logging.info("do_dvs_fileset. infile_count=%d subinfile=%d suboutfile=%d ", infile_count, sub_infile_count, sub_outfile_count)
        dc = dvs.DVS(options=options)
        dc.set_message(f"test_dvs_fileset(infile_count={infile_count}, "
                       f"sub_infile_count={sub_infile_count}, sub_outfile_count={sub_outfile_count}) in py.test")
        dc.set_author(os.getenv("USER"))
        if "JBID" in os.environ:
            dc.add_kv(key="x-jbid", value=os.getenv("JBID"))
        dc.add_git_commit(dc.COMMIT_METHOD, auto=True)
        dc.set_attribute(dc.ATTRIBUTE_EPHEMERAL)

        # Test add_local_path with multiple objects
        infiles = [make_local_tempfile(prefix, "infile", num, filesize) for num in range(1, infile_count) ]
        if infile_count:
            dc.add_local_paths(dc.COMMIT_BEFORE, infiles  )

        # Test lots of individual adds, each with an object
        if sub_infile_count:
            d2 = dvs.DVS(options=options)
            d2.add_local_paths(dc.COMMIT_BEFORE,
                               [make_local_tempfile(prefix, "sub-infile", num, filesize) for num in range(1,sub_infile_count+1)])
            d2.set_attribute(dc.ATTRIBUTE_EPHEMERAL)
            dc.add_child(dc.COMMIT_BEFORE, d2)

        sub_outfiles = [make_local_tempfile(prefix, "sub-outfile", num, filesize) for num in range(1, sub_outfile_count+1)]
        if sub_outfile_count:
            d3 = dvs.DVS(options=options)
            d3.add_local_paths(dc.COMMIT_AFTER, sub_outfiles)
            d3.set_attribute(dc.ATTRIBUTE_EPHEMERAL)
            dc.add_child(dc.COMMIT_AFTER, d3)
        dc.commit()


        # Now let's make sure that the cache is operational



def test_dvs_fileset_440():
    # Simple tranaction with 4 inputs and 4 outputs and sub-commits
    do_dvs_fileset(4, 4, 0)

def test_dvs_commit_1000():
    # Transaction with 1000 inputs
    do_dvs_fileset(1000, 0, 0)

def test_dvs_commit_1500_fail():
    # Transaction with 1500 inputs. Should generate error
    try:
        do_dvs_fileset(1500, 0, 0, options={dvs.OPTION_NO_AUTO_SUB_COMMIT:True})
        raise RuntimeError("DVS commit with >1000 objects should fail")
    except dvs.exceptions.DVSClientError as e:
        return

def test_dvs_commit_1500_pass():
    # Transaction with 1500 inputs, automatically making sub-commits. Should not generate error
    do_dvs_fileset(1500, 0, 0)


if __name__=="__main__":
    # Run with requests logging
    import requests
    import logging
    logging.basicCOnfig(level=logging.DEBUG)
    test_dvs_fileset()
