#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request
import logging
import warnings
import pytest
import time
import tempfile
"""
Test the dvs module
"""

# Make this work as either a module or as a command
from os.path import dirname, abspath, basename
sys.path.append( dirname(dirname( abspath( __file__ ) )))
import dvs


def test_simple_commit():
    dc = dvs.DVS()
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        tf.write(time.asctime()) # always different
        dc.add_before( filename = tf.name )
        dc.add_method( filename = __file__ )
        dc.commit()


if __name__=='__main__':
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    args = parser.parse_args()
    simple_commit_test()
