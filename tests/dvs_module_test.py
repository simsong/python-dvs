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
from dvs.dvs_constants import COMMIT_BEFORE,COMMIT_METHOD,COMMIT_AFTER


def test_simple_commit():
    dc = dvs.DVS()
    with tempfile.NamedTemporaryFile(mode='w') as tf:
        tf.write(time.asctime()) # always different
        dc.add_local_paths( COMMIT_BEFORE, [tf.name] )
        dc.add_local_paths( COMMIT_METHOD, [__file__] )
        dc.commit()


def test_singleton():
    d1 = dvs.DVS_Singleton()
    d2 = dvs.DVS_Singleton()

    # Make sure it does DVS like things
    assert isinstance(d1.t0, float)
    assert d1.t0 == d2.t0
