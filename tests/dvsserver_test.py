#!/usr/bin/env python3

import os
import sys
import urllib.parse
import urllib.request
sys.path.append( os.path.join( os.path.dirname(__file__), '..'))
import webmaint
import ctools
import ctools.dbfile as dbfile

import pytest

"""
Test programs for the dvsserver both without and with the bottle server.
"""
