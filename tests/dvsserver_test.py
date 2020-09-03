#!/usr/bin/env python3
import os
import sys
import urllib.parse
import urllib.request
import logging
import warnings
import pytest
"""
Test programs for the dvsserver both without and with the bottle server.
"""
def test_first():
    try:
        import webmaint
    except ImportError:
        warnings.warn("Cannot run first_test without webmaint")
        return
