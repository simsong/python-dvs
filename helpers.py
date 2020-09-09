# module

import os
import os.path
import requests
import urllib3
import logging
import datetime
import requests
import json
import hashlib
import socket
import time

from ctools import clogging
from dvs_constants import *

BLOCK_SIZE=1024*1024

def clean_float(v):
    return int(v) if isinstance(v,float) else v

def json_stat(path: str) -> dict:
    """Performs a stat(2) of a file and returns the results in a dictionary"""
    s_obj = os.stat(path)
    return {k: clean_float(getattr(s_obj, k)) for k in dir(s_obj) if k.startswith('st_')}

def hash_filehandle(f):
    sha1_hash = hashlib.sha1()
    md5_hash = hashlib.md5()
    fb = f.read(BLOCK_SIZE)
    while len(fb) > 0:
        sha1_hash.update(fb)
        md5_hash.update(fb)
        fb = f.read(BLOCK_SIZE)
    logging.debug("End hashing %s. sha1=%s",f,sha1_hash.hexdigest())
    return {HEXHASH:sha1_hash.hexdigest(),
            'md5':md5_hash.hexdigest()}
    

def hash_file(fullpath):
    """Hash a file and return its sha1.
    Right now this is a 100% python
    implementation, but we should exec out to openssl for faster
    performance for large files It would be really nice to move to a
    parallelized hash. 
    """
    logging.debug("Start hashing %s",fullpath)
    with open(fullpath, 'rb') as f:
        return hash_filehandle(f)

def get_file_update(path, prev_mtime=None):
    """Analyze a file and return its metadata. If prev_mtime is set and mtime hasn't changed, don't hash."""
    fullpath = os.path.abspath(path)
    update = {METADATA : json_stat(path),
              FILENAME : os.path.basename(fullpath),
              DIRNAME  : os.path.dirname(fullpath),
              HOSTNAME : socket.gethostname(),
              TIME     : int(time.time())
    }
    # If we don't have the previous mtime, or if it has changed,
    # re-hash the file and return that too
    if update[METADATA][ST_MTIME] != prev_mtime:
        update = {**update, **hash_file(fullpath)}
    return update

