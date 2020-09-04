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
from .dvs_constants import *

BLOCK_SIZE=1024*1024

def clean_float(v):
    return int(v) if isinstance(v,float) else v

def json_stat(path: str) -> dict:
    s_obj = os.stat(path)
    return {k: clean_float(getattr(s_obj, k)) for k in dir(s_obj) if k.startswith('st_')}

def hash_file(fullpath):
    """Hash a file and return its sha1.
    Right now this is a 100% python
    implementation, but we should exec out to openssl for faster
    performance for large files It would be really nice to move to a
    parallelized hash. We return both the hexhash and the etag (md5)
    """
    file_hash = hashlib.sha1()
    etag_hash = hashlib.md5()
    with open(fullpath, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb)
            etag_hash.update(fb)
            fb = f.read(BLOCK_SIZE)
    return {HEXHASH:file_hash.hexdigest(),
            ETAG:etag_hash.hexdigest()}

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

