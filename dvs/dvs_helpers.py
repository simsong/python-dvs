"""
Misc. functions that are used in the DAS code.
"""

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
import string
import sys
import pwd
import grp
import string

from .dvs_constants import *

BLOCK_SIZE    = 1024*1024
INCLUDE_GECOS = False
MIN_HASH_LENGTH = 6

def comma_args(count,rows=1,parens=False):
    v = ",".join(["%s"]*count)
    if rows!=1 and not parens:
        raise ValueError("parens must be True if rows>1")
    if parens:
        v = f"({v})"
        return ",".join([v] * rows)
    return v


def clean_float(v):
    return int(v) if isinstance(v,float) else v

def json_stat(path: str) -> dict:
    """Performs a stat(2) of a file and returns the results in a
    dictionary. Do not include atime (it's frequently wrong). Include full
    username and groupname, rather than just UID/GUID

    :param path: the path to stat
"""
    s_obj = os.stat(path)
    obj = {k: clean_float(getattr(s_obj, k)) for k in dir(s_obj) if k.startswith('st_') and ("atime" not in k)}
    try:
        p = pwd.getpwuid(obj['st_uid'])
    except KeyError as e:
        pass
    else:
        obj['pw_pwname'] = p.pw_name
        if INCLUDE_GECOS:
            obj['pw_gecos'] = p.pw_gecos

    try:
        g = grp.getgrgid(obj['st_gid'])
    except KeyError as e:
        pass
    else:
        obj['gr_name'] = g.gr_name

    return obj

def hash_filehandle(f):
    """Right now this is done single-threaded. It could be parallelized.
    """
    sha512_hash = hashlib.sha512()
    sha256_hash = hashlib.sha256()
    sha1_hash   = hashlib.sha1()
    md5_hash    = hashlib.md5()
    fb          = f.read(BLOCK_SIZE)
    count       = 0
    next_gig    = 100_000_000
    while len(fb) > 0:
        sha1_hash.update(fb)
        md5_hash.update(fb)
        count += len(fb)
        if count>next_gig:
            print(f"  ... hashed {count:,} bytes",file=sys.stderr)
            next_gig += 100_000_000
        fb = f.read(BLOCK_SIZE)
    logging.debug("End hashing %s. sha1=%s",f,sha1_hash.hexdigest())
    return {SHA512:sha512_hash.hexdigest(),
            SHA256:sha256_hash.hexdigest(),
            SHA1:sha1_hash.hexdigest(),
            MD5:md5_hash.hexdigest()}


def hash_file(fullpath):
    logging.debug("Start hashing %s",fullpath)
    with open(fullpath, 'rb') as f:
        return hash_filehandle(f)

def hexhash_string(s):
    """Just return the hexadecimal SHA1 of a string"""
    assert HEXHASH_ALG == SHA1
    sha1_hash = hashlib.sha1()
    sha1_hash.update(s.encode('utf-8'))
    return sha1_hash.hexdigest()

def is_hexadecimal(s):
    """Return true if s is hexadecimal string"""
    if isinstance(s,str)==False:
        return False
    elif len(s)==0:
        return False
    elif len(s)==1:
        return s in string.hexdigits
    else:
        return all([is_hexadecimal(ch) for ch in s])

def canonical_json(obj):
    """Turns obj into a string in the canonical json format"""
    return json.dumps(obj,sort_keys=True,default=str)

def canonical_json_hexhash(obj):
    """Turns obj into a string in the canonical json format"""
    return hexhash_string(json.dumps(obj,sort_keys=True,default=str))

def get_file_observation(path):
    """Return a file update without the file hashes"""
    fullpath = os.path.abspath(path)

    obj= {FILE_METADATA : json_stat(path),
          FILENAME : os.path.basename(fullpath),
          DIRNAME  : os.path.dirname(fullpath),
          HOSTNAME : socket.getfqdn()}

    # Note approach for finding ipaddresses does not work if hostname is not in DNS
    try:
        obj[IPADDR] = socket.gethostbyaddr(socket.gethostname())[3][0]
    except (KeyError, IndexError, socket.herror):
        pass

    return obj


def get_file_observation_with_hash(path):
    """Return a file update with the hash"""
    return {**get_file_observation(path), **{FILE_HASHES:hash_file(path)}}


def objects_dict(objects):
    """Given a list of objects, return a dictionary where the key for each object is is canonical_json_hexhash"""
    return {canonical_json_hexhash(obj):obj for obj in objects}


def check_length_is_unique_prefix(hexhashes:set, length:int) -> bool:
    """Returns True if the length is sufficient to distinguish all of the hex hashes."""
    prefixes = set()
    for hexhash in hexhashes:
        prefix = hexhash[0:length]
        if prefix in prefixes:
            return False
        prefixes.add(prefix)
    return True

def length_of_unique_prefix(hexhashes) -> int:
    """Given a list of hexhashes, return the length of necessary to distinguish them all.
    There is probably a clever O(N) way to do this, but I coudln't figure it out. This seems fast enough.
    :params hexhashes: a list of hexhashes
    returns an integer.
    """
    # We could probably do this with a binary search. But instead, we just start with 8 and count up
    hexhashes = set(hexhashes)  # make sure we have each exactly once
    for length in range(MIN_HASH_LENGTH, max([len(hexhash) for hexhash in hexhashes])):
        if check_length_is_unique_prefix(hexhashes,length):
            return length
    raise RuntimeError("We should not reach here")

def dvs_debug_obj_str(obj):
    """For debugging, return a subjset of the object"""
    return f"{obj.get('dirname','')}/{obj.get('filename','')}  {obj.get('hashes',{}).get('sha1','')}"
