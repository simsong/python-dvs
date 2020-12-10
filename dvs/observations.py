import os
import os.path
import requests
import urllib3
import logging
import datetime
import requests
import json
import sys
import boto3
import botocore
import time
import shutil
import subprocess
import socket
import copy
from multiprocessing import Pool
"""
Routines for getting observations.
"""

from .dvs_constants import *
from .dvs_helpers  import *
from .server import MAX_SEARCH_OBJECTS
from .exceptions import DVSServerError
from .dvs_helpers import dvs_debug_obj_str


DEFAULT_THREADS=20
MAX_DEBUG_PRINT=260
MAX_HTTP_RETRIES = 5
CACHE_CHECK_LOCAL_MIN_FILE_SIZE = 64*1024*1024 # if the file is smaller than 64MiB, don't check the server
DVS_SERVER_SEARCH_BATCH_SIZE    = 100 # batch size of searches


# Impelmentretries with requests
# https://dev.to/ssbozy/python-requests-with-retries-4p03

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session( retries=MAX_HTTP_RETRIES,
                            backoff_factor=0.3,
                            status_forcelist=(500, 502, 504),
                            session=None ):
    session = session or requests.Session()
    retry = Retry( total=retries,
                   read=retries,
                   connect=retries,
                   backoff_factor=backoff_factor,
                   status_forcelist=status_forcelist )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# debug flags
debug_hash_every_s3path   = False
debug_hash_every_s3prefix = True
debug_hash_server        = True


def debug_str(s):
    """Return s if smaller than MAX_DEBUG_PRINT , otherwise print something more understandable"""
    s = str(s)
    if len(s) < MAX_DEBUG_PRINT:
        return s
    return f"{s[0:MAX_DEBUG_PRINT -30]} ... ({len(s):,} chars) ... {s[-30:]}"


################################################################
### s3 support routines


def s3olen(s3obj):
    """return the length of the object in AWS S3 referneces by the s3obj.
    Note that if s3 is an s3.Object, then we use s3obj.content_length,
    but if s3 is an s3.ObjectSummary, we use s3obj.size.
    It would be nice if Amazon had been consistent.
    """
    try:
        return s3obj.content_length
    except AttributeError as e:
        pass

    try:
        return s3obj.size
    except AttributeError as e:
        pass

    raise RuntimeError(f"Unknown object {str(s3obj)} responds to {dir(s3obj)}")


def get_bucket_key(loc):
    """Given a location, return the (bucket,key)"""
    from urllib.parse import urlparse
    p = urlparse(loc)
    if p.scheme == 's3':
        return p.netloc, p.path[1:] # strips the leading /
    assert ValueError("{} is not an s3 location".format(loc))


def clean_etag(etag):
    """Amazon's S3 protocol returns etags surrounded by quotes for an unknown reason"""
    if etag[0] == '"':
        return etag[1:-1]
    return etag


def s3path_to_s3obj(s3path):
    """Given an s3path, return a tuple of (s3path, s3obj). Designed to be parallelized with python multiprocessing library.
    :param s3path: the s3path, including s3://"""
    (bucket,key) = get_bucket_key(s3path)
    try:
        # Get and return the object, validating that it exists
        s3obj = boto3.resource( AWS_S3 ).Object( bucket, key)
        assert s3obj.content_length >= 0
        return s3obj
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code']=='404':
            raise FileNotFoundError(s3path)
        raise(e)


def s3obj_to_s3path(s3obj):
    """Given an s3obj, turn it into a path"""
    return f"s3://{s3obj.Bucket().name}/{s3obj.key}"

################################################################

def server_search_post(*, search_endpoint, search_dicts, stride_length=MAX_SEARCH_OBJECTS, verify=DEFAULT_VERIFY):
    """Actually performs the server search."""
    return_list = []
    search_dict_values = list(search_dicts.values())
    for offset in range(0, len(search_dict_values), stride_length):
        stride = search_dict_values[offset:offset+stride_length]

        logging.debug("Search send: %d/%d%s", stride, len(search_dict_values), debug_str(stride))
        print("..Search send: %d/%d len(stride)=%d" % (offset,len(search_dict_values),len(stride)),file=sys.stderr)
        print("..search endpoint=",search_endpoint,file=sys.stderr)
        r = requests_retry_session().post(search_endpoint,
                          data = {'searches':json.dumps(stride, default=str)},
                          verify = verify)
        print(f"Return. r.status_code={r.status_code} len(r.text)={len(r.text)}\n", file=sys.stderr)
        logging.debug(f"Return. r.status_code={r.status_code} len(r.text)={len(r.text)}")
        if r.status_code!=HTTP_OK:
            raise RuntimeError("Server response: %d %s" % (r.status_code,r.text))
        try:
            return_list.extend(r.json())
        except json.decoder.JSONDecodeError as e:
            print("Invalid response from server for search request: ",r,file=sys.stderr)
            raise DVSServerError()
    logging.debug("Search server objects returned: %d",len(return_list))
    for obj in return_list:
        logging.debug("   %s",obj)
    return return_list


def hash_s3obj(s3obj):
    """hash_s3obj: given an s3object, return an DVS observation including a hash."""
    if debug_hash_every_s3path:
        print(f"PID {os.getpid()} S3 Hashing s3://{bucket}/{key} {s3obj.content_length:,} bytes...",file=sys.stderr)
    hashes = hash_filehandle(s3obj.get()['Body'])
    return {HOSTNAME: DVS_S3_PREFIX + s3obj.Bucket().name,
            DIRNAME:  os.path.dirname( s3obj.key),
            FILENAME: os.path.basename( s3obj.key),
            FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                            ST_MTIME : int(s3obj.last_modified.timestamp()),
                            ETAG     : clean_etag(s3obj.e_tag)},
            FILE_HASHES: hashes}


def hash_s3path(s3path):
    return hash_s3obj( s3path_to_s3obj( s3path ))

def get_s3objs_observations(s3objs:list, *, search_endpoint:str, verify=DEFAULT_VERIFY, threads=DEFAULT_THREADS):
    """Given a list of s3.Object or s3.ObjectSummary objects:.
    1. If a search_endpoint is specified, send searches to the endpoint in batches of DVS_SERVER_SEARCH_BATCH_SIZE.
    2. For those objects that we coudln't find the hashehs on the sever, hash the s3 path. oO this in parallel too.
    3. Return a list of observations.
    3. If there is, use the hash that is already on the object server.
    4. If not, download the S3 file and hash it.
    5. Return an observation
        """

    # https://stackoverflow.com/questions/52402421/retrieving-etag-of-an-s3-object-using-boto3-client

    assert isinstance(s3objs, list)

    # Get the s3obj for all of the paths. This will give us the S3 Etag, length, and last access time
    # TODO: Could we have gotten this information when we did the original list?
    logging.info("getting objects  for %s paths",len(s3objs))
    if debug_hash_server:
        print(f"get_s3file_observations: Getting tags for {len(s3objs)} paths", file=sys.stderr)

    # This is (annoyingly) still single-threaded. For each object, execute a search
    # We could just do a single search for all of them..

    s3file_observations = []
    if search_endpoint is None:
        logging.debug("search_endpoint is None. Will not use cache")
        if debug_hash_server:
            print(f"Search_endpoint is None. will not use cache",file=sys.stderr)
        s3objs_to_hash      = copy.copy(s3objs)

    elif DVS_OBJECT_CACHE_ENV in os.environ:
        logging.debug("Running with DVS_OBJECT_CACHE. Not checking server for cached hash.")
        s3objs_to_hash      = copy.copy(s3objs)

    else:
        logging.info("Checking server for %s paths",len(s3objs))
        if debug_hash_server:
            print(f"Checking server for {len(s3objs)} paths",file=sys.stderr)
        s3objs_to_hash = []
        for offset in range(0, len(s3objs), DVS_SERVER_SEARCH_BATCH_SIZE):
            print("\nOFFSET:",offset,file=sys.stderr)
            stride = s3objs[offset:offset+DVS_SERVER_SEARCH_BATCH_SIZE]
            search_dicts = {ct :
                            { HOSTNAME:  DVS_S3_PREFIX + s3obj.Bucket().name,
                              DIRNAME:   os.path.dirname( s3obj.key),
                              FILENAME:  os.path.basename( s3obj.key),
                              FILE_METADATA: {ST_SIZE  : s3olen(s3obj),
                                              ST_MTIME : int(s3obj.last_modified.timestamp()),
                                              ETAG     : clean_etag(s3obj.e_tag)},
                              ID: ct}
                            for (ct,s3obj) in enumerate( stride, offset)}

            print("LEN SEARCH_DICTS=",len(search_dicts),file=sys.stderr)
            if debug_hash_server:
                print(f"  ** Requesting search on {len(search_dicts)} objects",file=sys.stderr)


            rjson = server_search_post(search_endpoint = search_endpoint,
                                       search_dicts = search_dicts,
                                       stride_length = DVS_SERVER_SEARCH_BATCH_SIZE,
                                       verify = verify)
            rjson=[]


            # every response is a success for which we do not need to hash.
            results_by_searchid = {response[SEARCH][ID] : response[RESULTS] for response in rjson}
            print(f" %% Response. count={len(results_by_searchid)}")
            unfound_offsets = set()

            # See if we have observations from the server that match any of the observations
            # we got from the server. If they do, use them, and remove them from the list of s3objs
            # that need to be hashed.
            for (ct,s3obj) in enumerate(stride,offset):
                unfound_offsets.add(ct)
                if ct in results_by_searchid:
                    for result in results:
                        objr = result[OBJECT]
                        if (objr.get(DIRNAME,None)       == os.path.dirname( s3obj.key)  and
                            objr.get(FILENAME,None)      == os.path.basename( s3obj.key) and
                            objr.get(FILE_METADATA,None) == file_metadata and
                            FILE_HASHES in objr):
                            logging.info("using hash from server for %s/%s %s ",dirname,filename,file_metadata)
                            s3file_observations.append( objr )
                            unfound_offsets.remove( ct )
                            break
            # Now note the ones that we need hashing for
            for ct in unfound_offsets:
                s3objs_to_hash.append( s3objs[ct] )
            # At this point every object has been searched on the server. Some need to be hashesd, some don't
    assert len(s3file_observations) + len(s3objs_to_hash) == len(s3objs)

    # Still need to parallelize this.
    # Use the StreamingBody() to download the object.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html
    # This code is very similar to server_s3search above and should probably be factored into that.

    logging.info("Parallel hashing of remaining %s s3 objects",len(s3objs_to_hash))
    if debug_hash_every_s3prefix:
        print("Parallel hashing of %s files with %d threads" % (len(s3objs_to_hash), threads),file=sys.stderr)
    with Pool(threads) as p:
        s3file_observations.extend( p.map(hash_s3path,
                                          [s3obj_to_s3path(s3obj) for s3obj in s3objs_to_hash] ))

    logging.info("Parallel hashing of %s files DONE",len(s3objs_to_hash))
    if debug_hash_every_s3prefix:
        print("Parallel hashing of %s files with %d threads DONE" % (len(s3objs_to_hash), threads),file=sys.stderr)
        print("Returning %d objects" % (len(s3file_observations)),file=sys.stderr)
    return s3file_observations


# Note: get_file_observations is similar to function above,
# except it pipelines multiple searches at once.
def get_file_observations(paths:list, *, search_endpoint:str, verify=DEFAULT_VERIFY):
    """Create a list of file observations for a list of paths.
    1. Send the list of paths to the server and ask if the mtime for any of them are known
       We make a search_dictionary, which is the search object for each of the paths passed in,
       indexed by path
    2. Hash the files that are not known to the server.
    3. Return the list of observation objects.
    """
    logging.debug("paths 1: %s",paths)
    assert isinstance(paths,list)
    assert all([isinstance(path,str) for path in paths])

    # Get the metadata for each path once.
    metadata_for_path = {path:json_stat(path) for path in paths}

    if search_endpoint is None:
        logging.debug("will not search")
        results_by_path = {}

    elif DVS_OBJECT_CACHE_ENV in os.environ:
        logging.debug("Will not search remote cache")
        results_by_path = {}

    else:
        logging.debug("Searching to see if dirname, filename, and mtime is known for any of our commits")
        search_dicts = {ct :
                        { HOSTNAME: socket.getfqdn(),
                          PATH: os.path.abspath(path),
                          DIRNAME: os.path.dirname(os.path.abspath(path)),
                          FILENAME: os.path.basename(path),
                          FILE_METADATA: metadata_for_path[path],
                          ID : ct }
                        for (ct,path) in enumerate(paths)
                        if metadata_for_path[path][ST_SIZE] > CACHE_CHECK_LOCAL_MIN_FILE_SIZE}
        results_by_searchid = {}

        # Now we want to send all of the objects to the server as a list

        rjson = server_search_post(search_endpoint=search_endpoint,
                                   search_dicts=search_dicts,
                                   verify=verify)
        results_by_path = {response[SEARCH][PATH] : response[RESULTS] for response in rjson}


    # Now we get the back and hash all of the objects for which the server has no knowledge, or for which the mtime does not agree
    file_objs = []
    logging.debug("paths: %s",paths)
    for path in paths:
        obj = None
        if path in results_by_path:
            results = results_by_path[path]
            # If any of the objects has a metadata that matches, and it has a hash, use it
            for result in results:
                objr = result[OBJECT]
                if (objr.get(DIRNAME,None)       == os.path.dirname(path)  and
                    objr.get(FILENAME,None)      == os.path.basename(path) and
                    objr.get(FILE_METADATA,None) == metadata_for_path[path] and
                    FILE_HASHES in objr):
                    logging.info("using hash from server for %s ",path)
                    obj = {**objr, **get_file_observation(path)}
                logging.debug("does not match %s",dvs_debug_obj_str(objr))
        if obj is None:
            logging.debug("Could not find hash; hashing file")
            obj = get_file_observation_with_hash(path)
        file_objs.append(obj)
    return file_objs
