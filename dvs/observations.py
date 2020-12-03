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
CACHE_CHECK_S3_MIN_FILE_SIZE    = 16*1024*1024 # if the file is smaller than 16MiB, don't check the server
CACHE_CHECK_LOCAL_MIN_FILE_SIZE = 64*1024*1024 # if the file is smaller than 64MiB, don't check the server

def debug_str(s):
    """Return s if smaller than MAX_DEBUG_PRINT , otherwise print something more understandable"""
    s = str(s)
    if len(s) < MAX_DEBUG_PRINT:
        return s
    return f"{s[0:MAX_DEBUG_PRINT -30]} ... ({len(s):,} chars) ... {s[-30:]}"


def get_bucket_key(loc):
    """Given a location, return the (bucket,key)"""
    from urllib.parse import urlparse
    p = urlparse(loc)
    if p.scheme == 's3':
        return p.netloc, p.path[1:]
    assert ValueError("{} is not an s3 location".format(loc))


def get_s3path_etag_bytes(s3path):
    """Given an s3path, return a tuple of (s3path, ETag, bytes). Designed to be parallelized"""
    (bucket,key) = get_bucket_key(s3path)
    s3obj        = boto3.resource( AWS_S3 ).Object( bucket, key)
    try:
        etag     = s3obj.e_tag
        obytes   = s3obj.content_length
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code']=='404':
            raise FileNotFoundError(s3path)
        raise(e)
    # Annoying, S3 ETags come with quotes, which we will now remove. But perhaps one day they won't,
    # so check for the tag before removing it
    if etag[0] == '"':
        etag = etag[1:-1]
    return (s3path, etag, s3obj.content_length)


def server_search_post(*, search_endpoint, search_dicts, verify=DEFAULT_VERIFY):
    """Actually performs the server search. Handles search_dicts>server.MAX_SEARCH_OBJECTS"""
    # For testing, use a stride of 5
    MAX_SEARCH_OBJECTS=5
    return_list = []
    search_dict_values = list(search_dicts.values())
    for stride in range(0, len(search_dict_values), MAX_SEARCH_OBJECTS):
        stride_dicts = search_dict_values[stride:stride+MAX_SEARCH_OBJECTS]
        logging.debug("Search send: %d/%d%s", stride, len(search_dict_values), debug_str(stride_dicts))
        print(json.dumps(stride_dicts))
        r = requests.post(search_endpoint,
                          data = {'searches':json.dumps(stride_dicts, default=str)},
                          verify = verify)
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

def server_s3search(*, s3path, s3path_etag, search_endpoint, verify=DEFAULT_VERIFY ):
    (bucket,key) = get_bucket_key(s3path)
    s3obj        = boto3.resource( AWS_S3 ).Object( bucket, key)
    search_dicts = {1 :
                    { HOSTNAME:  DVS_S3_PREFIX + bucket,
                      DIRNAME:   os.path.dirname(key),
                      FILENAME:  os.path.basename(key),
                      FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                                      ST_MTIME : int(s3obj.last_modified.timestamp()),
                                      ETAG     : s3obj.e_tag},
                      ID: 1
                  }}

    # Now we want to send all of the objects to the server as a list
    logging.debug("Search send: %s",debug_str(search_dicts))
    rjson = server_search_post(search_endopint = search_endpoint,
                               search_dicts = search_dicts,
                               verify = verify)

    results_by_searchid = {response[SEARCH][ID] : response[RESULTS] for response in rjson}


    # Now we get the back and hash all of the objects for which the server has no knowledge, or for which the mtime does not agree
    file_objs = []
    for (search_id,search) in search_dicts.items():
        dirname       = search[DIRNAME]
        filename      = search[FILENAME]
        file_metadata = search[FILE_METADATA]
        response_filehash = None
        try:
            results = results_by_searchid[search_id]
        except KeyError:
            logging.debug("file %s was not found in search",search_id)
            results = []
            continue

        # If any of the objects has a metadata that matches, and it has a hash, use it
        obj = None
        for result in results:
            objr = result[OBJECT]
            if (objr.get(DIRNAME,None)==dirname  and
                objr.get(FILENAME,None)==filename and
                objr.get(FILE_METADATA,None)==file_metadata and
                FILE_HASHES in objr):

                logging.info("using hash from server for %s/%s %s ",dirname,filename,file_metadata)

                # Take the old values, because it hasn't changed
                return objr
            else:
                logging.debug("Not in %s",debug_str(result))
    return None



def hash_s3path(s3path:str):
    """Called from Pool in get_s3file_observations"""
    (bucket,key) = get_bucket_key(s3path)
    s3obj        = boto3.resource( AWS_S3 ).Object( bucket, key)
    print(f"PID {os.getpid()} S3 Hashing s3://{bucket}/{key} {s3obj.content_length:,} bytes...",file=sys.stderr)
    hashes = hash_filehandle(s3obj.get()['Body'])
    return {HOSTNAME: DVS_S3_PREFIX + bucket,
            DIRNAME:  os.path.dirname(key),
            FILENAME: os.path.basename(key),
            FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                            ST_MTIME : int(s3obj.last_modified.timestamp()),
                            ETAG     : s3obj.e_tag},
            FILE_HASHES: hashes}


def get_s3file_observations(s3paths:list, *, search_endpoint:str, verify=DEFAULT_VERIFY, threads=DEFAULT_THREADS):
    """Given an S3 path,
    1. Get the metadata from AWS for the object.
    2. Given this metadata, see if the object is registered in the DVS server.
    3. If it is registered, return the re is metadata on the Object server that matches.
    3. If there is, use the hash that is already on the object server.
    4. If not, download the S3 file and hash it.
    5. Return an observation
        """

    # https://stackoverflow.com/questions/52402421/retrieving-etag-of-an-s3-object-using-boto3-client

    if not isinstance(s3paths, list):
        raise ValueError(f"s3paths ({s3paths}) is a {type(s3paths)} and not a list.")

    # Get the ETag for all of the paths
    logging.info("getting tags for %s paths",len(s3paths))
    with Pool(threads) as p:
        rows = p.map(get_s3path_etag_bytes, s3paths)
    s3path_etags = {row[0]:row[1] for row in rows} # make paths to etags
    s3path_content_lengths = {row[0]:row[2] for row in rows} # make paths to content-lengths

    # This is (annoyingly) still single-threaded. For each object, execute a search
    # We could just do a single search for all of them..
    s3path_searches = dict()
    if search_endpoint is None:
        logging.debug("Output files. will not use cache")
    elif DVS_OBJECT_CACHE_ENV in os.environ:
        logging.debug("Running with DVS_OBJECT_CACHE. Not checking server for cached hash.")
    else:
        logging.info("Checking server for %s paths",len(s3paths))
        for s3path in s3paths:
            if s3path_content_lengths[s3path] > CACHE_CHECK_S3_MIN_FILE_SIZE:
                objr =  server_s3search(s3path=s3path, s3path_etag=s3path_etags[s3path],
                                        search_endpoint=search_endpoint, verify=DEFAULT_VERIFY)
                if objr:
                    s3path_searches[s3path] = objr
        logging.info("Got response on %s",len(s3path_searches))

    # Still need to parallelize this.
    # Use the StreamingBody() to download the object.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html
    # THis code is very similar to server_s3search above and should probably be factored into that.

    # get the objects for the s3paths that we had
    objs            = [s3path_searches[s3path] for s3path in s3paths if s3path in s3path_searches]
    s3paths_to_hash = [s3path for s3path in s3paths if s3path not in s3path_searches]

    # hash the s3paths that aren't

    logging.info("Parallel hashing of %s files",len(s3paths_to_hash))
    with Pool(threads) as p:
        objs.extend( p.map(hash_s3path, s3paths_to_hash ))

    return objs


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
