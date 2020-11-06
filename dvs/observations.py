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

DEFAULT_THREADS=40

def get_bucket_key(loc):
    """Given a location, return the (bucket,key)"""
    from urllib.parse import urlparse
    p = urlparse(loc)
    if p.scheme == 's3':
        return p.netloc, p.path[1:]
    assert ValueError("{} is not an s3 location".format(loc))


def get_s3path_etag(s3path):
    """Given an s3path, return a tuple of (s3path, ETag). Designed to be parallelized"""
    (bucket,key) = get_bucket_key(s3path)
    s3obj        = boto3.resource( AWS_S3 ).Object( bucket, key)
    try:
        etag      = s3obj.e_tag
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code']=='404':
            raise FileNotFoundError(s3path)
        abort
    # Annoying, S3 ETags come with quotes, which we will now remove. But perhaps one day they won't,
    # so check for the tag before removing it
    if etag[0] == '"':
        etag = etag[1:-1]
    return (s3path, etag)

def server_s3search(*, s3path, s3path_etag,search_endpoint, verify=True ):
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
    logging.debug("Search send: %s",str(search_dicts))
    r = requests.post(search_endpoint,
                      data={'searches':json.dumps(list(search_dicts.values()), default=str)},
                      verify=verify)
    if r.status_code!=HTTP_OK:
        raise RuntimeError("Server response: %d %s" % (r.status_code,r.text))

    try:
        results_by_searchid = {response[SEARCH][ID] : response[RESULTS] for response in r.json()}
    except json.decoder.JSONDecodeError as e:
        print("Invalid response from server for search request: ",r,file=sys.stderr)
        raise RuntimeError


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
                logging.debug("Not in %s",result)
    return None



def get_s3file_observations_with_remote_cache(s3paths:list, *, search_endpoint:str, verify=True, threads=DEFAULT_THREADS):
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
    with Pool(threads) as p:
        s3path_etags = dict(p.map(get_s3path_etag, s3paths))

    # This is (annoyingly) still single-threaded. For each object, execute a search
    s3path_searches = dict()
    if DVS_OBJECT_CACHE_ENV in os.environ:
        logging.debug("Running with DVS_OBJECT_CACHE. Not checking server for cached hash.")
    else:
        for s3path in s3paths:
            objr =  server_s3search(s3path=s3path, s3path_etag=s3path_etags[s3path],
                                    search_endpoint=search_endpoint, verify=verify)
            if objr:
                s3path_searches[s3path] = objr

    # Still need to parallelize this.
    # Use the StreamingBody() to download the object.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html
    # THis code is very similar to server_s3search above and should probably be factored into that.

    objs = []
    for s3path in s3paths:
        if s3path in s3path_searches:
            objs.append(s3path_searches[s3path])
        else:
            (bucket,key) = get_bucket_key(s3path)
            s3obj        = boto3.resource( AWS_S3 ).Object( bucket, key)
            print("S3 Hashing s3://{}/{} {:,} bytes...".format(bucket,key,s3obj.content_length),file=sys.stderr)
            hashes = hash_filehandle(s3obj.get()['Body'])
            objr = {HOSTNAME: DVS_S3_PREFIX + bucket,
                    DIRNAME:  os.path.dirname(key),
                    FILENAME: os.path.basename(key),
                    FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                                    ST_MTIME : int(s3obj.last_modified.timestamp()),
                                    ETAG     : s3obj.e_tag},
                    FILE_HASHES: hashes}
            objs.append(objr)
    return objs


# Note: get_file_observations_with_remote_cache is similar to function above,
# except it pipelines multiple searches at once.
def get_file_observations_with_remote_cache(paths:list, *, search_endpoint:str, verify=True):
    """Create a list of file observations for a list of paths.
    1. Send the list of paths to the server and ask if the mtime for any of them are known
       We make a search_dictionary, which is the search object for each of the paths passed in,
       indexed by path
    2. Hash the files that are not known to the server.
    3. Return the list of observation objects.
    """
    assert isinstance(paths,list)
    assert all([isinstance(path,str) for path in paths])

    # Get the metadata for each path once.
    metadata_for_path = {path:json_stat(path) for path in paths}

    if DVS_OBJECT_CACHE_ENV in os.environ:
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
                    for (ct,path) in enumerate(paths)}
        results_by_searchid = {}

        # Now we want to send all of the objects to the server as a list
        logging.debug("Search send: %s",str(search_dicts))
        r = requests.post(search_endpoint,
                          data={'searches':json.dumps(list(search_dicts.values()), default=str)},
                          verify=verify)
        if r.status_code!=HTTP_OK:
            raise RuntimeError("Server response: %d %s" % (r.status_code,r.text))

        try:
            results_by_path = {response[SEARCH][PATH] : response[RESULTS] for response in r.json()}
        except json.decoder.JSONDecodeError as e:
            print("Invalid response from server for search request: ",r,file=sys.stderr)


    # Now we get the back and hash all of the objects for which the server has no knowledge, or for which the mtime does not agree
    file_objs = []
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
        if obj is None:
            logging.debug("Could not find hash; hashing file")
            obj = get_file_observation_with_hash(path)
        file_objs.append(obj)
    return file_objs
