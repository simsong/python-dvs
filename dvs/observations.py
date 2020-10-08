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
import time
import shutil
import subprocess
import socket

"""
Routines for getting observations.
"""

from .dvs_constants import *
from .dvs_helpers  import *

def get_bucket_key(loc):
    """Given a location, return the (bucket,key)"""
    from urllib.parse import urlparse
    p = urlparse(loc)
    if p.scheme == 's3':
        return p.netloc, p.path[1:]
    if p.scheme == '':
        if p.path.startswith("/"):
            (ignore, bucket, key) = p.path.split('/', 2)
        else:
            (bucket, key) = p.path.split('/', 1)
        return bucket, key
    assert ValueError("{} is not an s3 location".format(loc))


def get_s3file_observation_with_remote_cache(path:str, *, search_endpoint:str, verify=True):
    """Given an S3 path,
    1. Get the metadata from AWS for the object.
    2. Given this metadata, see if the object is registered in the DVS server.
    3. If it is registered, return the re is metadata on the Object server that matches.
    3. If there is, use the hash that is already on the object server.
    4. If not, download the S3 file and hash it.
    5. Return an observation"""

    # https://stackoverflow.com/questions/52402421/retrieving-etag-of-an-s3-object-using-boto3-client

    assert isinstance(path, str)
    (bucket,key)           = get_bucket_key(path)
    s3obj     = boto3.resource( AWS_S3 ).Object( bucket, key)

    # Annoying, S3 ETags come with quotes, which we will now remove
    etag      = s3obj.e_tag
    if etag[0] == '"':
        etag = s3obj.e_tag[1:-1]

    # Create the search
    search_dicts = {1 :
                    { HOSTNAME:DVS_S3_PREFIX + bucket,
                      DIRNAME:  os.path.dirname(key),
                      FILENAME: os.path.basename(key),
                      FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                                      ST_MTIME : int(s3obj.last_modified.timestamp()),
                                      ETAG     : etag},
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
            continue

        # If any of the objects has a metadata that matches, and it has a hash, use it
        obj = None
        for result in results:
            objr = result[OBJECT]
            if (objr.get(DIRNAME,None)==dirname  and
                objr.get(FILENAME,None)==filename and
                objr.get(FILE_METADATA,None)==file_metadata and
                FILE_HASHES in objr):

                logging.info("using hash from server for %s ",path)

                # Take the old values, because it hasn't changed
                return objr
            else:
                logging.debug("Not in %s",result)

    logging.debug("Could not find hash; hashing s3 file")

    # Use the StreamingBody() to download the object.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html

    print("S3 Hashing s3://{}/{} {:,} bytes...".format(bucket,key,s3obj.content_length),file=sys.stderr)
    hashes = hash_filehandle(s3obj.get()['Body'])
    obj = {HOSTNAME: DVS_S3_PREFIX + bucket,
           DIRNAME:  os.path.dirname(key),
           FILENAME: os.path.basename(key),
           FILE_METADATA: {ST_SIZE  : s3obj.content_length,
                           ST_MTIME : int(s3obj.last_modified.timestamp()),
                           ETAG     : etag},
           FILE_HASHES: hashes}
    return obj


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
    logging.debug("Searching to see if dirname, filename, and mtime is known for any of our commits")
    hostname = socket.gethostname()
    search_dicts = {ct :
                    { HOSTNAME: hostname,
                      PATH: os.path.abspath(path),
                      DIRNAME: os.path.dirname(os.path.abspath(path)),
                      FILENAME: os.path.basename(path),
                      FILE_METADATA: json_stat(path),
                      ID : ct }
                for (ct,path) in enumerate(paths)}

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
        path = search[PATH]
        dirname = search[DIRNAME]
        filename = search[FILENAME]
        file_metadata = search[FILE_METADATA]
        response_filehash = None
        try:
            results = results_by_searchid[search_id]
        except KeyError:
            logging.debug("file %s was not found in search",search_id)
            continue
        # If any of the objects has a metadata that matches, and it has a hash, use it
        obj = None
        for result in results:
            objr = result[OBJECT]
            if (objr.get(DIRNAME,None)==dirname  and
                objr.get(FILENAME,None)==filename and
                objr.get(FILE_METADATA,None)==file_metadata and
                FILE_HASHES in objr):
                logging.info("using hash from server for %s ",path)
                # Take the old values and overwrite with new ones
                obj = {**objr, **get_file_observation(path)}
                break
            else:
                logging.debug("Not in %s",result)
        if obj is None:
            logging.debug("Could not find hash; hashing file")
            obj = get_file_observation_with_hash(path)
        file_objs.append(obj)
    return file_objs
