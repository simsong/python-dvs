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

from dvs.dvs_constants import *
from dvs.dvs_helpers  import *

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


def get_s3file_observation_with_hash(path:str):
    """Given an S3 path,
    1. Get the metadata from AWS for the object.
    2. Given this metadata, see if there is metadata on the Object server that matches.
    3. If there is, use the hash that is already on the object server.
    4. If not, download the S3 file and hash it.
    5. Return an observation"""
    assert isinstance(path, str)
    s3 = boto3.resource('s3')
    (bucket,key)           = get_bucket_key(path)
    s3_object              = s3.Object(bucket,key)
    try:
        metadata_hashes    = json.loads(s3_object.metadata[AWS_METADATA_HASHES])
        metadata_st_size   = int(s3_object.metadata[AWS_METADATA_ST_SIZE])
        assert isinstance(metadata_hashes,dict)
    except (KeyError,json.decoder.JSONDecodeError):
        metadata_hashes    = None
        metadata_st_size   = None

    if ((metadata_hashes is None) or (metadata_st_size is None) or (metadata_st_size != s3_object.content_length)):
        if metadata_hashes is None:
            print(f"{path} does not have hashes in object metadata",file=sys.stderr)
        if (metadata_st_size is not None) and int(metadata_st_size) != int(s3_object.content_length):
            print(f"{path} size ({s3_object.content_length}) does not match what we previously stored in metadata ({metadata_st_size})",file=sys.stderr)
        print("Downloading and hashing s3 object",file=sys.stderr)

        # Use the StreamingBody() to download the object.
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
        # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html

        hashes = hash_filehandle(s3_object.get()['Body'])
        st_size  = s3_object.content_length
        print(f"{path} hashes {hashes}",file=sys.stderr)
        # Update the object metadata
        # https://stackoverflow.com/questions/39596987/how-to-update-metadata-of-an-existing-object-in-aws-s3-using-python-boto3
        new_metadata = {AWS_METADATA_HASHES:json.dumps(hashes,default=str),
                        AWS_METADATA_ST_SIZE: str(st_size)}

        s3_object.metadata.update(new_metadata)
        s3_object.copy_from(CopySource={'Bucket':bucket,'Key':key}, Metadata=s3_object.metadata, MetadataDirective='REPLACE')
        s3_object = s3.Object(bucket,key) # hopefully get the new object with the new mod time, but not guarenteed
        metadata_hashes = hashes          # don't bother to read it again
        metadata_st_size = st_size
    else:
        print(f"Using hashes from AWS metadata: {metadata_hashes}")
    assert isinstance(metadata_hashes,dict)
    return {HOSTNAME:'s3://' + bucket,
            DIRNAME :os.path.dirname(key),
            FILENAME:os.path.basename(path),
            FILE_HASHES: metadata_hashes,
            FILE_METADATA: {ST_SIZE: str(s3_object.content_length),
                            ST_MTIME: str(int(time.mktime(s3_object.last_modified.timetuple()))) }}


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
                    { PATH: os.path.abspath(path),
                      DIRNAME: os.path.dirname(os.path.abspath(path)),
                      FILENAME: os.path.basename(path),
                      HOSTNAME: hostname,
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
