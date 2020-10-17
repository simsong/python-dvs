import time
import warnings
import logging
import json
import requests
import subprocess
import os


from .dvs_constants import *
from .dvs_helpers import objects_dict,canonical_json
from .observations import get_s3file_observation_with_remote_cache,get_file_observations_with_remote_cache,get_bucket_key

# This should be simplified to be a single API_ENDPOINT which handles v1/search v1/commit and v1/dump
# And perhaps storage endpoint where files can just be dumped. The files are text files of JSON objects, one per line, in the format:
# hexhash,<<JSON_OBJECT>>\n

API_ENDPOINT = "https://dasexperimental.ite.ti.census.gov/api/dvs"

# these get added to the endpoint
API_V1 = {SEARCH:"/v1/search",
          COMMIT:"/v1/commit",
          DUMP:"/v1/dump" }

class DVSException(Exception):
    """Base class for DVS Exceptions"""
    pass

class DVSGitException(DVSException):
    pass

class DVS_Singleton:
    """The Python singleton pattern. There are many singleton objects,
    but they all reference the same embedded object,
    which responds to all getattr requests"""

    instance = None
    def __init__(self, **kwargs):
        if DVS_Singleton.instance is None:
            DVS_Singleton.instance = DVS(**kwargs)
    def __getattr__(self, name):
        return getattr(DVS_Singleton.instance, name)

class DVS():
    def __init__(self, base=None, api_endpoint=None, verify=True, debug=False):
        """Start a DVS transaction"""
        self.the_commit    = base if base is not None else {}
        self.file_obj_dict = {} # where the file objects will end up
        self.api_endpoint  = api_endpoint if api_endpoint is not None else API_ENDPOINT
        self.t0            = time.time()
        self.verify        = verify
        self.debug         = debug
        self.COMMIT_BEFORE = COMMIT_BEFORE
        self.COMMIT_AFTER  = COMMIT_AFTER
        self.COMMIT_METHOD = COMMIT_METHOD
        self.COMMIT_AUTHOR = COMMIT_AUTHOR
        self.COMMIT_DATASET= COMMIT_DATASET

    def add_kv(self, *, key, value, overwrite=False):
        """Adds an arbitrary key/value to the commit"""
        if key in self.the_commit and not overwrite:
            if self.the_commit[key]==value:
                return
            raise KeyError(f"{key} already in the_commit")
        if not key.startswith("x-"):
            raise ValueError(f"{key} must start with 'x-'")
        self.the_commit[key] = value


    def set_message(self, message):
        self.add_kv(key=COMMIT_MESSAGE, value=message)

    def set_author(self, author):
        self.add_kv(key=COMMIT_AUTHOR, value=author)

    def set_dataset(self, dataset):
        self.add_kv(key=COMMIT_DATASET, value=dataset)


    def add(self, which, *, obj):
        """Basic method for adding an object to one of the lists """
        logging.debug('add(%s,%s)',which,obj)
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        if which not in self.file_obj_dict:
            self.file_obj_dict[which] = list()
        self.file_obj_dict[which].append(obj)


    def add_git_commit(self, *, which=COMMIT_METHOD, url=None, commit=None, src=None):
        logging.debug('=== add_git_commit')
        if commit is None and src is None:
            raise RuntimeError("either commit or src must be provided")
        if commit is not None and src is not None:
            raise RuntimeError("both commit or src may not be provided")
        if src is not None:
            # ask git for the path of the commit for src
            logging.debug('**** src=%s',src)
            try:
                commit = subprocess.check_output(['git','rev-parse','HEAD'],encoding='utf-8',cwd=os.path.dirname(os.path.abspath(src))).strip()
                logging.debug('git commit=%s',commit)
            except subprocess.CalledProcessError as e:
                raise DVSGitException("Cannot find git installation")
        if url is None:
            try:
                url = subprocess.check_output(['git','remote','get-url','origin'],encoding='utf-8',cwd=os.path.dirname(os.path.abspath(src))).strip()
                logging.debug('git origin=%s',url)
            except subprocess.CalledProcessError as e:
                raise DVSGitException("Cannot find git installation")
        obj = { HEXHASH: commit, GIT_SERVER_URL: url}
        self.add(which, obj=obj)

    def add_s3_path(self, which, s3path, *, extra=None):
        """Add an s3 object, possibly hashing it.
        :param which: should be COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3path: an S3 path (e.g. s3://bucket/path) of the object to add
        :param extra:   additional key:value pairs to be added to the object
        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        obj = get_s3file_observation_with_remote_cache( s3path, search_endpoint=self.api_endpoint + API_V1[SEARCH])
        if extra is not None:
            assert set.intersection(set(obj.keys()), set(extra.keys())) == set()
            obj = {**obj, **extra}

        self.add( which, obj = obj)

    def add_s3_paths(self, which, s3paths, *, extra=None):
        """Add a set of s3 objects, possibly caching.
        :param s3paths: paths to add.
        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        for s3path in s3paths:
            self.add_s3path( which, s3path)


    def add_s3_prefix(self, which, s3prefix, *, threads=1, extra=None):
        """Add all of the s3 objects under a prefix."""
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        import boto3
        s3_client = boto3.client('s3')
        (bucket,key) = get_bucket_key(s3prefix)
        objs = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1000)
        keys = [r['Key'] for r in objs['Contents']]
        for k in keys:
            path = 's3://' + bucket + '/' + k
            self.add_s3_path( which, path, extra=extra )


    def add_s3_paths_or_prefixes(self, which, s3pops, *, threads=1, extra=None):
        """Add a path or prefix from S3. If it is a prefix, add all it contains"""
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        for s3pop in s3pops:
            if s3pop.endswith('/'):
                return self.add_s3_prefix(which, s3pop, threads=threads, extra=extra)
            else:
                return self.add_s3_path(which, s3pop, extra=extra)


    def add_local_paths(self, which, paths, extra=None):
        """Add multiple paths using remote cache"""
        file_objs = get_file_observations_with_remote_cache(paths, search_endpoint=self.api_endpoint + API_V1[SEARCH])
        for obj in file_objs:
            if extra is not None:
                assert set.intersection(set(obj.keys()), set(extra.keys())) == set()
                obj = {**obj, **extra}
            self.add( which, obj=obj)


    def add_before(self, *, obj):
        return self.add(COMMIT_BEFORE, obj=obj)


    def add_method(self, *args, obj, **kwargs):
        return self.add(COMMIT_METHOD, obj=obj)


    def add_after(self, *args, obj, **kwargs):
        return self.add(COMMIT_AFTER, obj=obj)

    def commit(self, *args, **kwargs):
        """Continue to build the commit.
        uses:
        self.the_commit - a dictionary with the base fields.
        self.file_objec_dict - A dictionary with optional COMMIT_BEFORE, COMMIT_METHOD, and COMMIT_AFTER objects,
                     which will be seralized and stored as part of the transaction.
        returns the object for the commit.
        """

        # Construct the FILE_OBJ list, which is the hexhash of the canonical JSON
        all_objects = {}
        # grab the COMMIT_BEFORE, COMMIT_METHOD, and COMMIT_AFTER object lists.
        for which, file_objs in self.file_obj_dict.items():
            assert isinstance(file_objs,list)
            assert all([isinstance(obj,dict) for obj in file_objs])
            objects       = objects_dict(file_objs)
            self.the_commit[which] = list(objects.keys())
            all_objects   = {**all_objects, **objects}

        ### DEBUG CODE START
        ### IS THAT SUPPOSED TO BE ONLY FOR THE LAST file_obj in the previous loop? That's the only one defined a this point
        logging.debug("# of objects to upload: %d",len(file_objs))
        for ct, obj in enumerate(file_objs, 1):
            logging.debug("object %d: %s",ct, obj)
        logging.debug("commit: %s",json.dumps(self.the_commit,default=str,indent=4))
        ### DEBUG CODE END

        # Send the objects and the commit
        r = requests.post(self.api_endpoint + API_V1[COMMIT],
                          data={API_OBJECTS:canonical_json(all_objects),
                                API_COMMIT:canonical_json(self.the_commit)},
                          verify=self.verify)
        logging.debug("response: %s",r)
        if r.status_code!=HTTP_OK:
            raise RuntimeError(f"Error from server: {r.status_code}: {r.text}")

        # Return the commit object
        return r.json()

    def dump_objects(self, *, limit=None, offset=None):
        """Request the last N objects from the server. Low-level primitive"""
        dump_request = {}
        if limit is not None:
            dump_request[LIMIT] = limit
        if offset is not None:
            dump_request[OFFSET] = offset

        data = {'dump':json.dumps(dump_request, default=str)}
        r = requests.post(self.api_endpoint + API_V1[DUMP], data=data,verify=self.verify)
        if r.status_code==HTTP_OK:
            return r.json()
        raise RuntimeError(f"Error on backend: result={r.status_code}  note:\n{r.text}")

    def search(self, search_list):
        data = {'searches':json.dumps(search_list, default=str)}
        r = requests.post(self.api_endpoint + API_V1[SEARCH], data=data, verify=self.verify)
        logging.debug("status=%s text: %s",r.status_code, r.text)
        if r.status_code==HTTP_OK:
            return r.json()
        raise RuntimeError(f"Error on backend: result={r.status_code}  note:\n{r.text}")


"""
TODO: Take logic in dvs.py/do_commit_send and move here.
TODO: remake dvs.py so that it uses the DVS module.
"""
