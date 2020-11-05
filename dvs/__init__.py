import time
import warnings
import logging
import json
import requests
import subprocess
import os


from .dvs_constants import *
from .dvs_helpers import objects_dict,canonical_json
from .observations import get_s3file_observations_with_remote_cache,get_file_observations_with_remote_cache,get_bucket_key

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
    def __init__(self, base=None, api_endpoint=None, verify=True, debug=False, ACL=None):
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
        self.ACL           = ACL
        if ACL is None and DVS_AWS_S3_ACL_ENV in os.environ:
            self.ACL = os.environ[DVS_AWS_S3_ACL_ENV]


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
                commit = subprocess.check_output(['git','rev-parse','HEAD'],encoding='utf-8',
                                                 cwd=os.path.dirname(os.path.abspath(src))).strip()
                logging.debug('git commit=%s',commit)
            except subprocess.CalledProcessError as e:
                raise DVSGitException("Cannot find git installation")
        if url is None:
            try:
                url = subprocess.check_output(['git','remote','get-url','origin'],encoding='utf-8',
                                              cwd=os.path.dirname(os.path.abspath(src))).strip()
                logging.debug('git origin=%s',url)
            except subprocess.CalledProcessError as e:
                raise DVSGitException("Cannot find git installation")
        obj = { HEXHASH: commit, GIT_SERVER_URL: url}
        self.add(which, obj=obj)

    def add_s3_paths(self, which, s3paths, *, threads=1, extra=None):
        """Add a set of s3 objects, possibly caching.
        :param which: should we COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3paths: paths to add.
        :param threads: how many threads to use. Currently ignored.

        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]

        s3objs = get_s3file_observations_with_remote_cache( s3paths, search_endpoint=self.api_endpoint + API_V1[SEARCH])
        if extra is not None:
            for s3obj in s3objs:
                assert set.intersection(set(s3obj.keys()), set(extra.keys())) == set()
            s3objs = [{**s3obj, **extra} for s3obj in s3objs]

        for s3obj in s3objs:
            self.add( which, obj = s3obj)


    def add_s3_prefix(self, which, s3prefix, *, threads=DEFAULT_THREADS, page_size=100, extra=None):
        """
        Add all of the s3 objects under a prefix. We get the objects to add, then send them all to add_s3_path,
        with the hope that it will be made multithreaded at some point
        :param which: should we COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3prefix: The s3://bucket/url/ of which we should add.
        :param threads: how many threads to use.
        :param page_size: how many objects to fetch at a time; 1000 was creating errors, so we moved to 100
        :param extra: a dictionary of additional metadata to add to each object being committed
        """
        self.add_s3_paths( which, paths, threads=threads, extra=extra )


    def add_s3_paths_or_prefixes(self, which, s3pops, *, threads=DEFAULT_THREADS, extra=None):
        """Add a path or prefix from S3. If it is a prefix, add all it contains"""
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        import boto3
        s3paths = []
        for s3pop in s3pops:
            if s3pop.endswith('/'):
                (bucket_name,prefix) = get_bucket_key(s3prefix)
                paths = [f's3://{bucket_name}/{s3object.key}'
                         for s3object in boto3.resource('s3').Bucket(bucket_name).objects.page_size(100).filter(Prefix=prefix)]
                s3paths.extend(paths)
            else:
                s3paths.append(s3pop)
        return self.add_s3_paths(which, s3paths, threads=threads, extra=extra)


    def add_local_paths(self, which, paths, extra=None):
        """Add multiple paths using remote cache"""
        file_objs = get_file_observations_with_remote_cache(paths, search_endpoint=self.api_endpoint + API_V1[SEARCH],
                                                            verify=self.verify)
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

        data = {API_OBJECTS:canonical_json(all_objects),
                API_COMMIT:canonical_json(self.the_commit)}

        if DVS_OBJECT_CACHE_ENV in os.environ:
            import boto3
            # https://github.com/boto/boto3/issues/894
            boto3.set_stream_logger('boto3.resources', logging.INFO, format_string='%(message).1600s')
            from urllib.parse import urlparse
            from hashlib import md5
            data_bytes = canonical_json(data).encode('utf-8')
            print("len(data_bytes)=",len(data_bytes))
            m = md5()
            m.update(data_bytes)
            hexhash = m.hexdigest()
            url = os.environ[DVS_OBJECT_CACHE_ENV]+'/'+hexhash
            p = urlparse(url)
            if self.ACL:
                boto3.resource('s3').Object(p.netloc, p.path[1:]).put(Body=data_bytes, ACL=self.ACL)
            else:
                boto3.resource('s3').Object(p.netloc, p.path[1:]).put(Body=data_bytes)
            return data


        # Send the objects and the commit
        r = requests.post(self.api_endpoint + API_V1[COMMIT],
                          data=data,
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
        r = requests.post(self.api_endpoint + API_V1[DUMP], data=data, verify=self.verify)
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
