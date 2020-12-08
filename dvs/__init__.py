import time
import warnings
import logging
import json
import requests
import subprocess
import os
import inspect
import sys

r"""
The DVS class supports the following operations:
dc = DVS() - make an object
dc.set_message() - sets the COMMIT_MESSAGE
dc.set_author()  - sets the COMMIT_AUTHOR
dc.set_dataset() - sets the COMMIT_DATASET
dc.add(which, obj=obj) - adds an object to COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER
dc.add_git_commit(which, url=, commit=, src=) - adds a git commit to the commit
dc.add_s3_paths(which, s3paths=) - adds s3 paths
dc.add_s3_paths_or_prefixes(which, s3paths=) - adds s3 paths or prefixes
dc.add_local_paths(which, paths=) - adds local paths (filenames)
dc.commit() - writes the transaction to the local store or the remote server
dc.add_child()   - Adds a child DVS commit as a child DVS commit.
                 This allows files to be grouped together to prevent single commits with a million files.
                 Instead, you have 1000 sub-commits with 1000 files each, and then 1 commit with 1000 sub commits.

dc.set_attribute(attrib) - sets ATTRIBUTE_EPHEMERAL for the transaction and its children (allows GC according to policy)

if >1000 objects are present in a before or after, a group commit needs to be created.


"""

from .dvs_constants import *
from .dvs_helpers   import objects_dict,canonical_json,dvs_debug_obj_str
from .observations  import get_s3file_observations, get_file_observations, get_bucket_key
from .exceptions    import *

# This should be simplified to be a single API_ENDPOINT which handles v1/search v1/commit and v1/dump
# And perhaps storage endpoint where files can just be dumped. The files are text files of JSON objects, one per line, in the format:
# hexhash,<<JSON_OBJECT>>\n

API_ENDPOINT = "https://dasexperimental.ite.ti.census.gov/api/dvs"
DEFAULT_TIMEOUT = 5.0

# these get added to the endpoint
API_V1 = {SEARCH:"/v1/search",
          COMMIT:"/v1/commit",
          DUMP:"/v1/dump" }

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
    def __init__(self, base=None, api_endpoint=None, verify=DEFAULT_VERIFY, debug=False, ACL=None, timeout=DEFAULT_TIMEOUT):
        """Start a DVS transaction"""
        self.the_commit    = base if base is not None else {}
        self.file_obj_dict = {} # where the file objects will end up
        self.api_endpoint  = api_endpoint if api_endpoint is not None else API_ENDPOINT
        self.t0            = time.time()
        self.verify        = verify
        self.debug         = debug
        self.timeout       = timeout
        # Copy over select constants
        for attrib in dir(dvs_constants):
            if attrib.startswith("COMMIT") or attrib.startswith("ATTRIBUTE"):
                setattr(self,attrib,getattr(dvs_constants,attrib))
        self.ACL           = ACL # S3 ACL
        if ACL is None and DVS_AWS_S3_ACL_ENV in os.environ:
            self.ACL = os.environ[DVS_AWS_S3_ACL_ENV]
        self.children      = [] # stores tuples of (which, DVS) objects.


    def set_attribute(self, attrib, value='true'):
        """Set the attribute in the current commit. The attribute will be set in children on commis."""
        if attrib not in ATTRIBUTES:
            raise ValueError(f"{attrib} is not a valid DVS attribute")
        self.the_commit[attrib] = value

    def dump(self,file=sys.stderr):
        print("DVS object ",id(self),file=file)
        for k in dir(self):
            if not k.startswith("__"):
                print(f"   {k} = {getattr(self,k)}",file=file)

    def add_kv(self, *, key, value, overwrite=False):
        """Adds an arbitrary key/value to the commit"""
        if key in self.the_commit and not overwrite:
            if self.the_commit[key]==value:
                return
            raise KeyError(f"{key} already in the_commit")
        # Make sure it is one of our allowed
        if key not in [COMMIT_AUTHOR, COMMIT_DATASET, COMMIT_MESSAGE] and not key.startswith("x-"):
            raise ValueError(f"{key} must be a pre-defined name or start with 'x-'")
        self.the_commit[key] = value


    def set_message(self, message):
        self.add_kv(key=COMMIT_MESSAGE, value=message)

    def set_author(self, author):
        self.add_kv(key=COMMIT_AUTHOR, value=author)

    def set_dataset(self, dataset):
        self.add_kv(key=COMMIT_DATASET, value=dataset)

    def add(self, which, *, obj):
        """Basic method for adding an object to one of the lists """
        logging.debug('add(%s,%s)',which,dvs_debug_obj_str(obj))
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        if which not in self.file_obj_dict:
            self.file_obj_dict[which] = list()
        self.file_obj_dict[which].append(obj)

    def add_git_commit(self, which=COMMIT_METHOD, *, url=None, commit=None, src=None, auto=False):
        """Add a pointer to a remote URL (typically a git commit)
        :param which: which commit part this is. Either COMMIT_BEFORE, COMMIT_METHOD, or COMMIT_AFTER.
        :param url: remote URL.
        :param commit: a git SHA-1. If provided, src may not be provided.
        :param src: A file to examine to determine its git commit. If provided, commit may not be provied.
        :param auto: automatically infer src= from the caller. If provided, src= is set.
        """

        logging.debug('=== add_git_commit')
        if auto:
            src = inspect.stack()[1].filename

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

    def get_search_endpoint(self, which):
        """Returns the DVS server search endpoint, or None if we do not need to search (because these are output files)"""
        if which==COMMIT_BEFORE or which==COMMIT_METHOD:
            return self.api_endpoint + API_V1[SEARCH]
        elif which==COMMIT_AFTER:
            return None
        else:
            raise ValueError(f"which is {which} and not COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER")


    def add_s3_paths(self, which, s3paths, *, threads=DEFAULT_THREADS, extra=None):
        """Add a set of s3 objects, possibly caching.
        :param which: should we COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3paths: paths to add.
        :param threads: how many threads to use. Currently ignored.

        """
        s3objs = get_s3file_observations( s3paths, search_endpoint = self.get_search_endpoint(which), threads=threads)
        if extra is not None:
            for s3obj in s3objs:
                assert set.intersection(set(s3obj.keys()), set(extra.keys())) == set()
            s3objs = [{**s3obj, **extra} for s3obj in s3objs]

        for s3obj in s3objs:
            self.add( which, obj = s3obj)


    def add_s3_paths_or_prefixes(self, which, s3pops, *, threads=DEFAULT_THREADS, extra=None):
        """Add a path or prefix from S3. If it is a prefix, add all it contains"""
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        import boto3
        s3paths = []
        for s3pop in s3pops:
            if s3pop.endswith('/'):
                (bucket_name,prefix) = get_bucket_key(s3pop)
                paths = [f's3://{bucket_name}/{s3object.key}'
                         for s3object in boto3.resource('s3').Bucket(bucket_name).objects.page_size(100).filter(Prefix=prefix)]
                s3paths.extend(paths)
            else:
                s3paths.append(s3pop)
        self.add_s3_paths(which, s3paths, threads=threads, extra=extra)


    def add_local_paths(self, which, paths, extra=None):
        """Add multiple paths using remote cache"""

        if isinstance(paths,str):
            raise ValueError("add_local_paths takes a list of string-like objects, not a string-like object")

        # Get full path name for every file
        paths = [os.path.abspath(p) for p in paths]
        file_objs = get_file_observations(paths,
                                          search_endpoint =self.get_search_endpoint(which),
                                          verify=self.verify)
        for obj in file_objs:
            if extra is not None:
                assert set.intersection(set(obj.keys()), set(extra.keys())) == set()
                obj = {**obj, **extra}
            self.add( which, obj=obj)


    def add_child(self, which, obj):
        logging.debug('add(%s,%s)', which, obj)
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        self.children.append( (which, obj) )

    def commit(self, *args, **kwargs):
        """Continue to build the commit.
        uses:
        self.the_commit - a dictionary with the base fields.
        self.file_objec_dict - A dictionary with optional COMMIT_BEFORE, COMMIT_METHOD, and COMMIT_AFTER objects,
                     which will be seralized and stored as part of the transaction.
        :returns : a dictionary of {hexhash:commit_dict}, either generated by the server or as stored in S3.
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

        if len(all_objects)==0:
            raise DVSCommitError("Will not commit with no BEFORE, METHOD, or AFTER objects")

        ### DEBUG CODE START
        ### IS THAT SUPPOSED TO BE ONLY FOR THE LAST file_obj in the previous loop? That's the only one defined a this point
        logging.debug("# of objects to upload: %d",len(all_objects))
        for ct, obj in enumerate(all_objects, 1):
            logging.debug("object %d: %s",ct, dvs_debug_obj_str(obj))
        logging.debug("commit: %s",json.dumps(self.the_commit,default=str,indent=4))
        ### DEBUG CODE END

        # For each of the child commits:
        # 1 - make sure all of the children have the attributes of the parent.
        # 2 - commit the child and add its hexhash directly to the current commit
        for (which, child) in self.children:

            for attrib in ATTRIBUTES:
                if attrib in self.the_commit:
                    child.set_attribute( attrib, self.the_commit[attrib] )


            child_commit = child.commit()
            if which not in self.the_commit:
                self.the_commit[which] = []
            assert len(list(child_commit))==1
            self.the_commit[which].append(list(child_commit.keys())[0])

        data = {API_OBJECTS:canonical_json(all_objects),
                API_COMMIT:canonical_json(self.the_commit)}

        # If we are using the S3 object cache, then upload the object to S3 and return the object.
        if DVS_OBJECT_CACHE_ENV in os.environ:
            # https://github.com/boto/boto3/issues/894
            import boto3
            boto3.set_stream_logger('boto3.resources', logging.INFO, format_string='%(message).1600s')
            from urllib.parse import urlparse
            from hashlib import sha1

            data_bytes = canonical_json(data).encode('utf-8')
            m = sha1()
            m.update(data_bytes)
            hexhash = m.hexdigest()
            url = os.environ[DVS_OBJECT_CACHE_ENV]+'/'+hexhash
            p = urlparse(url)
            if self.ACL:
                boto3.resource('s3').Object(p.netloc, p.path[1:]).put(Body=data_bytes, ACL=self.ACL)
            else:
                boto3.resource('s3').Object(p.netloc, p.path[1:]).put(Body=data_bytes)
            return {hexhash:data}


        # Send the objects to the server and return the commit
        try:
            commit_url = self.api_endpoint + API_V1[COMMIT]
            r = requests.post(commit_url,
                              data=data,
                              verify=self.verify,
                              timeout = self.timeout)
        except requests.exceptions.Timeout:
            raise DVSServerTimeout(commit_url)

        logging.debug("response: %s",r)
        if r.status_code!=HTTP_OK:
            raise DVSServerError(f"Error from server: {r.status_code}: {r.text}")

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
        try:
            dump_url = self.api_endpoint + API_V1[DUMP]
            r = requests.post(dump_url, data=data, verify=self.verify, timeout=self.timeout)
        except requests.exceptions.Timeout as e:
            raise DVSServerTimeout(dump_url)
        if r.status_code==HTTP_OK:
            return r.json()
        raise DVSServerError(f"Error on backend: result={r.status_code}  note:\n{r.text}")

    def search(self, search_list):
        data = {'searches':json.dumps(search_list, default=str)}
        try:
            search_url = self.api_endpoint + API_V1[SEARCH]
            r = requests.post(search_url, data=data, verify=self.verify, timeout=self.timeout)
        except requests.exceptions.Timeout as e:
            raise DVSServerTimeout(search_url)

        logging.debug("status=%s text: %s",r.status_code, r.text)
        if r.status_code==HTTP_OK:
            return r.json()
        raise DVSServerError(f"Error on backend: result={r.status_code}  note:\n{r.text}")


"""
TODO: Take logic in dvs.py/do_commit_send and move here.
TODO: remake dvs.py so that it uses the DVS module.
"""
