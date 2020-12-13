import time
import warnings
import logging
import json
import requests
import subprocess
import os
import inspect
import sys
import socket
import boto3
from urllib.parse import urlparse
from hashlib import sha1


r"""
The DVS class supports the following operations:
dc = DVS() - make an object
dc.set_message() - sets the COMMIT_MESSAGE
dc.set_author()  - sets the COMMIT_AUTHOR
dc.set_dataset() - sets the COMMIT_DATASET
dc.add(which, obj=obj) - adds an object to COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER
dc.add_git_commit(which, url=, commit=, src=) - adds a git commit to the commit
dc.add_s3_objs(which, s3objs=) - adds boto3 s3 objects
dc.add_s3_paths_or_prefixes(which, s3paths=) - adds s3 paths or prefixes
dc.add_local_paths(which, paths=) - adds local paths (filenames)
dc.commit() - writes the transaction to the local store or the remote server
dc.add_child()   - Adds a child DVS commit as a child DVS commit.
                 This allows files to be grouped together to prevent single commits with a million files.
                 Instead, you have 1000 sub-commits with 1000 files each, and then 1 commit with 1000 sub commits.

dc.set_attribute(attrib) - sets ATTRIBUTE_EPHEMERAL for the transaction and its child transactions, and all of the underlying objects. (allows GC according to policy by setting EPHEMERAL.) If a file is added with both EPHEMERAL and without, there will be two instances of it, with the same hashes, but with different hexhash.

if >1000 objects are present in a before or after, a group commit needs to be created.

TODO:
* Make which an object
* Create a class that represent each DVS object, rather than using a dictionary.


"""

from .dvs_constants import *
from .dvs_helpers   import objects_dict,canonical_json,dvs_debug_obj_str
from .observations  import get_s3objs_observations, get_file_observations, get_bucket_key, requests_retry_session
from .exceptions    import *

# This should be simplified to be a single API_ENDPOINT which handles v1/search v1/commit and v1/dump
# And perhaps storage endpoint where files can just be dumped. The files are text files of JSON objects, one per line, in the format:
# hexhash,<<JSON_OBJECT>>\n

API_ENDPOINT = "https://dasexperimental.ite.ti.census.gov/api/dvs"
DEFAULT_TIMEOUT = 10.0

debug_server = True

# these get added to the endpoint
API_V1 = {SEARCH: "/v1/search",
          COMMIT: "/v1/commit",
          DUMP  : "/v1/dump" }

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
    def __init__(self, base=None, api_endpoint=None, verify=DEFAULT_VERIFY,
                 debug=False, ACL=None, timeout=DEFAULT_TIMEOUT, options=dict()):
        """Start a DVS transaction"""
        self.the_commit    = base if base is not None else {}
        self.file_obj_dict = {} # where the file objects will end up
        self.api_endpoint  = api_endpoint if api_endpoint is not None else API_ENDPOINT
        self.t0            = time.time()
        self.verify        = verify
        self.debug         = debug
        self.timeout       = timeout
        self.options       = options
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
        if (attrib not in ATTRIBUTES) and (not attrib.lower().startswith("x-")):
            raise ValueError(f"{attrib} is not a valid DVS attribute")
        self.the_commit[attrib] = value

    def set_option(self, option, value='true'):
        if option not in OPTIONS:
            raise ValueError(f"{option} is not a valid DVS option")
        self.options[option] = value


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
        assert isinstance(obj, dict)
        if which not in self.file_obj_dict:
            self.file_obj_dict[which] = list()
        self.file_obj_dict[which].append(obj)

        if len(self.file_obj_dict[which]) > MAX_OBJECTS_LIST:
            if OPTION_NO_AUTO_SUB_COMMIT in self.options:
                raise DVSTooManyObjects(f"len(file_obj_dict[{which}])={(len(self.file_obj_dict[which]))} and OPTION_NO_AUTO_SUB_COMMIT set")

    def add_git_commit(self, which=COMMIT_METHOD, *, url=None, commit=None, src=None, auto=False):
        """Add a pointer to a remote URL (typically a git commit)
        :param which: which commit part this is. Either COMMIT_BEFORE, COMMIT_METHOD, or COMMIT_AFTER.
        :param url: remote URL.
        :param commit: a git SHA-1. If provided, src may not be provided.
        :param src: A file to examine to determine its git commit. If provided, commit may not be provied.
        :param auto: automatically infer src= from the caller. If provided, src= is set.
        """

        logging.debug('=== add_git_commit ===')
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
        if OPTION_SEARCH not in self.options:
            return None

        if (which==COMMIT_BEFORE
            or which==COMMIT_METHOD
            or (which==COMMIT_AFTER and OPTION_SEARCH_FOR_AFTERS in self.options)):
            return self.api_endpoint + API_V1[SEARCH]
        elif which==COMMIT_AFTER:
            return None
        else:
            raise ValueError(f"which is {which} and not COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER")


    def add_s3_objs(self, which, s3objs, *, threads=DEFAULT_THREADS, extra=None):
        """Add a set of s3 objects, possibly caching.
        :param which: COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3objs: boto3.resources.factory.s3.Object objects.
        :param threads: how many threads to use for hashing.
        :param extra: a dictionary of extra information to provide

        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        assert isinstance(s3objs, list)
        assert isinstance(threads, int)
        s3objs = get_s3objs_observations( s3objs, search_endpoint = self.get_search_endpoint(which), threads=threads)
        if extra is not None:
            assert isinstance(extra, dict)
            for s3obj in s3objs:
                assert set.intersection(set(s3obj.keys()), set(extra.keys())) == set()
            s3objs = [{**s3obj, **extra} for s3obj in s3objs]

        for s3obj in s3objs:
            self.add( which, obj = s3obj)


    def add_s3_paths_or_prefixes(self, which, s3pops, *, threads=DEFAULT_THREADS, extra=None):
        """
        Add a path or prefix from S3. If it is a prefix, add all of the s3 objects underneath.
        Internally, we get the boto3.resource.factory.s3.Object and put those in the array.
        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        s3objs = []
        for s3pop in s3pops:
            (bucket_name, prefix) = get_bucket_key(s3pop)
            bucket = boto3.resource('s3').Bucket(bucket_name)
            if prefix.endswith('/'):
                s3objs.extend( bucket.objects.page_size(100).filter(Prefix=prefix).limit(MAX_S3_FILES-1))
            else:
                s3objs.append( boto3.resource('s3').Object(bucket_name, prefix) )
            if len(s3objs) >= MAX_S3_FILES:
                print(f"** ERROR **",file=sys.stderr)
                print(f"add_s3_paths_or_prefixes asked to add > {MAX_S3_FILES} S3 objects. Break this into smaller transactions.",file=sys.stderr)
                print(f"prefix              s3 objects")
                for pre in s3pops:
                    (b,p) = get_bucket_key(pre)
                    print(f"{pre}    {len(list(bucket.objects.page_size(100).filter(Prefix=p)))}")
                raise ValueError(f"Too many s3 objects added to commit")
        self.add_s3_objs(which, s3objs, threads=threads, extra=extra)


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


    def add_child(self, which, child):
        logging.debug('add(%s,%s)', which, child)
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        self.children.append( (which, child) )

    def commit(self, *args, **kwargs):
        """Continue to build the commit.
        uses:
        self.the_commit - a dictionary with the base fields.
        self.file_objec_dict - A dictionary with optional COMMIT_BEFORE, COMMIT_METHOD, and COMMIT_AFTER objects,
                     which will be seralized and stored as part of the transaction.
        :returns : a dictionary of {hexhash:commit_dict}, either generated by the server or as stored in S3.
        """

        # Scan the objects being commited
        for which in set([COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]).intersection(self.file_obj_dict.keys()):

            # Repeat while we have too many children
            while len(self.file_obj_dict[which]) > MAX_OBJECTS_LIST:

                # If we do not automatically make children, abort
                if OPTION_NO_AUTO_SUB_COMMIT in self.options:
                    raise DVSTooManyObjects(f"len(file_obj_dict[{which}])={(len(self.file_obj_dict[which]))} and OPTION_NO_AUTO_SUB_COMMIT set")

                # Move all of the objects to the child list.
                children = []
                while len(self.file_obj_dict[which]) > 0:
                    child = DVS()
                    for i in range( min(MAX_OBJECTS_LIST, len(self.file_obj_dict[which]))):
                        child.add( which, obj=self.file_obj_dict[which].pop())
                    children.append(child)
                # Now add all of the children
                for child in children:
                    self.add_child( which, child)
                # If we added more than 1000 children, we will loop and children when be made children of new children.
                # This will scale to any number of objects, and the final children all will be at the same depth.

            # Add attributes in commit to the BEFORE, METHOD and AFTER objects
            for attrib in set(ATTRIBUTES).intersection(self.the_commit.keys()):
                for obj in self.file_obj_dict[which]:
                    obj[attrib] = self.the_commit[attrib]
            # The attributes will be added to the children below

        # Construct the FILE_OBJ list, which is the hexhash of the canonical JSON of all the objects
        all_objects = {}

        # grab the COMMIT_BEFORE, COMMIT_METHOD, and COMMIT_AFTER object lists.
        for which, file_objs in self.file_obj_dict.items():
            assert isinstance(file_objs,list)
            assert all([isinstance(obj,dict) for obj in file_objs])
            objects       = objects_dict(file_objs)
            self.the_commit[which] = list(objects.keys())
            all_objects   = {**all_objects, **objects}

        if len(all_objects)==0 and len(self.children)==0:
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
            boto3.set_stream_logger('boto3.resources', logging.INFO, format_string='%(message).1600s')

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
            if debug_server:
                print(f"POST {commit_url} data={str(data)[0:160]}... "
                      f"(total {len(str(data))} bytes; {len(json.loads(data[API_OBJECTS]))} objects, 1 commit)",
                      file=sys.stderr)
            r = requests_retry_session().post(commit_url,
                              data    = data,
                              verify  = self.verify,
                              timeout = self.timeout)
            if debug_server:
                print(f"RESPONSE: {r} len(r.text)={len(r.text)}\n",file=sys.stderr)
        except (requests.exceptions.Timeout, socket.timeout) as e:
            print(str(e),file=sys.stderr)
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
            r = requests_retry_session().post(dump_url, data=data, verify=self.verify, timeout=self.timeout)
        except requests.exceptions.Timeout as e:
            raise DVSServerTimeout(dump_url)
        if r.status_code==HTTP_OK:
            return r.json()
        raise DVSServerError(f"Error on backend: result={r.status_code}  note:\n{r.text}")

    def search(self, search_list, limit=dvs_constants.API_SEARCH_LIMIT):
        data = {'searches':json.dumps(search_list, default=str),
                'limit':limit}
        try:
            search_url = self.api_endpoint + API_V1[SEARCH]
            r = requests_retry_session().post(search_url, data=data, verify=self.verify, timeout=self.timeout)
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
