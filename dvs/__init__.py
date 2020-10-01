import time
import warnings
import logging
import json
import requests


from .dvs_constants import *
from .dvs_helpers import objects_dict,canonical_json
from .observations import get_s3file_observation_with_hash,get_file_observations_with_remote_cache

ENDPOINTS = {SEARCH:"https://dasexperimental.ite.ti.census.gov/api/dvs/search",
             COMMIT:"https://dasexperimental.ite.ti.census.gov/api/dvs/commit",
             DUMP:"https://dasexperimental.ite.ti.census.gov/api/dvs/dump" }

class DVS():
    def __init__(self, base={}, endpoints=ENDPOINTS, verify=False):
        """Start a DVS transaction"""
        self.the_commit    = base
        self.file_obj_dict = {} # where the file objects will end up
        self.endpoints     = endpoints
        self.t0            = time.time()
        self.verify        = verify
        pass

    def add(self, which, *, obj):
        """Basic method for adding an object to one of the lists """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        if which not in self.file_obj_dict:
            self.file_obj_dict[which] = list()
        self.file_obj_dict[which].append(obj)


    def add_s3path(self, which, s3path, extra={}, update_metadata=True):
        """Add an s3 object, possibly hashing it.
        :param which: should be COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER
        :param s3path: an S3 path (e.g. s3://bucket/path) of the object to add
        :param extra:   additional key:value pairs to be added to the object
        """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        obj = get_s3file_observation_with_hash( s3path, update_metadata=update_metadata)
        if extra:
            assert set.intersection(set(obj.keys()), set(extra.keys())) == set()
            obj = {**obj, **extra}

        self.add( which, obj = obj)


    def add_local_paths(self, which, paths, extra={}):
        """Add multiple paths using remote cache"""
        file_objs = get_file_observations_with_remote_cache(paths, search_endpoint=self.endpoints[SEARCH])
        for obj in file_objs:
            if extra:
                assert set.intersection(set(obj.keys()), set(extra.keys())) == set()
                obj = {**obj, **extra}
            self.add( which, obj=obj)


    def add_before(self, *, obj):
        return self.add(COMMIT_BEFORE, obj=obj)

    def add_method(self, *args, **kwargs):
        return self.add(COMMIT_METHOD, obj=obj)

    def add_after(self, *args, **kwargs):
        return self.add(COMMIT_AFTER, obj=obj)

    def commit(self, *args, **kwargs):
        """Continue to build the commit.
        uses:
        self.the_commit - a dictionary with the base fields.
        self.file_objec_dict - A dictionary with optional BEFORE, METHOD, and AFTER objects,
                     which will be seralized and stored as part of the transaction.
        """

        # Construct the FILE_OBJ list, which is the hexhash of the canonical JSON
        all_objects = {}
        # grab the BEFORE, METHOD, and AFTER object lists.
        for (which,file_objs) in self.file_obj_dict.items():
            assert isinstance(file_objs,list)
            assert all([isinstance(obj,dict) for obj in file_objs])
            objects       = objects_dict(file_objs)
            self.the_commit[which] = list(objects.keys())
            all_objects   = {**all_objects, **objects}

        ### DEBUG CODE START
        logging.debug("# of objects to upload: %d",len(file_objs))
        for (ct,obj) in enumerate(file_objs,1):
            logging.debug("object %d: %s",ct,obj)
        logging.debug("commit: %s",json.dumps(self.the_commit,default=str,indent=4))
        ### DEBUG CODE END

        # Send the objects and the commit
        r = requests.post(self.endpoints[COMMIT],
                          data={API_OBJECTS:canonical_json(all_objects),
                                API_COMMIT:canonical_json(self.the_commit)},
                          verify=self.verify)
        logging.debug("response: %s",r)
        if r.status_code!=HTTP_OK:
            raise RuntimeError(f"Error from server: {r.status_code}: {r.text}")

        # Return the commit object
        return r.json()

"""
TODO: Take logic in dvs.py/do_commit_send and move here.
TODO: remake dvs.py so that it uses the DVS module.
"""
