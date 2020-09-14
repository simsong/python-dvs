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

"""
debug with:
python dvs.py --garfi303 --debug -m test$$ --register tests/dvs_demo.txt --loglevel DEBUG
"""


# Get 'ctools' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Until this is properly packaged, just put . into the path
sys.path.append( os.path.dirname(__file__ ))

import ctools
import ctools.s3
from ctools import clogging

from dvs_constants import *
from helpers import *

#urllib3.disable_warnings()

ENDPOINTS = {SEARCH:"https://dasexperimental.ite.ti.census.gov/api/dvs/search",
             UPDATE:"https://dasexperimental.ite.ti.census.gov/api/dvs/update"}
#VERIFY=False

VERIFY=False
if VERIFY==False:
    import urllib3
    urllib3.disable_warnings()

def set_debug_endpoints(prefix):
    """If called, changes the endpoints to be the debug endpoints"""
    for e in ENDPOINTS:
        ENDPOINTS[e] = ENDPOINTS[e].replace("census.gov/api",f"census.gov/{prefix}/api")


class SingletonCounter():
    class __SingletonCounter:
        def __init__(self):
            self.count = 0
        def __str__(self):
            return repr(self) + str(self.count)
    instance = None
    def __init__(self):
        if not SingletonCounter.instance:
            SingletonCounter.instance = SingletonCounter.__SingletonCounter()
    
    @classmethod
    def next_id(self):
        sc = SingletonCounter()
        sc.instance.count += 1
        return sc.instance.count

def do_register_updates(updates, *, note=None, dataset=None):
    """Send the updates with a given note and dataset"""
    # Finally, send the updates to the server with the note
    # If there is only a single update, send the note with it. 
    # If there are multiple updates and a note or a dataset, create an update for the dataset, give the dataset
    # that note, and send it as well
    if len(updates)!=1:
        raise RuntimeError("Currently we handle just a single dataset")
    
    if note:
        updates[0][NOTE] = note
        updates[0][AUTHOR] = os.getenv('USER')
    if dataset:
        updates[0][DATASET] = dataset

    logging.debug("updates:\n %s",json.dumps(updates,indent=4))
    r = requests.post(ENDPOINTS[UPDATE], 
                      data={'updates':json.dumps(updates, default=str)},
                      verify=VERIFY)
    logging.debug("response: %s",r)
    if r.status_code!=HTTP_OK:
        raise RuntimeError("Error from server: %s" % r)


def do_register_s3_files(paths, *, note=None, dataset=None):
    #
    # Get the metadata for each s3 object.
    # If the backend does not know the hash (ie: it doesn't know the etag),
    # then we need to download the file and hash it.  It seems that sometimes
    # an object can be updated without our knowing it. We tried storing the object
    # modification_time in the object, but when we updated it, the modification time
    # got updated, so that didn't work. It would be nice if we could store a version number,
    # but we would have the same versioning issue. I guess we just need to trust users
    # to be careful about updating the objects in place. 
    #
    updates = []
    s3_objects = {}
    s3 = boto3.resource('s3')
    for path in paths:
        (bucket,key) = ctools.s3.get_bucket_key(path)
        s3_object              = s3.Object(bucket,key)
        try:
            metadata_hexhash   = s3_object.metadata[AWS_METADATA_SHA1]
            metadata_st_size   = int(s3_object.metadata[AWS_METADATA_ST_SIZE])
        except KeyError:
            metadata_hexhash   = None
            metadata_st_size   = None
            
        if ((metadata_hexhash is None) or (metadata_st_size is None) or (metadata_st_size != s3_object.content_length)):
            if metadata_hexhash is None:
                print(f"{path} does not have a SHA1 in object metadata",file=sys.stderr)
            if (metadata_st_size is not None) and int(metadata_st_size) != int(s3_object.content_length):
                print(f"{path} size ({s3_object.content_length}) does not match what we previously stored in metadata ({metadata_st_size})",file=sys.stderr)
            print("Downloading and hashing s3 object",file=sys.stderr)
            hashes = hash_filehandle(s3_object.get()['Body'])
            print(f"{path} sha1: {hashes}",file=sys.stderr)
            metadata_hexhash  = hashes[HEXHASH]
            metadata_st_size  = s3_object.content_length

            # Update the object metadata
            # https://stackoverflow.com/questions/39596987/how-to-update-metadata-of-an-existing-object-in-aws-s3-using-python-boto3
            new_metadata = {AWS_METADATA_SHA1:metadata_hexhash,
                            AWS_METADATA_ST_SIZE: str(metadata_st_size)}
            s3_object.metadata.update(new_metadata)
            s3_object.copy_from(CopySource={'Bucket':bucket,'Key':key}, Metadata=s3_object.metadata, MetadataDirective='REPLACE')
            s3_object = s3.Object(bucket,key) # hopefully get the new object with the new mod time, but not guarenteed
        

        updates.append({HOSTNAME:'s3://' + bucket,
                        DIRNAME :os.path.dirname(key),
                        FILENAME:os.path.basename(path),
                        HEXHASH: metadata_hexhash,
                        METADATA: {ST_SIZE: str(s3_object.content_length),
                                   ST_MTIME: str(int(time.mktime(s3_object.last_modified.timetuple())))
                                   }})
                        
    # Can we use https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.ObjectSummary.get
    # the StreamingBody() and do multiple gets in the background?
    # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html

    return do_register_updates(updates, note=note, dataset=dataset)


def do_register_local_files(paths, *, note=None, dataset=None):
    # Send the list of paths to the server and ask if the mtime for any of them are known
    # We make a search_dictionary, which is the search object for each of the paths passed in,
    # indexed by path
    search_dicts = {ct : 
                    { PATH: os.path.abspath(path),
                      DIRNAME: os.path.dirname(os.path.abspath(path)),
                      FILENAME: os.path.basename(path),
                      METADATA: json_stat(path),
                      ID : ct }
                for (ct,path) in enumerate(paths)}

    # Now we want to send all of the objects to the server as a list
    r = requests.post(ENDPOINTS[SEARCH], 
                      data={'searches':json.dumps(list(search_dicts.values()), default=str)}, 
                      verify=VERIFY)
    logging.debug("status=%s text: %s",r.status_code,r.text)
    responses = {response[SEARCH][ID] : response for response in r.json()}
    logging.debug("responses:\n%s",json.dumps(responses,indent=4))

    # Now we get the back and hash all of the objects for which the server has no knowledge, or for which the mtime does not agree
    updates = []
    for search in search_dicts.values():
        if search[ID] in responses:
            if HEXHASH in responses[search[ID]]:
                response_mtime = responses[search[ID]].get(METADATA_MTIME,None)
            else:
                response_mtime = None
            updates.append(get_file_update( search[PATH], response_mtime))

    return do_register_updates(updates, note=note, dataset=dataset)

def do_register(paths, *, note=None, dataset=None):
    # Make sure all files are either local or s3
    s3count = sum([1 if path.startswith("s3://") else 0 for path in paths])
    if s3count==0:
        return do_register_local_files(paths,note=note,dataset=dataset)
    if s3count==len(paths):
        return do_register_s3_files(paths,note=note,dataset=dataset)
    raise RuntimeError("All files to be registered must be local or on s3://")

    


def do_search(paths, debug=False):
    """Ask the server to do a broad search"""
    search_list = [{SEARCH_ANY: path, 
                    FILENAME: os.path.basename(path) } for path in paths]
    data = {'searches':json.dumps(search_list, default=str)}
    if debug:
        data['debug'] = 'True'
        print("ENDPOINT: ",ENDPOINTS[SEARCH],file=sys.stderr)
        print("DATA: ",json.dumps(data,indent=4),file=sys.stderr)
    r = requests.post(ENDPOINTS[SEARCH], 
                      data=data, 
                      verify=VERIFY)
    logging.debug("status=%s text: %s",r.status_code, r.text)
    if r.status_code==HTTP_OK:
        return r.json()
    raise RuntimeError(f"Error on backend: result={r.status_code}  note:\n{r.text}")
    
def render_search(obj):
    count = 0
    FMT = "{:>20}: {:<}"
    print(f"{obj[SEARCH][FILENAME]}:")
    if len(obj[RESULTS])==0:
        print("   not on file\n")
        return
    for result in obj[RESULTS]:
        if count>0:
            print("   ---   ")
        for (k,v) in sorted(result.items()):
            if k==NOTES:
                continue
            elif k==METADATA:
                for (kk,vv) in json.loads(v).items():
                    if kk==ST_SIZE:
                        print(FMT.format('size',vv))
                    elif kk==ST_MTIME:
                        print(FMT.format('mtime',time.asctime(time.localtime(int(vv)))))
                    elif kk==ST_CTIME:
                        print(FMT.format('ctime',time.asctime(time.localtime(int(vv)))))
            elif k==METADATA_MTIME:
                print(FMT.format(METADATA_MTIME,time.asctime(time.localtime(int(v)))))
            else:
                print(FMT.format(k,v))
        if NOTES in result:
            for note in sorted(result[NOTES],key=lambda note:note[CREATED]):
                print(note[CREATED],note[AUTHOR],note[NOTE])
        count += 1
        
    print("")


if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", nargs='*', help="One or more files or directories to process")
    parser.add_argument("-m", "--message", help="Note when registering the presence of a file")
    parser.add_argument("--dataset", help="Specifies the name of a dataset when registering a file")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--garfi303", action='store_true')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--search",   "-s", help="Search for information about the path", action='store_true')
    group.add_argument("--register", "-r", help="Register a file or path. Default action", action='store_true')
    clogging.add_argument(parser,loglevel_default='WARNING')
    args = parser.parse_args()
    clogging.setup(args.loglevel)

    if args.garfi303:
        set_debug_endpoints("~garfi303adm/html/")

    if args.search:
        for search in do_search(args.path,debug=args.debug):
            render_search(search)
    else:
        do_register(args.path, note=args.message, dataset=args.dataset)
