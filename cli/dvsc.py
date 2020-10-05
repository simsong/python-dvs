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

"""
This is the dvs CLI client.
This version only works with a remote server.

debug with:
python dvs.py --garfi303 --debug -m test$$ --register tests/dvs_demo.txt --loglevel DEBUG
"""

### bring in ctools if we can find it
from os.path import dirname, abspath, basename
POSSIBLE_DAS_DECENNIAL=dirname(dirname(dirname(dirname(abspath(__file__)))))
if basename(POSSIBLE_DAS_DECENNIAL)=='das_decennial':
    sys.path.append(os.path.join(POSSIBLE_DAS_DECENNIAL,'das_framework'))
try:
    import ctools.clogging
except ModuleNotFoundError:
    ctools = None
###


# import the dvs module
try:
    import dvs
except ModuleNotFoundError:
    sys.path.append(dirname(dirname(abspath(__file__))))
    import dvs

################################################################
###
### metadata plugins are functions that take a file descriptor and return a dictionary of name: value pairs
###
# for now, just import the metata plugins
sys.path.append(dirname(abspath(__file__)))
from plugins.metadata_das_header import metadata_das_header as plug1

plugins = [plug1]
################################################################


from dvs.dvs_constants import *
from dvs.dvs_helpers  import *
from dvs.observations import get_s3file_observation_with_remote_cache,get_file_observations_with_remote_cache,get_bucket_key

VERIFY=False
if VERIFY==False:
    import urllib3
    urllib3.disable_warnings()

def set_debug_endpoints(prefix):
    """If called, changes the endpoints to be the debug endpoints"""
    for e in ENDPOINTS:
        ENDPOINTS[e] = ENDPOINTS[e].replace("census.gov/api",f"census.gov/{prefix}/api")


def do_commit_local_files(commit, paths):
    """Find local files, optionally hash them, and send them to the server.
    1. Send the list of paths to the server and ask if the mtime for any of them are known
       We make a search_dictionary, which is the search object for each of the paths passed in,
       indexed by path
    2. Hash the files that are not known to the server.
    3. Send to the server a list of all of the files as a commit.
    TODO: plug-in additional hash attributes.
    """
    d = dvs.DVS( base=commit)
    d.add_local_paths( BEFORE, paths)
    return d.commit()


def do_commit_s3_files(commit, paths, update_metadata=True):
    """Get the metadata for each s3 object.
    Then do a search and ask the sever if it knows for an object with this etag.
    If the backend knows about this etag, then we don't need to hash again.
    This could be made more efficient by doing the multiple S3 actions in parallel.
    """
    s3 = boto3.resource('s3')
    d = dvs.DVS( base=commit)
    for path in paths:
        (bucket,key) = get_bucket_key(path)
        first64k = s3.Object(bucket,key).get()['Body'].read(65536)
        # get the s3 metadata
        extra = {}
        for plugin in plugins:
            extra = {**extra, **plugin(first64k)}
        d.add_s3path(BEFORE, path, extra=extra, update_metadata=update_metadata)
    return d.commit( )

def do_commit(commit, paths, update_metadata=True):
    """Given a commit and a set of paths, figure out if they are local files or s3 files, add each, and process.
    TODO: Make this work with both S3 and local files?
    """
    # Make sure all files are either local or s3
    s3count = sum([1 if path.startswith("s3://") else 0 for path in paths])
    if s3count==0:
        return do_commit_local_files(commit, paths)
    elif s3count==len(paths):
        return do_commit_s3_files(commit, paths, update_metadata=update_metadata)
    else:
        raise RuntimeError("All files to be registered must be local or on s3://")


def do_dump(limit, offset):
    dump = {}
    if limit is not None:
        dump[LIMIT] = limit
    if offset is not None:
        dump[OFFSET] = offset

    data = {'dump':json.dumps(dump, default=str)}
    r = requests.post(ENDPOINTS[DUMP],data=data,verify=VERIFY)
    if r.status_code==HTTP_OK:
        return r.json()
    raise RuntimeError(f"Error on backend: result={r.status_code}  note:\n{r.text}")


def do_search(paths, debug=False):
    """Ask the server to do a broad search for a string. Return the results."""
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
    if len(obj[RESULTS])==0:
        print("   not on file\n")
        return
    for (ct,result) in enumerate(obj[RESULTS]):
        if ct>0:
            print("   ---   ")
        # Go through result and delete stuff we don't want to see and reformat what we need to reformat
        for (a,b) in ((FILE_METADATA,ST_MTIME),
                      (FILE_METADATA,ST_CTIME)):
            try:
                result[OBJECT][a][b] = time.asctime(time.localtime(int(result[OBJECT][a][b]))) + " (converted)"
            except KeyError:
                pass
        print(json.dumps(result,indent=4,default=str))
    print("")


def print_commit(commit):
    print("COMMIT:")
    print(json.dumps(commit,indent=4,default=str,sort_keys=True))


def do_cp(commit,src_path,dst_path):
    """Implement a file copy, with the fact of the copy recorded in the DVS"""

    use_s3 = False
    t0 = time.time()
    if src_path.startswith("s3://"):
        use_s3  = True
        src_objs = [get_s3file_observation_with_remote_cache(src_path, search_endpoint=ENDPOINTS[SEARCH])]
    else:
        src_objs = get_file_observations_with_remote_cache([src_path],search_endpoint=ENDPOINTS[SEARCH])

    if dst_path.startswith("s3://"):
        use_s3  = True
        if dst_path.endswith("/"):
            dst_path += os.path.basename(src_path)
    else:
        if os.path.isdir(dst_path):
            dst_path = os.path.join(dst_path, os.path.basename(src_path))
        if os.path.exists(dst_path):
            raise FileExistsError(dst_path)

    method_obj = get_file_observation_with_hash(__file__)
    if use_s3:
        cmd = ['aws','s3','cp',src_path,dst_path]
        print(" ".join(cmd))
        subprocess.check_call(cmd)
    else:
        shutil.copyfile(src_path,dst_path)

    if dst_path.startswith("s3://"):
        dst_objs = [get_s3file_observation_with_hash(dst_path)]
    else:
        dst_objs = get_file_observations_with_remote_cache([dst_path],search_endpoint=ENDPOINTS[SEARCH])
    commit[DURATION] = time.time() - t0
    return do_commit_send(commit,{BEFORE:src_objs,
                                  METHOD:[method_obj],
                                  AFTER:dst_objs})


if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", nargs='*', help="One or more files or directories to process")
    parser.add_argument("--message", "-m", help="Message when registering the presence of a file")
    parser.add_argument("--dataset", help="Specifies the name of a dataset when registering a file")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--garfi303", action='store_true')
    parser.add_argument("--noupdate",     help="Do not update metadata on s3", action='store_true')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--search",   "-s", help="Search for information about the path", action='store_true')
    group.add_argument("--register", "-r", help="Register a file or path. ", action='store_true')
    group.add_argument("--commit",   "-c", help="Commit. Synonym for register", action='store_true')
    group.add_argument("--dump",           help="Dump database. Optional arguments are LIMIT and OFFSET", action='store_true')
    group.add_argument("--cp",             help="Copy file1 to file2 and log in DVS. Also works for S3 files", action='store_true')
    if ctools is not None:
        ctools.clogging.add_argument(parser,loglevel_default='WARNING')
    args = parser.parse_args()
    if args.debug:
        args.loglevel='DEBUG'
    if ctools is not None:
        ctools.clogging.setup(args.loglevel)

    if args.garfi303:
        set_debug_endpoints("~garfi303adm/html")

    commit = {}
    if args.message:
        commit[COMMIT_MESSAGE]= args.message
        commit[COMMIT_AUTHOR] = os.getenv('USER')
    if args.dataset:
        commit[COMMIT_DATASET] = dataset

    if args.search:
        for search in do_search(args.path, debug=args.debug):
            render_search(search)
    elif args.register or args.commit:
        print_commit( do_commit(commit, args.path, update_metadata=not args.noupdate))
    elif args.dump:
        limit  = int(args.path[0]) if len(args.path)>0 else None
        offset = int(args.path[1]) if len(args.path)>1 else None
        print_commit( do_dump(limit,offset))
    elif args.cp:
        if len(args.path)!=2:
            print("--cp requires 2 arguments",file=sys.stderr)
            exit(1)
        print_commit( do_cp(commit,args.path[0],args.path[1]))
