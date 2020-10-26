import os
import os.path
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
import io
import copy

"""
This is the dvs CLI client.
This version only works with a remote server.

debug with:
python dvs.py --garfi303 --debug -m test$$ --register tests/dvs_demo.txt --loglevel DEBUG
"""

### bring in ctools if we can find it
from os.path import dirname, abspath, basename, realpath

POSSIBLE_DAS_DECENNIAL=dirname(dirname(dirname(dirname(realpath(__file__)))))
if basename(POSSIBLE_DAS_DECENNIAL)=='das_decennial':
    sys.path.append(os.path.join(POSSIBLE_DAS_DECENNIAL,'das_framework'))
    sys.path.append(os.path.join(POSSIBLE_DAS_DECENNIAL,'programs/python_dvs'))
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


import dvs
from dvs.dvs_constants import COMMIT_BEFORE as BEFORE, COMMIT_AFTER as AFTER, COMMIT_METHOD as METHOD, COMMIT_MESSAGE, COMMIT_AUTHOR, COMMIT_DATASET
from dvs.dvs_constants import LIMIT, DUMP, OFFSET, HTTP_OK, SEARCH, SEARCH_ANY, FILENAME, RESULTS, FILE_METADATA, ST_MTIME, ST_CTIME, OBJECT, DURATION, HEXHASH
from dvs.dvs_helpers  import get_file_observation_with_hash,length_of_unique_prefix
from dvs.observations import get_s3file_observation_with_remote_cache,get_file_observations_with_remote_cache,get_bucket_key

def set_debug_endpoints(prefix):
    """If called, changes the endpoints to be the debug endpoints"""
    dvs.API_ENDPOINT = dvs.API_ENDPOINT.replace("census.gov/api",f"census.gov/{prefix}/api")


def do_commit(dc, paths):
    """Given a commit and a set of paths, figure out if they are local files or s3 files, add each, and process.
    """
    try:
        dc.add_local_paths( dc.COMMIT_BEFORE, [path for path in paths if not path.startswith("s3://")] )
        dc.add_s3_paths_or_prefixes( dc.COMMIT_BEFORE, [path for path in paths if path.startswith("s3://")] )
    except FileNotFoundError as e:
        print(f"File not found: {e.args[0]} ({e.__context__})",file=sys.stderr)
        exit(1)
    return dc.commit()


def do_search(dc, paths, debug=False):
    """Ask the server to do a broad search for a string. Return the results."""
    search_list = [{SEARCH_ANY: path,
                    FILENAME: os.path.basename(path) } for path in paths]
    return dc.search(search_list)


def obj2line(c, hashlen=8):
    f = io.StringIO()
    print(c['created'],c['hexhash'][0:hashlen],end='  ',file=f)
    obj = c['object']
    if 'hostname' in obj:
        print(obj['hostname']+":",end='',file=f)
    if 'dirname' in obj:
        print(obj['dirname']+"/",end='',file=f)
    if 'filename' in obj:
        print(obj['filename'],end='',file=f)
    if 'url' in obj:
        print(obj['url'],end='',file=f)
    if 'metadata' in obj:
        m = obj['metadata']
        if 'st_size' in m:
            print(' '+str(m['st_size'])+' bytes',end='',file=f)
    if 'before' in obj:
        print(" ".join([o[0:8] for o in obj['before']]),end=' ',file=f)
    if 'method' in obj:
        print(" + [",end='',file=f)
        print(" ".join([o[0:8] for o in obj['method']]),end=' ',file=f)
        print("] ",end='',file=f)
    if 'after' in obj:
        print(" =>",end='',file=f)
        print(" ".join([o[0:8] for o in obj['after']]),end=' ',file=f)
    if 'message' in obj:
        print(obj['message'],end='',file=f)
    return f.getvalue()

def obj2str(c):
    """return an object as a nicely formatted string"""
    result = copy.copy(c)
    # Go through result and delete stuff we don't want to see and reformat what we need to reformat
    for (a,b) in ((FILE_METADATA,ST_MTIME),
                  (FILE_METADATA,ST_CTIME)):
        try:
            result[OBJECT][a][b] = time.asctime(time.localtime(int(result[OBJECT][a][b]))) + " (converted)"
        except KeyError:
            pass
    return(json.dumps(result,indent=4,default=str))
    

def shortest_prefix_for_objects(objects):
    return length_of_unique_prefix([obj[HEXHASH] for obj in objects])
    

def print_last(commits):
    for c in commits:
        print(obj2line(c, shortest_prefix_for_objects(commits)))

def render_search_result(search_results):
    search_str = search_results['search']['*']
    results = search_results[RESULTS]
    if len(results)==0:
        print("   not on file\n")
        return

    # First do objectid disambiguation
    prefixes = list()
    for result in results:
        if result[HEXHASH].startswith(search_str):
            prefixes.append(result)
    if len(prefixes)>1:
        # Many objects were returned by a search for this hexhash.
        # Display a one-line for each, with disambiguation
        print("Search disambiguation:")
        shortest = shortest_prefix_for_objects(prefixes)
        for c in prefixes:
            print(obj2line(c, shortest))
        print()
    else:
        prefixes = list()        # clear it
        
    # Next do hash disambiguation
    print("(Hash disambiguation not yet implemented.)")

    # Print the remaining
    count = 0
    for result in results:
        if result in prefixes:
            continue
        count += 1
        if count==1:
            print("Search Results:")
        elif count > 1:
            print("   ---   ")
        print(obj2str(result))
    print("")


def json_print(title,obj):
    print(title)
    print(json.dumps(obj,indent=4,default=str,sort_keys=True))


def do_cp(dc, src_path, dst_path):
    """Implement a file copy, with the fact of the copy recorded in the DVS"""

    dc.add_git_commit(src=__file__)
    use_s3 = False
    if src_path.startswith("s3://"):
        use_s3  = True
        dc.add_s3_paths(dc.COMMIT_BEFORE, [src_path])
    else:
        dc.add_local_paths(dc.COMMIT_BEFORE, [src_path])

    if dst_path.startswith("s3://"):
        use_s3  = True
        if dst_path.endswith("/"):
            dst_path += os.path.basename(src_path)
    else:
        if os.path.isdir(dst_path):
            dst_path = os.path.join(dst_path, os.path.basename(src_path))
        if os.path.exists(dst_path):
            raise FileExistsError(dst_path)

    if use_s3:
        cmd = ['aws','s3','cp',src_path,dst_path]
        print(" ".join(cmd))
        subprocess.check_call(cmd)
    else:
        shutil.copyfile(src_path,dst_path)

    if dst_path.startswith("s3://"):
        dc.add_s3_path(dc.COMMIT_AFTER, dst_path)
    else:
        dc.add_local_paths(dc.COMMIT_AFTER, [dst_path])
    return dc.commit()


if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", nargs='*', help="One or more files or directories to process")
    parser.add_argument("--message", "-m", help="Message when registering the presence of a file")
    parser.add_argument("--dataset", help="Specifies the name of a dataset when registering a file")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--git", action='store_true', help='Treat the first filename as a registered git file and add a git commit for it as well')
    parser.add_argument("--garfi303", action='store_true', help='Use the ~garfi303adm/html endpoint')
    parser.add_argument("--noverify", '--insecure', '-K', action='store_true', help='Disable certificate check')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--search",   "-s", help="Search for information about the path", action='store_true')
    group.add_argument("--register", "-r", help="Register a file or path. ", action='store_true')
    group.add_argument("--commit",   "-c", help="Commit. Synonym for register", action='store_true')
    group.add_argument("--dump",           help="Dump database. Optional arguments are LIMIT and OFFSET", action='store_true')
    group.add_argument("--cp",             help="Copy file1 to file2 and log in DVS. Also works for S3 files", action='store_true')
    group.add_argument("--last", type=int, help="print last N commits, one per line")
    if ctools is not None:
        ctools.clogging.add_argument(parser,loglevel_default='WARNING')
    args = parser.parse_args()
    if args.debug:
        args.loglevel='DEBUG'
    if ctools is not None:
        ctools.clogging.setup(args.loglevel)

    if args.garfi303:
        set_debug_endpoints("~garfi303adm/html")

    verify = True
    if args.noverify:
        import urllib3
        urllib3.disable_warnings()
        verify = False

    dc = dvs.DVS(verify=verify)

    if args.message:
        dc.set_message(args.message)

    if args.dataset:
        dc.set_dataset(args.dataset)

    if args.search:
        for search_result in do_search(dc, args.path, debug=args.debug):
            render_search_result(search_result)
    elif args.register or args.commit:
        if args.git:
            dc.add_git_commit( src=args.path[0])
        json_print( 'COMMIT', do_commit(dc, args.path))
    elif args.dump:
        limit  = int(args.path[0]) if len(args.path)>0 else None
        offset = int(args.path[1]) if len(args.path)>1 else None
        json_print( 'DUMP', dc.dump_objects(limit=limit, offset=offset))
    elif args.last:
        print_last( dc.dump_objects(limit=args.last, offset=0))
    elif args.cp:
        if len(args.path)!=2:
            print("--cp requires 2 arguments",file=sys.stderr)
            exit(1)
        json_print( do_cp(dc,args.path[0],args.path[1]))
