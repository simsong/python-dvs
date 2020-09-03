import os
import os.path
import requests
import urllib3
import logging
import datetime
import requests
import json
from ctools import clogging
#urllib3.disable_warnings()
SEARCH='search'
UPDATE='update'
ENDPOINTS = {SEARCH:"https://dasexperimental.ite.ti.census.gov/api/dvs/search",
             UPDATE:"https://dasexperimental.ite.ti.census.gov/api/dvs/update"}
#VERIFY=False
METADATA='metadata'
DIRNAME='dirname'
FILENAME='filename'
PATHNAME='pathname'
SHA1='sha1'
BLOCKSIZE=1024*1024
VERIFY=False
if VERIFY==False:
    import urllib3
    urllib3.disable_warnings()
def clean_float(v):
    return int(v) if isinstance(v,float) else v
def json_stat(path: str) -> dict:
    s_obj = os.stat(path)
    return {k: clean_float(getattr(s_obj, k)) for k in dir(s_obj) if k.startswith('st_')}
def hash_file(fullpath):
    """Hash a file and return its sha1.
    Right now this is a 100% python
    implementation, but we should exec out to openssl for faster
    performance for large files It would be really nice to move to a
    parallelized hash.
    """
    file_hash = hashlib.sha1()
    with open(fullpath, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = f.read(BLOCKSIZE)
    return file_hash.hexdigest()
def analyze_file(path, prev_mtime=None):
    """Analyze a file and return its metadata. If prev_mtime is set and mtime hasn't changed, don't hash."""
    fullpath = os.path.abspath(path)
    obj = {METADATA : json_stat(path),
           PATHNAME : fullpath}
    if obj[METADATA].st_mtime != prev_mtime:
        obj[SHA1] = hash_file(fullpath)
    return obj
def do_register(paths, message):
    # Send the list of paths to the server and ask if the mtime for any of them are known
    print("paths:",paths)
    full_paths = [os.path.abspath(path) for path in paths]
    searches   = [{PATHNAME: os.path.abspath(path),
                   METADATA: json_stat(path)}
                  for path in paths]
    data = {'search':json.dumps(searches)}
    r = requests.post(ENDPOINTS[SEARCH], data=data, verify=VERIFY)
    logging.debug("status=%s text: %s",r.status_code,r.text)
if __name__ == "__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", nargs='*', help="One or more files or directories to process")
    parser.add_argument("-m", "--message", help="Message when registering the presence of a file")
    parser.add_argument("--dataset", help="Specifies the name of a dataset when registering a file")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--garfi303", action='store_true')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--search",   "-s", help="Search for information about the path", action='store_true')
    group.add_argument("--register", "-r", help="Register a file or path. Default action", action='store_true')
    clogging.add_argument(parser)
    args = parser.parse_args()
    clogging.setup(args.loglevel)
    if args.garfi303:
        for e in ENDPOINTS:
            ENDPOINTS[e] = ENDPOINTS[e].replace("/api","/~garfi303adm/html/api")
    print("path:",args.path)
    if args.search:
        do_search()
    else:
        do_register(args.path, args.message)
