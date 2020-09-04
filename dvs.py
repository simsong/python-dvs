import os
import os.path
import requests
import urllib3
import logging
import datetime
import requests
import json
from ctools import clogging

from .dvs_constants import *

#urllib3.disable_warnings()

ENDPOINTS = {SEARCH:"https://dasexperimental.ite.ti.census.gov/api/dvs/search",
             UPDATE:"https://dasexperimental.ite.ti.census.gov/api/dvs/update"}
#VERIFY=False


VERIFY=False
if VERIFY==False:
    import urllib3
    urllib3.disable_warnings()

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
