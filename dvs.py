import os
import os.path
import requests
import urllib3
import logging
import datetime
import requests
import json
import sys

"""
debug with:
python dvs.py --garfi303 --debug -m test$$ --register tests/dvs_demo.txt --loglevel DEBUG
"""


# Get 'ctools' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Until this is properly packaged, just put . into the path
sys.path.append( os.path.dirname(__file__ ))

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

        
def do_register(paths, *, message=None, dataset=None):
    # Send the list of paths to the server and ask if the mtime for any of them are known
    # We make a search_dictionary, which is the search object for each of the paths passed in,
    # indexed by path
    search_dicts = {ct : { PATH: os.path.abspath(path),
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

    # Finally, send the updates to the server with the message
    # If there is only a single update, send the message with it. 
    # If there are multiple updates and a message or a dataset, create an update for the dataset, give the dataset
    # that message, and send it as well
    if len(updates)!=1:
        raise RuntimeError("Currently we handle just a single dataset")
    
    logging.debug("updates:\n %s",json.dumps(updates,indent=4))
    r = requests.post(ENDPOINTS[UPDATE], 
                      data={'updates':json.dumps(updates, default=str)},
                      verify=VERIFY)
    logging.debug("response: %s",r)
    if r.status_code!=HTTP_OK:
        raise RuntimeError("Error from server: %s" % r)

def do_search(paths, debug=False):
    """Ask the server to do a broad search"""
    search_list = [{SEARCH_ANY: path, 
                    FILENAME: os.path.basename(path) } for path in paths]
    data = {'searches':json.dumps(search_list, default=str)}
    if debug:
        data['debug'] = 'True'
    r = requests.post(ENDPOINTS[SEARCH], 
                      data=data, 
                      verify=VERIFY)
    logging.debug("status=%s text: %s",r.status_code, r.text)
    return r.json()
    


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
        set_debug_endpoints("~garfi303adm/html/")

    print("path:",args.path)
    if args.search:
        for search in do_search(args.path):
            print(f"{search}:")
            print(render_search(search))
            print("")
    else:
        do_register(args.path, message=args.message, dataset=args.dataset)
