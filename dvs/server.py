#!/usr/bin/env python3

"""
dvs server:
Functions that support the server.

"""

import os
import sys
import random
import json
import warnings
import time

###
# Get 'ctools' into the path.
# This assumes a layout on the DAS dashboard.
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from ctools import dbfile
from ctools import tydoc
#
###


from dvs_constants import *
import dvs_helpers

###
### v2 object-based API
###


def do_v2search(auth, *, search, debug=False):
    """Implements the low-level v2 search. This will change when we move to GraphQL.
    Currently the search is a dictionary that is matched against. The special wildcard SEARCH_ANY
    is matched against all possible fields. the response is a list of dictionaries of all matches.
    Right now there is no indexing on the objects. We may wish to create an index for the properties that we care about.
    Perhaps we should have used MongoDB?
    """
    cmd = """SELECT objectid,created,hexhash,object,url from dvs_objects where """
    wheres = []
    vals   = []
    search_any = search.get(SEARCH_ANY,None)
    search_hashes = []
    if dvs_helpers.is_hexadecimal(search_any):
        search_hashes.append(search_any + "%") # add the wildcard
    if HEXHASH in search:
        search_hashes.append(search.get(HEXHASH)+"%") # add the wildcard

    search_filenames = []
    if search_any:
        search_filenames.append(search_any)
    if FILENAME in search:
        search_filenames.append(search.get(FILENAME))

    search_dirnames = []
    if search_any:
        search_dirnames.append(search_any)
    if DIRNAME in search:
        search_dirnames.append(search.get(FILENAME))

    search_hostnames = []
    if search_any:
        search_hostnames.append(search_any)
    if HOSTNAME in search:
        search_hostnames.append(search.get(HOSTNAME))

    if search_hashes:
        wheres.extend([" (hexhash LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hashes.md5')) LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hashes.sha1')) LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hashes.sha256')) LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hashes.sha512')) LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)

    if search_filenames:
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.filename')) LIKE %s) "] * len(search_filenames))
        vals.extend(search_filenames)

    if search_dirnames:
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.dirname')) LIKE %s) "] * len(search_dirnames))
        vals.extend(search_dirnames)

    if search_hostnames:
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hostname')) LIKE %s) "] * len(search_dirnames))
        vals.extend(search_dirnames)

    if len(vals)==0:
        return []

    rows = dbfile.DBMySQL.csfr(auth, cmd + " OR ".join(wheres), vals, asDicts=True, debug=debug)
    # load the JSON...
    return [{**row, **{OBJECT:json.loads(row[OBJECT])}} for row in rows]



def search_api(auth):
    """Bottle interface for search. Keep everything that has to do with bottle here so that we can implement unit tests.
    The search request is a list of searches. Each search is a dict that is matched.
    The response is a list of dicts. Each dict contains the search array and a list of the search responses.
    """
    import bottle
    try:
        searches = json.loads(bottle.request.params.searches)
    except json.decoder.JSONDecodeError:
        bottle.response.status = 404
        if len(bottle.request.params.searches)==0:
            return f"searches parameter was not supplied"
        return f"searches parameter ({bottle.request.params.searches}) is not a valid JSON value"
    if not isinstance(searches,list):
        bottle.response.status = 404
        return f"Searches parameter must be a JSON-encoded list"
    if any([isinstance(obj,dict) is False for obj in searches]):
        bottle.response.status = 404
        return f"Searches parameter must be a JSON-encoded list of dictionaries"
    responses = [{SEARCH:search,
                 RESULTS:do_v2search(auth,search=search, debug=bottle.request.params.debug)}
                 for search in searches]

    bottle.response.content_type = 'text/json'
    return json.dumps(responses,default=str)



def store_objects(auth,objects):
    """Objects is a dictionary of key:values that will be stored. The value might be a URL or a dictionary"""
    assert isinstance(objects,dict)
    if len(objects)==0:
        return
    vals = []
    for (key,val) in objects.items():
        if isinstance(val,dict):
            # we were given an object to store
            val_json = dvs_helpers.canonical_json( val )
            assert key == dvs_helpers.hexhash_string( val_json )
            vals.append(key)
            vals.append(val_json)
            vals.append(None)
        elif isinstance(val,str):
            # we were given a URL to store
            vals.append(key)
            vals.append(None)
            vals.append(val)
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE INTO dvs_objects (hexhash,object,url) VALUES "
                        + dvs_helpers.comma_args(3,rows=len(objects),parens=True), vals)

def get_objects(auth,hexhashes):
    """Returns the objects for the hexhashes. If the hexhash is a url, returns a proxy (which is a string, rather than an object)"""
    rows = dbfile.DBMySQL.csfr(auth,"SELECT * from dvs_objects where hexhash in" + dvs_helpers.comma_args(len(hexhashes),parens=True),
                        hexhashes,
                        asDicts=True)

    return {row[HEXHASH]:(json.loads(row[OBJECT]) if row[OBJECT] else row['url']) for row in rows}


def store_commit(auth,commit):
    """The commit is an object that has hashes in COMMIT_BEFORE, COMMIT_METHOD, or COMMIT_AFTER
fields. Make sure they are valid hashes and refer to objects in our
database. If so, add a timestamp, store it in the object store, and
return the object. It's returned as a list in case we want to be able
to support multiple commits in the future, and because all of our
other methods return lists of objects
    """
    assert isinstance(commit, dict)
    hashes = []
    for check in [COMMIT_BEFORE,COMMIT_METHOD,COMMIT_AFTER]:
        if check in commit:
            objlist = commit[check];
            if not isinstance(objlist, list):
                raise ValueError(f"{check} is not a list")
            if not all([isinstance(elem,str) for elem in objlist]):
                raise ValueError(f"{check} is not a list of strings")
            if not all([dvs_helpers.is_hexadecimal(elem) for elem in objlist]):
                raise ValueError(f"{check} contains a value that is not a hexadecimal hash")
            hashes.extend(objlist)
    if len(hashes)==0:
        raise ValueError("Commit does not include any hexhashes in the before, method or after sections")
    # Make sure that all of the hashes are in the database
    rows = dbfile.DBMySQL.csfr(auth,
                               "SELECT COUNT(*) FROM dvs_objects where hexhash in "
                               + dvs_helpers.comma_args(len(hashes),parens=True),
                               hashes)
    assert len(rows)==1
    if rows[0][0]!=len(hashes):
        raise ValueError(f"received {len(hashes)} hashes in commit but only {rows[0][1]} are in the local database")

    # Add the timestamp
    commit[TIME] = time.time()

    # store it and return the object
    objects      = dvs_helpers.objects_dict([commit])
    store_objects(auth,objects)
    return objects

def dump_objects(auth,limit,offset):
    """Returns objects from offset..limit, in reverse order. If offset is NULL, start at the last"""
    cmd = "SELECT hexhash,created,JSON_UNQUOTE(object) as object,url from dvs_objects order by objectid desc "
    vals = []
    if limit:
        cmd += " LIMIT %s "
        vals.append(limit)
    if offset:
        cmd += " OFFSET %s "
        vals.append(offset)
    rows = dbfile.DBMySQL.csfr(auth,cmd,vals,asDicts=True)
    # load the JSON...
    return [{**row, **{OBJECT:json.loads(row[OBJECT])}} for row in rows]

def commit_api(auth):
    """Bottle interface for commits."""
    import bottle
    # Decode and validate the arguments
    # First validate the objects
    try:
        objects = json.loads(bottle.request.params.objects)
    except json.decoder.JSONDecodeError:
        bottle.response.status = 400
        return f"objects parameter is not a valid JSON value"
    if not isinstance(objects,dict):
        bottle.response.status = 400
        return f"objects parameter is not a JSON-encoded dictionary"
    for (key,value) in objects.items():
        if not dvs_helpers.is_hexadecimal(key):
            bottle.response.status = 400
            return f"object key {key} is not a hexadecimal value"
        if isinstance(value,dict):
            cj = dvs_helpers.canonical_json(value)
            hh = dvs_helpers.hexhash_string(cj)
            if key != hh:
                bottle.response.status = 400
                return f"object key {key} has a computed hash of {hh}"
        elif instance(value,str):
            if ":" not in value:
                bottle.response.status = 400
                return f"object key {key} value is not a URL"
        else:
            return f"object key {key} is not a dict or a string"


    # Now validate the commit
    try:
        commit = json.loads(bottle.request.params.commit)
    except json.decoder.JSONDecodeError:
        bottle.response.status = 400
        return f"commit parameter is not a valid JSON value"
    if not isinstance(commit,dict):
        bottle.response.status = 400
        return f"commit parameter is not a JSON-encoded dictionary"
    for (key,value) in objects.items():
        if not isinstance(key,str):
            bottle.response.status=400
            return f"commit key {key} is not a string"

    # Paramters look good. Store the objects.
    # Todo: this should be done atomically, with a single SQL transaction.

    # Now store the new objects
    store_objects(auth,objects)

    commit[REMOTE_ADDR] = bottle.request.remote_addr

    # Now store the commit as another object
    commit_obj = store_commit(auth, commit)
    return json.dumps( commit_obj,default=str)


def dump_api(auth):
    """API for dumping"""
    import bottle

    try:
        dump  = json.loads(bottle.request.params.dump)
    except json.decoder.JSONDecodeError:
        bottle.response.status = 400
        return f"dump parameter is not a valid JSON value"

    if LIMIT in dump:
        try:
            limit = int(dump[LIMIT])
        except ValueError:
            bottle.response.status = 400
            return "limit must be an integer."
    else:
        limit = None

    if OFFSET in dump:
        try:
            offset = int(dump[OFFSET])
        except ValueError:
            bottle.response.status = 400
            return "offset must be an integer."
    else:
        offset = None

    return json.dumps( dump_objects(auth,limit,offset), default=str)


def search_html(auth):
    """User interface for searching. This is only run on the DAS dashboard"""
    import bottle
    import webmaint

    (doc,main) = webmaint.get_doc()
    grid = webmaint.apply_das_template(doc,title=f'DVS Search')
    grid.add_tag_text('p','Search:')
    grid.add_tag_elems('form', [tydoc.TyTag('input',attrib={'type':'search','name':'q','value':bottle.request.params.q}),
                                    tydoc.TyTag('input',attrib={'type':'submit','class':'searchButton'})],
                           attrib={'action':bottle.request.url})



    return doc.asString()