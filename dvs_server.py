#!/usr/bin/env python3

"""
dvsserver

"""

import os
import sys
import random
import json
import warnings
import time

# Get 'ctools' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Until this is properly packaged, just put . into the path
sys.path.append( os.path.dirname(__file__ ))


from ctools import dbfile
from ctools import tydoc
import webmaint

from dvs_constants import *
import helpers

def do_v1search(auth, *, search, debug=False):
    """Implements the low-level v2 search. This will change when we move to GraphQL.
    Currently the search is a dictionary that is matched against. The special wildcard SEARCH_ANY
    is matched against all possible fields. the response is a list of dictionaries of all matches.
    """
    cmd = """SELECT a.created as created,a.metadata as metadata, a.metadata_mtime as metadata_mtime,
                    b.hostname as hostname,
                    c.filename as filename,
                    d.dirname  as dirname,
                    e.hexhash as hexhash
    FROM dvs_updates a 
    NATURAL JOIN dvs_hostnames b
    NATURAL JOIN dvs_filenames c 
    NATURAL JOIN dvs_dirnames d 
    NATURAL JOIN dvs_hashes e 
    WHERE """
    search_any = search.get(SEARCH_ANY,None)
    search_any_fn  = search_any if (isinstance(search_any,str) and ('/' not in search_any)) else None
    search_any_hex = search_any.lower() if helpers.is_hexadecimal(search_any) else None
    wheres = []
    vals   = []
    if ('filename' in search) or (search_any_fn):
        if ('filename' in search) and (search_any_fn is None):
            wheres.append('a.filenameid in (select filenameid from dvs_filenames where filename=%s ) ')
            vals.append(search['filename'])
        elif ('filename' in search) and (search_any_fn is not None):
            wheres.append('a.filenameid in (select filenameid from dvs_filenames where filename=%s or filename=%s) ')
            vals.append(search['filename'])
            vals.append(search_any_fn)
        elif ('filename' not in search) and (search_any_fn is not None):
            wheres.append('a.filenameid in (select filenameid from dvs_filenames where filename=%s) ')
            vals.append(search_any_fn)
        else:
            raise RuntimeError("Logic Error")
            

    if (HEXHASH in search) or (search_any_hex):
        if (HEXHASH in search) and (search_any_hex is None):
            wheres.append('a.hashid in (select hashid from dvs_hashes where hexhash=%s)')
            vals.append(search['sha1'])
        elif (HEXHASH in search) and (search_any_hex is not None):
            wheres.append('a.hashid in (select hashid from dvs_hashes where hexhash=%s or hexhash like %s)')
            vals.append(search['sha1'])
            vals.append( search_any_hex + "%")
        elif (HEXHASH not in search) and (search_any_hex is not None):
            wheres.append('a.hashid in (select hashid from dvs_hashes where hexhash like %s)')
            vals.append( search_any_hex + "%")
        else:
            raise RuntimeError("Logic Error 2")

    
    if not wheres:
        return []
    cmd = cmd + " OR ".join(wheres) 
    return dbfile.DBMySQL.csfr(auth, cmd, vals, asDicts=True, debug=debug)


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
    if helpers.is_hexadecimal(search_any):
        search_hashes.append(search_any)
    if HEXHASH in search:
        search_hashes.append(search.get(HEXHASH))
    
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

    if search_hashes:
        wheres.extend([" (hexhash LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.hexhash')) LIKE %s) "] * len(search_hashes))
        vals.extend(search_hashes)

    if search_filenames:
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.filename')) LIKE %s) "] * len(search_filenames))
        vals.extend(search_filenames)

    if search_dirnames:
        wheres.extend([" (JSON_UNQUOTE(JSON_EXTRACT(object,'$.dirname')) LIKE %s) "] * len(search_dirnames))
        vals.extend(search_dirnames)

    if len(vals)==0:
        return []

    return dbfile.DBMySQL.csfr(auth, cmd + " OR ".join(wheres), vals, asDicts=True, debug=debug)


def get_hashid(auth, hexhash, etag):
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_hashes (hexhash,etag) values (%s,%s) ON DUPLICATE KEY UPDATE etag=etag",
                        (hexhash,etag))

    res = dbfile.DBMySQL.csfr(auth,"SELECT hashid,etag from dvs_hashes where hexhash=%s",(hexhash,))
    # handle the case where we didn't know the etag previously but we do now
    (hashid,etag_) = res[0]
    if etag_!=etag and (etag is not None):
        if len(etag_<16):
            warnings.warn("Changing hashid %d etag from %s to %s",hashid,str(etag_),str(etag))
            dbfile.DBMySQL.csfr(auth,"UPDATE dvs_hashes set etag=%s where hashid=%s",(etag,hashid))
    return hashid


def add_note(auth,*,hashid=None,hexhash=None,author,note):
    """This interface is used by the test logic. It is not used in production currently"""
    if not hashid:
        hashid = get_hashid(auth,hexhash,None)
    dbfile.DBMySQL.csfr(auth,"INSERT INTO dvs_notes (hashid,author,note) values (%s,%s,%s)",
                        (hashid, author, note))


def get_notes(auth,hexhash):
    """Right now this gets all the notes. It should probably get a set of them"""
    return dbfile.DBMySQL.csfr(auth,"SELECT * from dvs_notes where hashid=%s",
                               (get_hashid(auth,hexhash,None)),asDicts=True)

def do_update(auth, update):
    """
    """
    if HEXHASH not in update:
        return {'code':'fail',
                'reason':'hexhash not in update dictionary'}
    assert HOSTNAME in update
    assert TIME in update
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_hostnames  (hostname) values (%s) ON DUPLICATE KEY UPDATE hostname=hostname",(update[HOSTNAME],))

    hostid = dbfile.DBMySQL.csfr(auth,"SELECT hostid from dvs_hostnames where hostname=%s",
                                 (update[HOSTNAME],))[0][0]

    hashid = get_hashid(auth, update[HEXHASH], update.get(ETAG,None))


    cmd = """
        SELECT * from dvs_updates 
        WHERE hostid=%s AND hashid=%s
        """
    vals = [hostid,hashid]

    dirname = update.get(DIRNAME,None)
    if dirname:
        dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_dirnames (dirname) values (%s) ON DUPLICATE KEY UPDATE dirname=dirname", (dirname,))
        dirnameid = dbfile.DBMySQL.csfr(auth,"SELECT dirnameid from dvs_dirnames where dirname=%s",
                                        (dirname,))[0][0]
        cmd += " AND dirnameid =%s"
        vals.append(dirnameid)

    filename = update.get(FILENAME,None)
    if filename:
        dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_filenames (filename) values (%s) ON DUPLICATE KEY UPDATE filename=filename",
                            (filename,))
        filenameid = dbfile.DBMySQL.csfr(auth,"SELECT filenameid from dvs_filenames where filename=%s",
                                        (filename,))[0][0]
        cmd += " AND filenameid =%s"
        vals.append(filenameid)

    
    if dirname is None or filename is None:
        # Finish rest of generality later.
        raise RuntimeError("This implementation requires that dirname and filename be provided")

    res = dbfile.DBMySQL.csfr(auth, cmd, vals, asDicts=True)
    # If the update is not present, add it
    if len(res)==0:
        dbfile.DBMySQL.csfr(auth,
                            """
                            INSERT INTO dvs_updates (hashid,hostid,dirnameid,filenameid,metadata)
                            VALUES (%s,%s,%s,%s,%s)
                            """,
                            (hashid,hostid,dirnameid,filenameid,str(json.dumps(update[FILE_METADATA],default=str))))
        
    else:
        # Just update the first one (There shoudn't be more than one unless some were present
        # without a filename or directoryname)
        newmd = json.loads(res[0]['metadata'])
        for (key,val) in update[FILE_METADATA].items():
            newmd[key] = val
        dbfile.DBMySQL.csfr(auth,"UPDATE dvs_updates set metadata=%s,modified=now() where updateid=%s",
                            (str(json.dumps(newmd,default=str)),res[0]['updateid']))

    # If any note was provided for the hash, add it to the dvs_notes table
    if NOTE in update:
        add_note(auth,hashid=hashid,author=update.get(AUTHOR,None),note=update[NOTE])
             

def add_notes(auth,responses,debug=False):
    """Take an array of results and find all of the hexhashes. Do a single SQL query to get all of the notes for all of the hashes.
    Then add notes notes to each result."""
    hex_hashes = set()
    for response in responses:
        results = response[RESULTS]
        for result in results:
            hex_hashes.add(result[HEXHASH])
    

    if len(hex_hashes)==0:
        return                  # no notes!

    rows = dbfile.DBMySQL.csfr(auth,
                               """SELECT a.created as created,a.modified as modified,a.author as author, a.note as note, 
                                       b.hexhash as hexhash, b.etag as etag
                               FROM dvs_notes a
                               LEFT JOIN dvs_hashes b on a.hashid = b.hashid
                               WHERE b.hexhash in """ + helpers.comma_args(len(hex_hashes),parens=True) ,
                               list(hex_hashes),
                               asDicts=True,
                               debug=debug)
    # This is order n^2. It could be rewritten to be order n. Sorry.
    for response in responses:
        for result in response[RESULTS]:
            result[NOTES] = [row for row in rows if row[HEXHASH]==result[HEXHASH]]
    return

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

    add_notes(auth,responses)
    bottle.response.content_type = 'text/json'
    return json.dumps(responses,default=str)



###
### v2 object-based API
###




def store_objects(auth,objects):
    """Objects is a dictionary of key:values that will be stored. The value might be a URL or a dictionary"""
    assert isinstance(objects,dict)
    vals = []
    for (key,val) in objects.items():
        if isinstance(val,dict):
            # we were given an object to store
            val_json = helpers.canonical_json( val )
            assert key == helpers.hexhash_string( val_json )
            vals.append(key)
            vals.append(val_json)
            vals.append(None)
        elif isinstance(val,str):
            # we were given a URL to store
            vals.append(key)
            vals.append(None)
            vals.append(val)
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE INTO dvs_objects (hexhash,object,url) VALUES " 
                        + helpers.comma_args(3,rows=len(objects),parens=True), vals)

def get_objects(auth,hexhashes):
    """Returns the objects for the hexhashes. If the hexhash is a url, returns a proxy (which is a string, rather than an object)"""
    rows = dbfile.DBMySQL.csfr(auth,"SELECT * from dvs_objects where hexhash in" + helpers.comma_args(len(hexhashes),parens=True),
                        hexhashes,
                        asDicts=True)

    return {row[HEXHASH]:(json.loads(row[OBJECT]) if row[OBJECT] else row['url']) for row in rows}
    

def store_commit(auth,commit):
    """The commit is a list of 1 or more list of hashes"""
    assert isinstance(commit, dict)
    hashes = []
    for check in [BEFORE,METHOD,AFTER]:
        if check in commit:
            if not isinstance(commit[check], list):
                raise ValueError(f"{check} is not a list")
            if not all([isinstance(elem,str) for elem in commit[check]]):
                raise ValueError(f"{check} is not a list of strings")
            if not all([helpers.is_hexadecimal(elem) for elem in commit[check]]):
                raise ValueError(f"{check} contains a value that is not a hexadecimal hash")
            hashes.extend(commit[check])
    if len(hashes)==0:
        raise ValueError("Commit does not include any hexhashes in the before, method or after sections")
    # Make sure that all of the hashes are in the database
    rows = dbfile.DBMySQL.csfr(auth,
                               "SELECT COUNT(*) FROM dvs_objects where hexhash in " 
                               + helpers.comma_args(len(hashes),parens=True),
                               hashes)
    assert len(rows)==1
    if rows[0][0]!=len(hashes):
        raise ValueError(f"received {len(hashes)} hashes in commit but only {rows[0][1]} are in the local database")
    
                        
    # Add the timestamp
    commit[TIME] = time.time()

    # store it and return the object
    objects      = helpers.objects_dict([commit])
    store_objects(auth,objects)
    return objects

def dump_objects(auth,limit,offset):
    """Returns objects from offset..limit, in reverse order. If offset is NULL, start at the last"""
    cmd = "SELECT * from dvs_objects order by objectid desc "
    vals = []
    if limit:
        cmd += " LIMIT %s "
        vals.append(limit)
    if offset:
        cmd += " OFFSET %s "
        vals.append(offset)
    return dbfile.DBMySQL.csfr(auth,cmd,vals,asDicts=True)

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
        if not helpers.is_hexadecimal(key):
            bottle.response.status = 400
            return f"object key {key} is not a hexadecimal value"
        if isinstance(value,dict):
            cj = helpers.canonical_json(value)
            hh = helpers.hexhash_string(cj)
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
    """User interface for searching"""
    import bottle
    (doc,main) = webmaint.get_doc()
    grid = webmaint.apply_das_template(doc,title=f'DVS Search')
    grid.add_tag_text('p','Search:')
    grid.add_tag_elems('form', [tydoc.TyTag('input',attrib={'type':'search','name':'q','value':bottle.request.params.q}),
                                    tydoc.TyTag('input',attrib={'type':'submit','class':'searchButton'})],
                           attrib={'action':bottle.request.url})

    
    
    return doc.asString()
