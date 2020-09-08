#!/usr/bin/env python3

"""
dvsserver

"""

import os
import sys
import random
import bottle
import json
import warnings
from bottle import request,response

# Get 'ctools' into the path
sys.path.append( os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Until this is properly packaged, just put . into the path
sys.path.append( os.path.dirname(__file__ ))


from ctools import dbfile
from ctools import tydoc
import webmaint

from dvs_constants import *

def ishexhash(val):
    if isinstance(val,str):
        hexset = set('0123456789abcdefABCDEF')
        return all([ch in hexset for ch in val])
    return False

def do_search(auth, *, search, debug=False):
    """Implements the low-level search. This will change when we move to GraphQL.
    Currently the search is a dictionary that is matched against. The special wildcard SEARCH_ANY
    is matched against all possible fields. the response is a list of dictionaries of all matches.
    """
    cmd = """SELECT a.created as created,a.metadata as metadata, a.metadata_mtime as metadata_mtime,
                    b.filename as filename,
                    c.dirname  as dirname,
                    d.hexhash as hexhash
    FROM dvs_updates a 
    LEFT JOIN dvs_filenames b on a.filenameid = b.filenameid 
    LEFT JOIN dvs_dirnames c on a.dirnameid = c.dirnameid
    LEFT JOIN dvs_hashes d on a.hashid = d.hashid
    WHERE """
    search_any = search.get(SEARCH_ANY,None)
    search_any_fn  = search_any if (isinstance(search_any,str) and ('/' not in search_any)) else None
    search_any_hex = search_any.lower() if ishexhash(search_any) else None
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
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_hosts  (hostname) values (%s) ON DUPLICATE KEY UPDATE hostname=hostname",(update[HOSTNAME],))

    hostid = dbfile.DBMySQL.csfr(auth,"SELECT hostid from dvs_hosts where hostname=%s",
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
                            (hashid,hostid,dirnameid,filenameid,str(json.dumps(update[METADATA],default=str))))
        
    else:
        # Just update the first one (There shoudn't be more than one unless some were present
        # without a filename or directoryname)
        newmd = json.loads(res[0]['metadata'])
        for (key,val) in update[METADATA].items():
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
    

    rows = dbfile.DBMySQL.csfr(auth,
                               """SELECT a.created as created,a.modified as modified,a.author as author, a.note as note, 
                                       b.hexhash as hexhash, b.etag as etag
                               FROM dvs_notes a
                               LEFT JOIN dvs_hashes b on a.hashid = b.hashid
                               WHERE b.hexhash in (""" + ",".join(["%s"]*len(hex_hashes)) + ")",
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
    try:
        searches = json.loads(bottle.request.params.searches)
    except json.decoder.JSONDecodeError:
        response.status = 404
        if len(bottle.request.params.searches)==0:
            return f"searches parameter was not supplied"
        return f"searches parameter ({bottle.request.params.searches}) is not a valid JSON value"
    if not isinstance(searches,list):
        response.status = 404
        return f"Searches parameter must be a JSON-encoded list"
    if any([isinstance(obj,dict) is False for obj in searches]):
        response.status = 404
        return f"Searches parameter must be a JSON-encoded list of dictionaries"
    responses = [{SEARCH:search,
                 RESULTS:do_search(auth,search=search, debug=bottle.request.params.debug)} for search in searches]

    add_notes(auth,responses)
    response.content_type = 'text/json'
    return json.dumps(responses,default=str)

def fix_update(update):
    if HOSTNAME not in update:
        update[HOSTNAME] = request.remote_addr
    if TIME not in update:
        update[TIME] = int(time.time())
    return update

def update_api(auth):
    """Bottle interface for updates."""
    try:
        updates = json.loads(bottle.request.params.updates)
    except json.decoder.JSONDecodeError:
        response.status = 404
        return "update parameter is not a valid JSON value"
    if not isinstance(updates,list):
        response.status = 404
        return f"Update parameter must be a JSON-encoded list"
    if any([isinstance(obj,dict) is False for obj in updates]):
        response.status = 404
        return f"Update parameter must be a JSON-encoded list of dictionaries"
    results = [{'id':update.get('id',0),
                RESULT:do_update(auth, fix_update(update))} for update in updates]
    return json.dumps(results,default=str)


def search_html(auth):
    """User interface for searching"""
    (doc,main) = webmaint.get_doc()
    grid = webmaint.apply_das_template(doc,title=f'DVS Search')
    grid.add_tag_text('p','Search:')
    grid.add_tag_elems('form', [tydoc.TyTag('input',attrib={'type':'search','name':'q','value':bottle.request.params.q}),
                                    tydoc.TyTag('input',attrib={'type':'submit','class':'searchButton'})],
                           attrib={'action':bottle.request.url})

    
    
    return doc.asString()
