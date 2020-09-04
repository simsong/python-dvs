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

def do_search(auth, *, search):
    cmd = "SELECT * from dvs_updates WHERE "
    wheres = []
    vals   = []
    if 'filename' in search:
        wheres.append('filenameid in (select filenameid from dvs_filenames where filename=%s )')
        vals.append(search['filename'])
    if HEXHASH in search:
        whereas.append('hashid in (select hashid from dvs_hashes where hexhash=%s)')
        vals.append(search['sha1'])
    
    if wheres:
        cmd = cmd + "AND".join(wheres) 
        return dbfile.DBMySQL.csfr(auth, cmd, vals, asDicts=True)
    return []

def get_hashid(auth, hexhash, etag):
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_hashes (hexhash,etag) values (%s,%s)",
                        (hexhash,etag))

    res = dbfile.DBMySQL.csfr(auth,"SELECT hashid,etag from dvs_hashes where hexhash=%s",(hexhash,))
    # handle the case where we didn't know the etag previously but we do now
    (hashid,etag_) = res[0]
    if etag_!=etag and (etag is not None):
        if len(etag_<16):
            warnings.warn("Changing hashid %d etag from %s to %s",hashid,str(etag_),str(etag))
            dbfile.DBMySQL.csfr(auth,"UPDATE dvs_hashes set etag=%s where hashid=%s",(etag,hashid))
    return hashid


def do_update(auth, update):
    """
    """
    if HEXHASH not in update:
        return {'code':'fail',
                'reason':'hexhash not in update dictionary'}
    assert HOSTNAME in update
    assert TIME in update
    dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_hosts  (hostname) values (%s)",(update[HOSTNAME],))

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
        dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_dirnames (dirname) values (%s)", (dirname,))
        dirnameid = dbfile.DBMySQL.csfr(auth,"SELECT dirnameid from dvs_dirnames where dirname=%s",
                                        (dirname,))[0][0]
        cmd += " AND dirnameid =%s"
        vals.append(dirnameid)

    filename = update.get(FILENAME,None)
    if filename:
        dbfile.DBMySQL.csfr(auth,"INSERT IGNORE into dvs_filenames (filename) values (%s)",
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

             

def add_note(auth,hexhash,author,note):
    dbfile.DBMySQL.csfr(auth,"INSERT INTO dvs_notes (hashid,author,note) values (%s,%s,%s)",
                        (get_hashid(auth,hexhash,None), author, note))

def get_notes(auth,hexhash):
    """Right now this gets all the notes. It should probably get a set of them"""
    return dbfile.DBMySQL.csfr(auth,"SELECT * from dvs_notes where hashid=%s",
                               (get_hashid(auth,hexhash,None)),asDicts=True)

def search_api(auth):
    """Bottle interface for search. Keep everything that has to do with bottle here so that we can implement unit tests"""
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
    results = [{'search':search,
                'result':do_search(auth,search=search)} for search in searches]
    response.content_type = 'text/json'
    return json.dumps(results,default=str)

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
                'result':do_update(auth, fix_update(update))} for update in updates]
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
