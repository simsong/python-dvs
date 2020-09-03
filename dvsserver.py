#!/usr/bin/env python3

"""
dvsserver

"""

import os
import random
import bottle
import json
from bottle import request,response

from ctools import dbfile
from ctools import tydoc
import webmaint

def do_query(auth, query):
    cmd = "SELECT * from dvs_files WHERE "
    wheres = []
    vals   = []
    if 'pathname' in query:
        wheres.append('filename==%s ')
        vals.append(os.path.basename(query['pathname']))
    if 'sha1' in query:
        whereas.append('hashid in (select hashid from dvs_hashes where hexhash=%s)')
        vals.append(query['sha1'])
    
    if wheres:
        cmd = cmd + "AND".join(wheres) 
        return dbfile.DBMySQL.csfr(auth, cmd, vals, asDicts=True)
    return []

def do_search(auth,search):
    """API for searching"""
    results = []
    for query in search:
        found = do_query(auth,query)
        results.append({'query':query,
                        'found':found})
    response.content_type = 'text/json'
    return json.dumps(results,default=str)


def search_api(auth):
    try:
        search = json.loads(bottle.request.params.search)
    except json.decoder.JSONDecodeError:
        response.status = 404
        return "search parameter is not a valid JSON value"
    if not isinstance(search,list):
        response.status = 404
        return f"Search parameter must be a JSON-encoded list"
    return do_search(auth,search)

def search_html(auth):
    """User interface for searching"""
    (doc,main) = webmaint.get_doc()
    grid = webmaint.apply_das_template(doc,title=f'DVS Search')
    grid.add_tag_text('p','Search:')
    grid.add_tag_elems('form', [tydoc.TyTag('input',attrib={'type':'search','name':'q','value':bottle.request.params.q}),
                                    tydoc.TyTag('input',attrib={'type':'submit','class':'searchButton'})],
                           attrib={'action':bottle.request.url})

    
    
    return doc.asString()
