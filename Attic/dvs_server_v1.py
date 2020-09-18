  /mnt2/home/garfi303adm/dev/bin/dvs/Attic:
  total used in directory 8 available 2197140560
  drwxr-xr-x. 2 garfi303adm domain users 4096 Sep 18 11:45 .
  drwxr-xr-x. 6 garfi303adm domain users 4096 Sep 18 11:45 ..
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
             

