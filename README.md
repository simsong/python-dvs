# dvs
The Data Vintaging System (v2)




## Principles 

hashes refer to:
 - individual files
 - JSON objects stored in normalized JSON (e.g. sorted, no indent)
 - Foreign hashes (commit points in git repo). Stores URL where it can be resolved

JSON objects can be:
 - an observation (host, path, metadata, hash) on a file.
 - a transformation on a file (inputs (hashes), program (hashes), outputs (hashes)
 - a set of files.


## Design Goals
- The entire system can be recreated from the object store.

## Use Cases

committing a file:
 - make an observation
 - Send the observation to the server.

The server can store:
 - JSON objects, indexed by hash (and possibly other fields)
 - A number of specific tables to improve efficency.

Registering a single object (e.g. committing in git):
  - Stores the JSON  the object store.
  - e.g.:  `{"before":[h1,h2,...],"comment":'string',"time":time_t}`
  - Returns the hash of the registration.
  - e.g.: `{"commit":{"before":[h1,h2,...],"comment":'string',"time":time_t}}`

Search for the hash of the registration:
  - Get back the registration

Search for the hash of the file:
  - Get back every registration

Registering two objects at the same time: (e.g. commit a b)
  - Registers each object.
  - Registers a connection between the two registrations
  - Returns the hash of the connection.

Search for the hash of the connection:
  - Returns the set of objects and their metadata at the time of the registration

Registrering a transformation from one set of files to another set of files through a program.
  - Registers files before.
  - Registers the transformation function (likely a foreign hash to a git repo)
  - registers the files after
  - Creates JSON object {"before":[h1,h2,...], "method":[h1,h2,...}, "after":[h1,h2,...], "start":time_t, "time":time_t}
  - Returns the commit point:
     {"commit":{"before":[h1,h2,...], "method":[h1,h2,...}, "after":[h1,h2,...]}}


So the fundamental object that we store for a commit is:
```json
 { "before":[hexhashes of befores],
   "method":[hexhashes of transformers],
   "after": [hexhases of afters],
   "comment":"a string"
}
```

Every field is optional, but there must be at least one BEFORE, METHOD or AFTER hash. Object is put into cannonical form, hashed, and stored in the object store.

The fundamental transaction to the server is:
  { "objects":[{"hexhash":<object>,"hexhash":<object>}],
    "commits":[obj,obj,obj]}

The server returns the committed objects.

Notes are a commit that has a `before` and a `comment` field.   


# Examples
## Data stored for an object:
```json
 {"hashes": {"md5": "3ae9e58a7b9960539bfc8598c206ace3", 
             "sha1": "666d6346e4bf5534c205d842567e0fbe82866ba3", 
             "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 
             "sha512": "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"}, 
  "dirname": "/mnt2/home/garfi303adm/public_html/bin/dvs/tests", 
  "filename": "dvs_demo.txt", 
  "hostname": "ir7dascp001.ite.ti.census.gov", 
  "metadata": {"st_dev": 66305, 
               "st_gid": 200000, 
               "st_ino": 237900809, 
               "st_uid": 201830676, 
               "st_mode": 33188, 
               "st_rdev": 0, 
               "st_size": 118, 
               "st_ctime": 1599159172, 
               "st_mtime": 1599159172, 
               "st_nlink": 1, 
               "st_blocks": 8, 
               "st_blksize": 4096, 
               "st_ctime_ns": 1599159172249671751, 
               "st_mtime_ns": 1599159172249671751}}
```
# See also
* https://dvc.org/
* https://opendata.stackexchange.com/questions/748/is-there-a-git-for-data
* https://locallyoptimistic.com/post/git-for-data-not-a-silver-bullet/
* https://swagger.io/tools/swaggerhub/hosted-api-documentation/
