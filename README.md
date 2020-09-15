# dvs
The Data Vintaging System


## Principles

hashes refer to:
 - individual files
 - JSON objects stored in normalized JSON (e.g. sorted, no indent)
 - Foreign hashes (commit points in git repo). Stores URL where it can be resolved

JSON objects can be:
 - an observation (host, path, metadata, hash) on a file.
 - a transformation on a file (inputs (hashes), program (hashes), outputs (hashes)
 - a set of files.


## Use Cases

committing a file:
 - make an observation
 - Send the observation to the server.

The server can store:
 - JSON objects, indexed by hash (and possibly other fields)
 - A number of specific tables to improve efficency.

Registering a single object (e.g. committing in git):
  - Stores the JSON  the object store.
  - {"after":[h1,h2,...],"comment":'string',"time":time_t}
  - Returns the hash of the registration.
  - {"commit":{"after":[h1,h2,...],"comment":'string',"time":time_t}}

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


So the fundamental object that we store is:
 { "before":[hexhashes of befores],
   "method":[hexhashes of transformers],
   "after":[hexhases of afters],
   "comment":"a string"
}

The fundamental transaction to the server is:
  { "objects":[{"hexhash":<object>,"hexhash":<object>}],
    "commits":[obj,obj,obj]}

Every field is optional. Object is put into cannonical form, hashed, and stored in the object store.

Design goal: The entire system can be recreated from the object store.
   