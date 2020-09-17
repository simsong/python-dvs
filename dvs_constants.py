HTTP_OK=200

AWS_METADATA_SHA1='uscb-object-sha1' # the hash on the S3 file
AWS_METADATA_ST_MTIME='uscb-object-st_mtime' # the time on the file
AWS_METADATA_ST_SIZE='uscb-object-st_size' # the size of the object
ID='id'

# Search API
SEARCH='search'
BEFORE='before'                 # commit: list of hashes before the commit
COMMIT='commit'                 # commit endpoint
AFTER='after'                   # commit: list of hashes after the commit
MESSAGE='message'               # commit: message with the commit
METHOD='method'                 # commit: list of hashes for programs that produced the commit
METADATA='metadata'
METADATA_MTIME='metadata_mtime'
DIRNAME='dirname'
FILENAME='filename'
NOTE='note'
NOTES='notes'                   # an array of NOTE objects
AUTHOR='author'                 # commit: author of commit
DATASET='datasets'              # commit: name of the dataset
HOSTNAME='hostname'
PATH='path'
TIME='time'
HEXHASH='hexhash'
ETAG='etag'
ST_MTIME='st_mtime'             # mtime as time_t 
ST_MTIME_NS='st_mtime_ns'       # mtime as time_t * 10E9
ST_ATIME='st_atime'
ST_CTIME='st_ctime'
ST_SIZE='st_size'
SEARCH_ANY='*'
SEARCH='search'                 # takes a single dict
REMOTE_ADDR='remote_addr'       # remote address for a commit
RESULT='result'                 # a single dict
RESULTS='results'               # a list of dicts
CREATED='created'



# Dump
DUMP='dump'
LIMIT='limit'
OFFSET='offset'
