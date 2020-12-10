"""
Constants used by the DVS system.
This is still under active design and development. We try to make this understandable.
The goal here is to avoid having quoted constants in the source code
"""


import multiprocessing

HTTP_OK=200
DEFAULT_THREADS = multiprocessing.cpu_count()
DEFAULT_VERIFY=True             # for https

DVS_S3_PREFIX='s3://'

# https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
AWS_S3='s3'
AWS_HEADER_CONTENT_LENGTH="Content-Length"
AWS_HEADER_CONTENT_TYPE="Content-Type"
AWS_HEADER_CONNECTIOn="Connection"
AWS_HEADER_DATE="Date"
AWS_HEADER_ETAG="Etag"
AWS_HEADER_SERVER="Server"

# Environment variables
# Object cache: If this variable is defined, just put the objects there, and do not talk to the server
DVS_OBJECT_CACHE_ENV='DVS_OBJECT_CACHE' # S3 location object cache
DVS_AWS_S3_ACL_ENV='DVS_AWS_S3_ACL'     # ACL to specify when writing to object cache

# Limits
MAX_OBJECTS_LIST = 1000         # throw an error if >1000 objects in BEFORE, METHOD, or AFTER


ID='id'

# File objects
FILE_METADATA='metadata'
FILE_HASHES='hashes'
ETAG='etag'
ST_MTIME='st_mtime'             # mtime as time_t
ST_MTIME_NS='st_mtime_ns'       # mtime as time_t * 10E9
ST_ATIME='st_atime'
ST_CTIME='st_ctime'
ST_SIZE='st_size'
MD5='md5'
SHA1='sha1'
SHA256='sha256'
SHA512='sha512'


# Search API
SEARCH='search'
OBJECT='object'

# Object properties

# Commit
COMMIT='commit'                 # commit endpoint
COMMIT_BEFORE='before'          # commit: list of hashes before the commit
COMMIT_AFTER='after'            # commit: list of hashes after the commit
COMMIT_MESSAGE='message'        # commit: message with the commit
COMMIT_METHOD='method'          # commit: list of hashes for programs that produced the commit
GIT_SERVER_URL='url'
METADATA_MTIME='metadata_mtime'
DIRNAME='dirname'
FILENAME='filename'
NOTE='note'
NOTES='notes'                   # an array of NOTE objects
COMMIT_AUTHOR='author'                 # commit: author of commit
COMMIT_DATASET='datasets'              # commit: name of the dataset
HOSTNAME='hostname'                    #
IPADDR='ipaddr'                 # commit: ipaddress of object
PATH='path'
HEXHASH='hexhash'
HEXHASH_ALG='sha1'              # which algorithm we are using
SEARCH_ANY='*'

# Attributes
ATTRIBUTE_EPHEMERAL="ephemeral"
ATTRIBUTES=set([ATTRIBUTE_EPHEMERAL])

# Options
OPTION_NO_AUTO_SUB_COMMIT='no_auto_sub_commit' # do not automatically create sub-commits
OPTION_SEARCH='search'          # search for observations
OPTION_SEARCH_FOR_AFTERS='search_for_afters'   # search for afters in cache as well as befores and methods
OPTIONS=set([OPTION_SEARCH, OPTION_NO_AUTO_SUB_COMMIT, OPTION_SEARCH_FOR_AFTERS])


# This is a duplicate SEARCH='search'                 # takes a single dict
REMOTE_FQDN='remote_fqdn'       # fqdn observed of remote ip address
REMOTE_ADDR='remote_addr'       # remote address for a commit
RESULT='result'                 # a single dict
RESULTS='results'               # a list of dicts
CREATED='created'
TIME='time'                     # commit - when method was finished
DURATION='duration'             # commit - how long the commit took (so we don't have a clock skew issue)


# Commit API
API_OBJECTS='objects'
API_COMMIT='commit'
API_SEARCH_LIMIT=100            # don't return more than 100 objects

# Dump
DUMP='dump'
LIMIT='limit'
OFFSET='offset'
