"""
Constants used by the DVS system.
This is still under active design and development. We try to make this understandable.
The goal here is to avoid having quoted constants in the source code
"""


HTTP_OK=200

DVS_S3_PREFIX='s3://'

# https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
AWS_S3='s3'
AWS_HEADER_CONTENT_LENGTH="Content-Length"
AWS_HEADER_CONTENT_TYPE="Content-Type"
AWS_HEADER_CONNECTIOn="Connection"
AWS_HEADER_DATE="Date"
AWS_HEADER_ETAG="Etag"
AWS_HEADER_SERVER="Server"


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
BEFORE='before'                 # commit: list of hashes before the commit

# Object properties

# Commit
COMMIT='commit'                 # commit endpoint
COMMIT_BEFORE='before'                 # commit: list of hashes before the commit
COMMIT_AFTER='after'                   # commit: list of hashes after the commit
COMMIT_MESSAGE='message'               # commit: message with the commit
COMMIT_METHOD='method'                 # commit: list of hashes for programs that produced the commit
METADATA_MTIME='metadata_mtime'
DIRNAME='dirname'
FILENAME='filename'
NOTE='note'
NOTES='notes'                   # an array of NOTE objects
COMMIT_AUTHOR='author'                 # commit: author of commit
COMMIT_DATASET='datasets'              # commit: name of the dataset
HOSTNAME='hostname'
PATH='path'
HEXHASH='hexhash'
HEXHASH_ALG='sha1'              # which algorithm we are using
SEARCH_ANY='*'
SEARCH='search'                 # takes a single dict
REMOTE_ADDR='remote_addr'       # remote address for a commit
RESULT='result'                 # a single dict
RESULTS='results'               # a list of dicts
CREATED='created'
TIME='time'                     # commit - when method was finished
DURATION='duration'             # commit - how long the commit took (so we don't have a clock skew issue)


# Commit API
API_OBJECTS='objects'
API_COMMIT='commit'


# Dump
DUMP='dump'
LIMIT='limit'
OFFSET='offset'
