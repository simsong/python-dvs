"""
The DVS exception class
"""


class DVSException(Exception):
    """Base class for DVS Exceptions"""
    pass

class DVSClientError(DVSException):
    """Exceptions thrown in the client"""
    pass

class DVSCommitError(DVSClientError):
    pass

class DVSGitException(DVSClientError):
    """Exceptions generated by running the git command line client"""
    pass

class DVSTooManyObjects(DVSClientError):
    """Too many objects in COMMIT_BEFORE, COMMIT_METHOD or COMMIT_AFTER and OPTION_AUTO_SUB_COMMIT not set"""
    pass

class DVSServerError(DVSException):
    """Exceptions thrown in the server"""
    pass

class DVSServerTimeout(DVSServerError):
    pass
