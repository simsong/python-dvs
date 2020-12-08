"""
The DVS exception class
"""


class DVSException(Exception):
    """Base class for DVS Exceptions"""
    pass

class DVSCommitError(DVSException):
    pass

class DVSGitException(DVSException):
    pass

class DVSServerError(DVSException):
    pass

class DVSServerTimeout(DVSServerError):
    pass
