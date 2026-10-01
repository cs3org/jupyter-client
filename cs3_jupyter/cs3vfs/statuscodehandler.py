"""
CS3 status code handler and error mapping.

Authors: Rasmus Oscar Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

import errno

from cs3client.exceptions import AuthenticationException, PermissionDeniedException, NotFoundException, AlreadyExistsException, FileLockedException, UnimplementedException, SecretNotSetException


# Subclass of OSError so existing `except OSError` handlers keep working,
# while lock conflicts stay distinguishable from other storage errors.
# No errno is set, so perm_to_403 will not swallow it.
class FileLockedError(OSError):
    def __init__(self, *args, holder=None):
        super().__init__(*args)
        # Who holds the lock, when the raiser knows (a rejected write does not).
        self.holder = holder


# The token is the problem (expired, or not read yet), not the permissions:
# the only failure worth re-reading the token file and retrying for.
class CS3AuthError(PermissionError):
    pass


def translate_cs3_error(e: Exception) -> None:
    """Re-raise a cs3client exception as the matching builtin OSError."""
    if isinstance(e, FileLockedException):
        raise FileLockedError("Resource is locked")
    if isinstance(e, AlreadyExistsException):
        raise FileExistsError("File already exists")
    if isinstance(e, UnimplementedException):
        raise NotImplementedError("Operation not implemented")
    if isinstance(e, NotFoundException):
        raise FileNotFoundError("No such file or directory")
    if isinstance(e, (AuthenticationException, SecretNotSetException)):
        raise CS3AuthError("Authentication failed")
    if isinstance(e, PermissionDeniedException):
        # EACCES lets perm_to_403 turn it into a 403.
        raise PermissionError(errno.EACCES, "Permission denied")
    if isinstance(e, ValueError):
        raise ValueError("Invalid input")
    # Keep the original text: reva reports several conditions (notably a
    # missing file during a lock operation) as a generic internal error,
    # and the message is the only thing that says which.
    raise OSError(f"Unknown error occurred: {e}") from e


# Most specific first: CS3AuthError is a PermissionError, FileLockedError an
# OSError. The cs3client types cover calls that reach the client untranslated
# (e.g. the token decode behind user_idp).
_HTTP_STATUS = (
    ((FileLockedError, FileLockedException), 423),
    ((FileExistsError, AlreadyExistsException), 409),
    ((NotImplementedError, UnimplementedException), 501),
    ((FileNotFoundError, NotFoundException), 404),
    ((CS3AuthError, AuthenticationException, SecretNotSetException), 401),
    ((PermissionError, PermissionDeniedException), 403),
    ((ValueError,), 400),
)


def http_status_for(e: Exception) -> int:
    """HTTP status for an exception raised by a CS3 call."""
    for types, status in _HTTP_STATUS:
        if isinstance(e, types):
            return status
    return 500
