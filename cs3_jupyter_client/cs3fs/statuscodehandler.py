"""
statuscodehandler.py

Authors: Rasmus Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

from cs3client.exceptions import AuthenticationException, PermissionDeniedException, NotFoundException, AlreadyExistsException, FileLockedException, UnimplementedException


class StatusCodeHandler:
    def handle_errors(self, e: Exception) -> None:

        if isinstance(e, FileLockedException):
            raise OSError("Resource temporarily unavailable")
        if isinstance(e, AlreadyExistsException):
            raise FileExistsError("File already exists")
        if isinstance(e, UnimplementedException):
            raise NotImplementedError("Operation not implemented")
        if isinstance(e, NotFoundException):
            raise FileNotFoundError("No such file or directory")
        if isinstance(e, AuthenticationException):
            raise PermissionError("Authentication failed")
        if isinstance(e, PermissionDeniedException):
            raise PermissionError("Permission denied")
        raise OSError("Unknown error occurred")
