"""Error translation, HTTP status mapping and the auth retry."""

import logging

import pytest
from cs3client.exceptions import (
    AuthenticationException,
    FileLockedException,
    NotFoundException,
    PermissionDeniedException,
)

from cs3_jupyter.cs3api.errors import (
    CS3AuthError,
    FileLockedError,
    http_status_for,
    translate_cs3_error,
)
from cs3_jupyter.cs3api.utils import retry_on_auth_failure


def translated(e):
    with pytest.raises(Exception) as exc:
        translate_cs3_error(e)
    return exc.value


@pytest.mark.parametrize(
    "cs3_error, status",
    [
        (NotFoundException("x"), 404),
        (PermissionDeniedException("x"), 403),
        (AuthenticationException("x"), 401),
        (FileLockedException("x"), 423),
        (RuntimeError("x"), 500),
    ],
)
def test_translated_errors_keep_their_status(cs3_error, status):
    """Handlers only ever see translated errors: they must not all be 500."""
    assert http_status_for(translated(cs3_error)) == status


def test_lock_conflict_stays_distinguishable():
    assert isinstance(translated(FileLockedException("x")), FileLockedError)


class Caller:
    log = logging.getLogger("test")

    def __init__(self, error):
        self.error = error
        self.calls = 0
        self.refreshes = 0

    def _refresh_auth(self):
        self.refreshes += 1

    @retry_on_auth_failure
    def rpc(self):
        self.calls += 1
        translate_cs3_error(self.error)


def test_auth_failure_refreshes_token_and_retries_once():
    caller = Caller(AuthenticationException("expired"))
    with pytest.raises(CS3AuthError):
        caller.rpc()
    assert (caller.calls, caller.refreshes) == (2, 1)


def test_permission_denied_is_not_retried():
    """A fresh token would be denied the same way: one RPC, no token re-read."""
    caller = Caller(PermissionDeniedException("no"))
    with pytest.raises(PermissionError):
        caller.rpc()
    assert (caller.calls, caller.refreshes) == (1, 0)
