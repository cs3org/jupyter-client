"""
One authenticated connection to CS3, and the base of the services using it.
"""

from __future__ import annotations

import logging
import os
from configparser import ConfigParser

import jwt
from cs3client.auth import Auth
from cs3client.cs3client import CS3Client


class FileAuth(Auth):
    """Auth that reloads client secret on demand.

    This is useful when:
    - The client secret file does not yet exist when the extension loads.
      In this case, it will reload the secret on the next get_token() call.
    - When the client secret file changes

    It tracks the mtime to determine if the client secret is newer than what
    is already cached.
    """

    def __init__(self, cs3_client: CS3Client, token_path: str) -> None:
        super().__init__(cs3_client)
        self._token_path = token_path
        self._secret_mtime: int | None = None

    def _load_secret(self) -> None:
        try:
            mtime = os.stat(self._token_path).st_mtime_ns
        except OSError:
            return  # token file does not exist (yet)
        if mtime == self._secret_mtime:
            return  # token file did not change, skip
        with open(self._token_path) as f:
            secret = f.read().strip()
        if not secret:
            return  # token file read mid-write, re-read next time
        self.set_client_secret(secret)
        self._secret_mtime = mtime

    def refresh(self) -> None:
        self._load_secret()
        self._token = None

    def get_token(self) -> tuple[str, str]:
        self._load_secret()
        return super().get_token()


class CS3Session:
    """The cs3client and its Auth, shared by every service built on them.

    The Auth re-reads the token file when it changes (it is rewritten when
    the token is renewed), and drops its token when a call fails
    authentication. The session also tells whose token it is.
    """

    def __init__(self, config: ConfigParser, token_path: str, client_id: str, log: logging.Logger):
        self.log = log
        self.client = CS3Client(config, "cs3client", log)
        self.auth = FileAuth(self.client, token_path)
        self.auth.set_client_id(client_id)

    def refresh_auth(self) -> None:
        """Reload the secret from disk and drop the cached token."""
        self.auth.refresh()

    def _decode_token(self) -> dict:
        _, token = self.auth.get_token()
        return jwt.decode(
            jwt=token,
            algorithms=["HS256"],
            options={"verify_signature": False},
        )

    @property
    def user_idp(self) -> str:
        return self._decode_token()["user"]["id"]["idp"]

    @property
    def user_opaque_id(self) -> str:
        return self._decode_token()["user"]["id"]["opaque_id"]


class CS3Service:
    """Base of the CS3 services: one area of the API over a shared session.

    retry_on_auth_failure expects the `log` and `_refresh_auth` it finds here.
    """

    def __init__(self, session: CS3Session):
        self.session = session
        self.client = session.client
        self.auth = session.auth
        self.log = session.log

    def _refresh_auth(self) -> None:
        self.session.refresh_auth()
