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


class CS3Session:
    """The cs3client and its Auth, shared by every service built on them.

    Re-reads the token file when a call fails authentication (the file is
    rewritten when the token is renewed), and tells whose token it is.
    """

    def __init__(self, config: ConfigParser, token_path: str, client_id: str, log: logging.Logger):
        self.log = log
        self.token_path = token_path
        self.client = CS3Client(config, "cs3client", log)
        self.auth = Auth(self.client)
        self.auth.set_client_id(client_id)
        self.refresh_auth()

    def refresh_auth(self) -> None:
        """Reload the token from disk into the Auth."""
        self.auth.set_client_secret(self._read_token())

    def _read_token(self) -> str:
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, "r") as f:
                    return f.read().strip()
            self.log.warning(f"Token file not found: {self.token_path}")
        except Exception as e:
            self.log.error(f"Failed to read token file {self.token_path}: {e}")
        return ""

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
