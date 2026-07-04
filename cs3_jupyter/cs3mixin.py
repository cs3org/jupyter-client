from __future__ import annotations

import asyncio
import jwt
import os
from configparser import ConfigParser
from typing import Any

from anyio.to_thread import run_sync
from cs3client.auth import Auth
from cs3client.cs3client import CS3Client
from traitlets import Bool, Int, Unicode
from traitlets.config.configurable import LoggingConfigurable


from .cs3vfs.statuscodehandler import StatusCodeHandler, FileLockedError
from .cs3vfs.cs3versions import CS3FileVersions
from .cs3vfs.cs3vfs import CS3VirtualFileSystem
from .cs3vfs.cs3sharing import CS3Sharing
from .cs3vfs.cs3spaces import CS3Spaces
from .cs3vfs.cs3groups import CS3Groups
from .cs3vfs.cs3users import CS3Users
from .cs3vfs.cs3lock import CS3Lock
from .sessiontracker import SessionTracker

class CS3Mixin(CS3VirtualFileSystem, CS3Groups, CS3Users, CS3Spaces, CS3Sharing, CS3FileVersions, CS3Lock, LoggingConfigurable):
    """Owns the shared CS3Client/Auth and persistent service instances."""

    host = Unicode(config=True, help="CS3 host address")
    tus_enabled = Bool(default_value=False, config=True, help="Enable TUS protocol")
    ssl_enabled = Bool(default_value=False, config=True, help="Enable SSL connection")
    token_path = Unicode(
        default_value="/tmp/cernbox_oauth.token",
        config=True,
        help="Path to OAuth token file",
    )
    lock_expiration = Int(
        default_value=300,
        config=True,
        help="Lock expiration time in seconds"
    )
    lock_app_name = Unicode(
        default_value="jupyter-rtc",
        config=True,
        help="String to use for application-level locking (EOS lowercases app names, keep it lowercase)"
    )
    lock_holder_suffix_client_id = Bool(
        default_value=True,
        config=True,
        help="Append '-<client_id>' to lock_app_name to make the lock holder per-user. "
             "Set to False on all servers to share one holder, letting them collaborate on locked files."
    )
    lock_value = Unicode(
        default_value="jupyter_rtc_lock",
        config=True,
        help="Value to use for application-level locking"
    )
    root_path = Unicode(default_value="", config=True, help="CS3 root path for the user")
    auth_login_type = Unicode(
        default_value="bearer", config=True, help="Authentication login type"
    )
    authtokenvalidity = Int(
        default_value=3600, config=True, help="Authentication token validity in seconds"
    )
    lock_not_impl = Bool(default_value=False, config=True, help="Lock not implemented flag")
    lock_by_setting_attr = Bool(default_value=False, config=True, help="Fall back to advisory xattr locks when the storage does not implement locking")
    client_id = Unicode(default_value="", config=True, help="CS3 client ID (can be set in config)")


    def __init__(self, **kwargs: Any):
        self.status_handler = StatusCodeHandler()
        self.cs3_token = ""
        self._lock_refresher = None
        super().__init__(**kwargs)
        self._read_token_file()
        self._config = self._create_cs3_config()
        self.client = CS3Client(self._config, "cs3client", self.log)
        self.auth = Auth(self.client)
        self.auth.set_client_id(self.client_id)
        self.auth.set_client_secret(self.cs3_token)
        self.session_tracker = SessionTracker(heartbeat_timeout_seconds=self.lock_expiration)
        self.log.debug(f"CS3ClientMixinBase initialized with path: {self.root_path}")

    @property
    def lock_holder(self) -> str:
        """The lock holder (CS3 lock app_name) identifying this server's locks.

        Evaluated lazily so it reflects traitlets config. Reva's EOS driver
        matches lock holders by app_name alone, so per-user holders serialize
        users while a shared holder lets all servers co-own locks.
        """
        if self.lock_holder_suffix_client_id and self.client_id:
            return f"{self.lock_app_name}-{self.client_id}"
        return self.lock_app_name

    def get_user_path(self) -> str:
        return self.root_path

    def _read_token_file(self) -> None:
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, "r") as f:
                    self.cs3_token = f.read().strip()
            else:
                self.log.warning(f"Token file not found: {self.token_path}")
        except Exception as e:
            self.log.error(f"Failed to read token file {self.token_path}: {e}")

    def _refresh_auth(self) -> None:
        """Reload token/secret from disk and update the shared Auth object."""
        self._read_token_file()
        self.auth.set_client_secret(self.cs3_token)

    def _create_cs3_config(self) -> ConfigParser:
        cs3config = ConfigParser()
        cs3config.add_section("cs3client")
        cs3config.set("cs3client", "host", self.host)
        cs3config.set("cs3client", "tus_enabled", str(self.tus_enabled).lower())
        cs3config.set("cs3client", "ssl_enabled", str(self.ssl_enabled).lower())
        cs3config.set("cs3client", "token_path", self.token_path)
        cs3config.set("cs3client", "auth_client_id", self.client_id)
        cs3config.set("cs3client", "auth_login_type", self.auth_login_type)
        cs3config.set("cs3client", "authtokenvalidity", str(self.authtokenvalidity))
        cs3config.set("cs3client", "lock_not_impl", str(self.lock_not_impl).lower())
        cs3config.set("cs3client", "lock_by_setting_attr", str(self.lock_by_setting_attr).lower())
        cs3config.set("cs3client", "lock_expiration", str(self.lock_expiration))
        return cs3config

    def ensure_lock_refresher(self) -> None:
        """Start the background lock refresher if it is not already running."""
        if self._lock_refresher is None or self._lock_refresher.done():
            self._lock_refresher = asyncio.ensure_future(self._lock_refresh_loop())

    async def _lock_refresh_loop(self) -> None:
        """Keep locks of open documents alive and release locks whose sessions all went stale.

        Runs while the session tracker has entries; exits when idle (restarted
        lazily by the next POST /lock).
        """
        interval = max(self.lock_expiration // 3, 10)
        while True:
            await asyncio.sleep(interval)
            async with self._lock_mutex:
                active, expired = self.session_tracker.sweep()
                for path in expired:
                    try:
                        await run_sync(self.unlock, path, self.lock_holder, self.lock_value)
                    except (FileLockedError, FileNotFoundError):
                        # Not ours anymore or already gone - nothing to release.
                        pass
                for path in active:
                    try:
                        await run_sync(self.refresh_lock, path, self.lock_holder, self.lock_value, self.lock_value)
                    except (FileLockedError, FileNotFoundError) as e:
                        self.log.warning(f"Lock refresh lost on {path}: {e}")
            if not active and not expired:
                return

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
