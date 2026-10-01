from __future__ import annotations

from configparser import ConfigParser
from functools import cached_property
from typing import Any

from traitlets import Bool, Int, Unicode
from traitlets.config.configurable import LoggingConfigurable

from .cs3vfs.session import CS3Session


class CS3Mixin(LoggingConfigurable):
    """The CS3 configuration, and the session built from it.

    Both contents managers and the server extension's client inherit these
    traits, so one config section (c.CS3Mixin.*) configures them all. The
    services each needs are built on the session by the subclasses.
    """

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
        super().__init__(**kwargs)
        _ = self.session  # connect now, not on the first request
        self.log.debug(f"CS3 session initialized with path: {self.root_path}")

    # A property rather than built in __init__: upstream validates config
    # (preferred_dir -> dir_exists) while traitlets loads it, before __init__
    # gets past super().__init__().
    @cached_property
    def session(self) -> CS3Session:
        return CS3Session(self._create_cs3_config(), self.token_path, self.client_id, self.log)

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
