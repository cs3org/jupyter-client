from __future__ import annotations


import os
from traitlets import Bool, Int, Unicode
from traitlets.config.configurable import LoggingConfigurable
from .cs3fs.cs3fs import create_cs3_filesystem
from configparser import ConfigParser
from functools import wraps

class CS3Mixin(LoggingConfigurable):
    """
    Base mixin providing CS3 filesystem access for all file operation classes.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._read_token_file()
        self._cs3_fs = None
        # Initialize CS3 filesystem
        self._user_path = f'{self.root_path}'
        self._config = self._create_cs3_config()
        self.log.debug(f"CS3Mixin initialized with path: {self._user_path}")

    host = Unicode(
        config=True,
        help="CS3 host address"
    )

    tus_enabled = Bool(
        default_value=False,
        config=True,
        help="Enable TUS protocol"
    )

    ssl_enabled = Bool(
        default_value=False,
        config=True,
        help="Enable SSL connection"
    )

    token_path = Unicode(
        default_value="/tmp/cernbox_oauth.token",
        config=True,
        help="Path to OAuth token file"
    )

    root_path = Unicode(
        default_value="",
        config=True,
        help="CS3 root path for the user"
    )

    auth_login_type = Unicode(
        default_value="bearer",
        config=True,
        help="Authentication login type"
    )

    authtokenvalidity = Int(
        default_value=3600,
        config=True,
        help="Authentication token validity in seconds"
    )

    lock_not_impl = Bool(
        default_value=False,
        config=True,
        help="Lock not implemented flag"
    )

    lock_as_attr = Bool(
        default_value=False,
        config=True,
        help="Lock as attribute flag"
    )

    cs3_token = Unicode(
        default_value="",
        config=True,
        help="CS3 authentication token"
    )

    def get_user_path(self):
        """Get the user path for CS3 operations."""
        return self._user_path

    def _read_token_file(self):
        """Read token from file and set cs3_token."""
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'r') as f:
                    self.cs3_token = f.read().strip()
            else:
                self.log.warning(f"Token file not found: {self.token_path}")
        except Exception as e:
            self.log.error(f"Failed to read token file {self.token_path}: {e}")

    def _create_cs3_config(self):
        """Create CS3 config object from trait values."""

        cs3config = ConfigParser()
        cs3config.add_section('cs3client')
        cs3config.set('cs3client', 'host', self.host)
        cs3config.set('cs3client', 'tus_enabled', str(self.tus_enabled).lower())
        cs3config.set('cs3client', 'ssl_enabled', str(self.ssl_enabled).lower())
        cs3config.set('cs3client', 'token_path', self.token_path)
        cs3config.set('cs3client', 'auth_login_type', self.auth_login_type)
        cs3config.set('cs3client', 'authtokenvalidity', str(self.authtokenvalidity))
        cs3config.set('cs3client', 'lock_not_impl', str(self.lock_not_impl).lower())
        cs3config.set('cs3client', 'lock_as_attr', str(self.lock_as_attr).lower())

        return cs3config

    def _get_cs3_fs_indep(self):
        """
        Get CS3 filesystem instance independent of the mixin,
        creates a new client each time, this was tested to be more
        performant than reusing the same client.
        """
        return create_cs3_filesystem(
            self._config,
            self.cs3_token,
            self.root_path
        )

    @property
    def cs3_fs(self):
        """CS3 filesystem instance, we create a new client on each access because this is more
        performant than trying to reuse the same client."""
        return self._get_cs3_fs_indep()

    def retry_on_auth_failure(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except PermissionError as e:
                self.log.error(f"cs3mixin: {func.__name__.upper()} AUTH ERROR - {e}, reading token and retrying...")
                try:
                    self._read_token_file()
                    return func(self, *args, **kwargs)
                except Exception as e:
                    self.log.error(f"cs3mixin: {func.__name__.upper()} ERROR - {e}")
                    raise e
        return wrapper

    @retry_on_auth_failure
    def access(self, path, mode):
        return self.cs3_fs.access(path, mode)

    @retry_on_auth_failure
    def lstat(self, path):
        return self.cs3_fs.lstat(path)

    @retry_on_auth_failure
    def is_dir(self, path):
        return self.cs3_fs.is_dir(path)

    @retry_on_auth_failure
    def list_dir(self, path):
        return self.cs3_fs.list_dir(path)

    @retry_on_auth_failure
    def mkdir(self, path):
        return self.cs3_fs.mkdir(path)

    @retry_on_auth_failure
    def rmdir(self, path):
        return self.cs3_fs.unlink(path)

    @retry_on_auth_failure
    def unlink(self, path):
        return self.cs3_fs.unlink(path)

    @retry_on_auth_failure
    def move(self, src, dest):
        return self.cs3_fs.rename(src, dest)

    @retry_on_auth_failure
    def is_file(self, path):
        return self.cs3_fs.is_file(path)

    @retry_on_auth_failure
    async def copy_tree(self, src, dest):
        return await self.cs3_fs.copy_tree(src, dest)

    @retry_on_auth_failure
    def get_dir_size(self, path):
        return self.cs3_fs._get_dir_size(path)

    @retry_on_auth_failure
    def abs_path(self, path):
        return self.cs3_fs.abs_path(path)

    @retry_on_auth_failure
    async def copyfile(self, src, dest):
        return await self.cs3_fs.copyfile(src, dest)

    @retry_on_auth_failure
    def open(self, path, mode, encoding = None, **kwargs):
        return self.cs3_fs.open(path, mode, encoding, **kwargs)

    @retry_on_auth_failure
    def read_file(self, path: str, format = None, raw = False):
        res = self.cs3_fs._read_file(path, format, raw)
        return res

    @retry_on_auth_failure
    def list_file_versions(self, path):
        return self.cs3_fs.list_file_versions(path)

    @retry_on_auth_failure
    def restore_file_version(self, path, version_key):
        return self.cs3_fs.restore_file_version(path, version_key)