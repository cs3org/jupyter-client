"""
CS3 Virtual File System utilities.

Authors: Rasmus Oscar Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

import stat
import time
from cs3client.cs3resource import Resource
import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr

import inspect
from functools import wraps
from typing import  Callable, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")

def retry_on_auth_failure(func: Callable[P, R]) -> Callable[P, R]:
    """Retry a CS3 call once after refreshing the token/secret.

    Expects `self` to implement `_refresh_auth()`.
    Supports both sync and async callables.
    """

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapped(self, *args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
            try:
                return await func(self, *args, **kwargs)
            except PermissionError as e:
                self.log.error(
                    f"cs3mixin: {func.__name__.upper()} AUTH ERROR - {e}, reading token and retrying..."
                )
                self._refresh_auth()
                return await func(self, *args, **kwargs)

        return cast(Callable[P, R], async_wrapped)

    @wraps(func)
    def wrapped(self, *args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
        try:
            return func(self, *args, **kwargs)
        except PermissionError as e:
            self.log.error(
                f"cs3mixin: {func.__name__.upper()} AUTH ERROR - {e}, reading token and retrying..."
            )
            self._refresh_auth()
            return func(self, *args, **kwargs)

    return cast(Callable[P, R], wrapped)

def resource_from_path(path: str) -> Resource:
        """Convert path to CS3 Resource object."""
        return Resource(abs_path=path)

class StatResult:
    def __init__(self, info) -> None:
        """Initialize StatResult from CS3 resource info."""
        # size is needed for jupyter
        self.st_size = getattr(info, 'size', 0)

        if hasattr(info, 'mtime') and info.mtime:
            self.st_mtime = float(info.mtime.seconds)
            if hasattr(info.mtime, 'nanos'):
                self.st_mtime += info.mtime.nanos / 1e9
        else:
            self.st_mtime = time.time()

        # mtime and ctime are needed for jupyter
        self.st_ctime = int(self.st_mtime)
        self.st_mtime = int(self.st_mtime)

        # type is needed for jupyter
        if hasattr(info, 'type'):
            if info.type == cs3spr.ResourceType.RESOURCE_TYPE_CONTAINER:
                self.st_mode = stat.S_IFDIR | 0o755
            elif info.type == cs3spr.ResourceType.RESOURCE_TYPE_FILE:
                self.st_mode = stat.S_IFREG | 0o644
            elif info.type == cs3spr.ResourceType.RESOURCE_TYPE_SYMLINK:
                self.st_mode = stat.S_IFLNK | 0o777
            else:
                self.st_mode = stat.S_IFREG | 0o644
        else:
            self.st_mode = stat.S_IFREG | 0o644
        # All resources do not have the permissions_set attribute, but
        # if a resource doesn't have this attribute it can't be writeable.
        if hasattr(info, 'permission_set'):
            if info.permission_set.create_container or info.permission_set.delete:
                self.writeable = True
            else:
                self.writeable = False
        else:
            self.writeable = False
