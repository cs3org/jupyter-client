"""
CS3 storage: file system operations over the CS3 APIs.

Replaces what the contents managers would otherwise do with os and shutil.

Authors: Rasmus Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

from __future__ import annotations

import json
import os
import stat
import time
from contextlib import contextmanager
from typing import Generator, List, Optional, Tuple

import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr
from cs3client.exceptions import NotFoundException

from .cs3file import CS3File
from .session import CS3Service, CS3Session
from .statuscodehandler import translate_cs3_error
from .utils import resource_from_path, retry_on_auth_failure, StatResult

# How long a Stat result stays usable. Long enough that the several lookups
# behind one request collapse into one RPC, short enough that a change made
# elsewhere (sync client, web office) shows up in the next file browser
# refresh, which is on a ~10s timer.
STAT_TTL_SECONDS = 2.0

# How long to keep expecting the mount to catch up with a CS3-side change
# before assuming something else keeps the two apart.
MOUNT_LAG_TIMEOUT_SECONDS = 300.0

_MISS = object()


class StatCache:
    """Recent Stat results by path; a cached None means "does not exist"."""

    def __init__(self, ttl: float = STAT_TTL_SECONDS):
        self.ttl = ttl
        self._entries: dict[str, tuple[object, float]] = {}

    def get(self, path: str):
        """The cached result, or _MISS if there is none still fresh."""
        hit = self._entries.get(path)
        if hit is not None and time.monotonic() - hit[1] < self.ttl:
            return hit[0]
        return _MISS

    def put(self, path: str, info) -> None:
        self._entries[path] = (info, time.monotonic())

    def invalidate(self, *paths: str) -> None:
        for path in paths:
            self._entries.pop(path, None)


class PendingChanges:
    """Paths changed through CS3 that the FUSE mount may not show yet.

    The mount cannot see a reva-side change until its own cache expires; the
    hybrid manager asks which paths in a directory are still pending to
    decide when a listing must come from CS3 instead.
    """

    def __init__(self, timeout: float = MOUNT_LAG_TIMEOUT_SECONDS):
        self.timeout = timeout
        self._since: dict[str, float] = {}

    def note(self, *paths: str) -> None:
        now = time.monotonic()
        self._expire(now)
        for path in paths:
            self._since[path] = now

    def in_dir(self, os_dir: str) -> List[str]:
        """Pending paths directly in os_dir; ones pending too long are given up on."""
        self._expire(time.monotonic())
        return [p for p in self._since if os.path.dirname(p) == os_dir]

    def settle(self, path: str) -> None:
        """The mount has caught up with path."""
        self._since.pop(path, None)

    def _expire(self, now: float) -> None:
        cutoff = now - self.timeout
        self._since = {p: t for p, t in self._since.items() if t > cutoff}


class CS3Storage(CS3Service):
    """
    CS3-based file system operations that can replace standard library functions.

    Writes, removals and renames carry our lock (holder and lock id), which is
    what lets reva accept them on a file we hold.
    """

    def __init__(self, session: CS3Session, lock_holder: str, lock_value: str):
        super().__init__(session)
        self.lock_holder = lock_holder
        self.lock_value = lock_value
        self.stat_cache = StatCache()
        self.pending = PendingChanges()

    def _stat_cached(self, path: str):
        """Stat `path`, reusing a recent result. None means "does not exist".

        Metadata reads dominate the gRPC traffic: building the model for one
        directory entry asks "does it exist?" and then stats it - the same call
        twice - and the file browser rebuilds every entry on each refresh.

        Only the not-found answer is cached alongside the positive one; every
        other failure stays uncached so it remains retryable (an expired token
        must still be refreshed and the call repeated by retry_on_auth_failure).
        """
        info = self.stat_cache.get(path)
        if info is not _MISS:
            return info
        try:
            info = self.client.file.stat(self.auth.get_token(), resource_from_path(path))
        except NotFoundException:
            info = None
        self.stat_cache.put(path, info)
        return info

    def invalidate_stat(self, *paths: str) -> None:
        """Drop cached Stat results, e.g. after a mutation performed on the mount."""
        self.stat_cache.invalidate(*paths)

    def note_cs3_change(self, *paths: str) -> None:
        """Record a CS3-side mutation: drop the cached stats and mark pending.

        Every CS3 mutation in this class calls this; mount-side mutations call
        invalidate_stat, since there is nothing for the mount to catch up on.
        """
        self.stat_cache.invalidate(*paths)
        self.pending.note(*paths)

    @contextmanager
    def open(self, path: str, mode: str = 'r', encoding: Optional[str] = None, **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for opening CS3 files."""
        cs3_file = CS3File(self, path, mode, encoding)
        cs3_file._init()
        try:
            yield cs3_file
        finally:
            cs3_file.close()

    @retry_on_auth_failure
    def exists(self, path: str) -> bool:
        """Check if path exists."""
        try:
            ## same Stat as is_file/is_dir/lstat/access - served once per path
            return self._stat_cached(path) is not None
        except Exception:
            return False

    @retry_on_auth_failure
    def is_file(self, path: str) -> bool:
        """Check if path is a file."""
        try:
            result = self._stat_cached(path)
            if result is None:
                return False

            is_file = hasattr(result, 'type') and result.type == cs3spr.ResourceType.RESOURCE_TYPE_FILE
            return is_file
        except Exception:
            return False

    @retry_on_auth_failure
    def is_dir(self, path: str) -> bool:
        """Check if path is a directory."""
        try:
            result = self._stat_cached(path)
            if result is None:
                return False
            return result.type == cs3spr.ResourceType.RESOURCE_TYPE_CONTAINER
        except Exception:
            return False

    @retry_on_auth_failure
    def list_dir(self, path: str) -> List[Tuple[str, 'StatResult']]:
        """List directory contents with stat information in one call."""
        resource = resource_from_path(path)
        try:
            result = self.client.file.list_dir(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            translate_cs3_error(e)
        items = []
        for item in result:
            stat_result = StatResult(item)
            # item.path should be the full path
            # but jupyter expects just the name
            # so we extract the name from the path
            name = item.path.split("/")[-1]
            items.append((name, stat_result))

        return items

    @retry_on_auth_failure
    def mkdir(self, path: str) -> None:
        """Create directory."""
        try:
            resource = resource_from_path(path)
            self.client.file.make_dir(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            self.note_cs3_change(path)

    def rmdir(self, path: str) -> None:
        """Remove directory (one call for the whole subtree)."""
        return self.unlink(path, with_lock=False)

    @retry_on_auth_failure
    def unlink(self, path: str, with_lock: bool = True) -> None:
        """Remove file."""
        try:
            resource = resource_from_path(path)
            self.client.file.remove_file(
                self.auth.get_token(),
                resource,
                ## A container cannot hold an EOS lock, so passing one asks reva
                ## to validate a lock that can never be there. For files it is
                ## load-bearing: it is what lets us remove a file we hold.
                lock_id=self.lock_value if with_lock else None,
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            self.note_cs3_change(path)

    @retry_on_auth_failure
    def touch(self, path: str) -> None:
        """Create an empty file."""
        try:
            resource = resource_from_path(path)
            self.client.file.touch_file(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            self.note_cs3_change(path)

    @retry_on_auth_failure
    def rename(self, src: str, dst: str) -> None:
        """Rename file or directory."""
        try:
            src_resource = resource_from_path(src)
            dst_resource = resource_from_path(dst)
            self.client.file.rename_file(
                self.auth.get_token(),
                src_resource,
                dst_resource,
                lock_id=self.lock_value
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            self.note_cs3_change(src, dst)

    @retry_on_auth_failure
    def lstat(self, path: str) -> 'StatResult':
        """Get file stats."""
        try:
            result = self._stat_cached(path)
            if result is None:
                raise NotFoundException("not found")
        except Exception as e:
            translate_cs3_error(e)

        return StatResult(result)

    @retry_on_auth_failure
    def access(self, path: str, mode: int) -> bool:
        """Check file access permissions."""
        try:
            if self._stat_cached(path) is None:
                ## kept raising rather than returning False: callers rely on a
                ## missing path surfacing as FileNotFoundError, not "no access"
                raise NotFoundException("not found")
            return True
        except Exception as e:
            translate_cs3_error(e)

    @retry_on_auth_failure
    def read_bytes(self, path: str) -> bytes:
        """Read a whole file."""
        try:
            chunks = self.client.file.read_file(self.auth.get_token(), resource_from_path(path))
        except Exception as e:
            translate_cs3_error(e)
        content = b""
        for chunk in chunks:
            if isinstance(chunk, Exception):
                raise chunk
            content += chunk
        return content

    @retry_on_auth_failure
    def write_bytes(self, path: str, content: bytes) -> None:
        """Write a whole file, under our lock (a foreign lock rejects it)."""
        try:
            self.client.file.write_file(
                self.auth.get_token(),
                resource_from_path(path),
                content,
                len(content),
                self.lock_holder,
                self.lock_value
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            ## size and mtime just changed, and save() stats right after
            self.note_cs3_change(path)

    @retry_on_auth_failure
    def get_dir_size(self, path: str) -> int:
        """Calculate total size of directory and subdirectories using CS3 stat."""
        try:
            resource = resource_from_path(path)
            try:
                result = self.client.file.stat(
                    self.auth.get_token(),
                    resource
                )
            except Exception as e:
                translate_cs3_error(e)

            # Get stat info which includes tree_size in opaque metadata
            stat_result = StatResult(result)

            # If it's a file, return its size
            if stat_result.st_mode & stat.S_IFREG:
                return stat_result.st_size

            # For directories, try to get tree_size from opaque metadata
            if result and hasattr(result, 'opaque') and result.opaque:
                # Look for EOS metadata with tree_size
                for key, value in result.opaque.map.items():
                    if key == "eos" and value.decoder == "json":
                        try:
                            eos_data = json.loads(value.value.decode('utf-8'))
                            if 'tree_size' in eos_data:
                                return int(eos_data['tree_size'])
                        except (json.JSONDecodeError, KeyError, ValueError):
                            pass

            # Fallback to directory size (not including subdirectories)
            return stat_result.st_size

        except Exception as e:
            self.log.warning(f"Error calculating directory size for {path}: {e}")
            return 0

    @retry_on_auth_failure
    def copyfile(self, src: str, dst: str) -> None:
        """Copy a file in the storage, streamed rather than loaded into memory.

        The write carries no lock: a copy destination is a new file, and if
        something else holds it the storage refuses the write.
        """
        try:
            # Get the source file size first
            info = self._stat_cached(src)
            if info is None:
                raise NotFoundException("not found")
            content_generator = self.client.file.read_file(
                self.auth.get_token(),
                resource_from_path(src)
            )
            self.client.file.write_file(
                self.auth.get_token(),
                resource_from_path(dst),
                content_generator,  # Pass generator directly
                info.size
            )
        except Exception as e:
            translate_cs3_error(e)
        finally:
            self.note_cs3_change(dst)

    def copy_tree(self, src: str, dst: str) -> None:
        """Copy directory tree."""
        self.mkdir(dst)
        for dir_name, _ in self.list_dir(src):
            src_path = os.path.join(src, dir_name)
            dst_path = os.path.join(dst, dir_name)

            if self.is_dir(src_path):
                self.copy_tree(src_path, dst_path)
            else:
                self.copyfile(src_path, dst_path)
