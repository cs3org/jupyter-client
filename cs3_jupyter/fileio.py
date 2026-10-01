"""
Utilities for file-based Contents/Checkpoints managers.
"""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from base64 import decodebytes, encodebytes
import errno
import os
import time
from contextlib import contextmanager, nullcontext
from typing import Generator, TYPE_CHECKING
import nbformat

from tornado.web import HTTPError
from anyio.to_thread import run_sync

from jupyter_server.utils import ApiPath, to_api_path, to_os_path
from .cs3mixin import CS3Mixin
from .cs3vfs.cs3lock import LOCK_NO_FILE
from .cs3vfs.cs3vfs import MOUNT_LAG_TIMEOUT_SECONDS
from .cs3vfs.statuscodehandler import FileLockedError


if TYPE_CHECKING:
    from .cs3vfs.cs3vfs import CS3File


def decode_model_content(content: str | None, format: str | None, os_path: str) -> bytes:
    """The bytes a file model's content stands for; 400 if it cannot be decoded."""
    if format not in {"text", "base64"}:
        raise HTTPError(
            400,
            "Must specify format of file contents as 'text' or 'base64'",
        )
    if not content:
        return b""
    try:
        if format == "text":
            return content.encode("utf8")
        return decodebytes(content.encode("ascii"))
    except Exception as e:
        raise HTTPError(400, f"Encoding error saving {os_path}: {e}") from e


def encode_model_content(bcontent: bytes, format: str | None, os_path: str, raw: bool = False):
    """File bytes as upstream's _read_file returns them: (content, format[, bcontent]).

    format 'text' decodes UTF-8 (400 if it is not), 'base64' encodes, 'byte'
    passes the bytes through, and None tries text before falling back to base64.
    """
    if format == "byte":
        # Not for http response but internal use
        return (bcontent, "byte", bcontent) if raw else (bcontent, "byte")

    if format is None or format == "text":
        # Try to interpret as unicode if format is unknown or if unicode
        # was explicitly requested.
        try:
            text = bcontent.decode("utf8")
            return (text, "text", bcontent) if raw else (text, "text")
        except UnicodeError as e:
            if format == "text":
                raise HTTPError(
                    400,
                    "%s is not UTF-8 encoded" % os_path,
                    reason="bad format",
                ) from e
    b64 = encodebytes(bcontent).decode("ascii")
    return (b64, "base64", bcontent) if raw else (b64, "base64")


def notebook_bytes(nb, capture_validation_error=None) -> bytes:
    """A notebook serialized the way nbformat.write puts it on disk."""
    text = nbformat.writes(
        nb,
        version=nbformat.NO_CONVERT,
        capture_validation_error=capture_validation_error,
    )
    if not text.endswith("\n"):
        text += "\n"
    return text.encode("utf8")


@contextmanager
def locked_to_423(os_path: str):
    """Context manager turning a lock conflict into a 423.

    Upstream save() turns anything that is not an HTTPError into a 500.
    """
    try:
        yield
    except FileLockedError as e:
        # A rejected write does not say who holds the lock; ensure_write_lock does.
        raise HTTPError(423, str(e) if e.holder else f"{os_path} is locked") from e


class CS3HybridFileManagerMixin(CS3Mixin):
    """
    Mixin routing all content mutations through CS3 (so reva-side locks are
    meaningful) while keeping the local FUSE mount as a fast path for metadata.
    """

    @contextmanager
    def open(self, os_path, *args, **kwargs):
        """wrapper around io.open that turns permission errors into 403"""
        # Only writes need the lock; reads of a foreign-locked file are fine
        # (the model is marked read-only instead). Our own savers write bytes
        # directly; this path serves everything else - chunked appends and
        # any upstream caller - so it must lock.
        mode = args[0] if args else kwargs.get("mode", "r")
        writing = any(c in mode for c in "wax")
        with self.perm_to_403(os_path), locked_to_423(os_path):
            with self.write_lock(os_path) if writing else nullcontext():
                with self.cs3open(os_path, *args, **kwargs) as f:
                    yield f

    # We have to overwrite atomic_writing to use our own open method.
    @contextmanager
    def atomic_writing(self, path: str, *, text: bool = True, encoding: str = "utf-8", **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for writing to CS3."""
        mode = "w" if text else "wb"
        with self.open(path, mode, encoding=encoding) as f:
            yield f

    # FUSE is only a fast path: a file just created via CS3 may not have
    # appeared in the mount yet, so on a local miss fall back to a CS3 stat.
    # This is what prevents the create-then-stat race for new files.
    def hybrid_lstat(self, os_path):
        try:
            return os.lstat(os_path)
        except FileNotFoundError:
            return self.lstat(os_path)

    def hybrid_exists(self, os_path):
        return os.path.exists(os_path) or self.vfs_exists(os_path)

    ## `or self.is_*` would ask CS3 whenever the mount answered "no" - including
    ## when the mount has the entry and it simply is not that type. Only a path
    ## the mount has never heard of needs the round-trip.
    def hybrid_isfile(self, os_path):
        if os.path.exists(os_path):
            return os.path.isfile(os_path)
        return self.is_file(os_path)

    def hybrid_isdir(self, os_path):
        if os.path.exists(os_path):
            return os.path.isdir(os_path)
        return self.is_dir(os_path)

    def mount_lags_in(self, os_dir: str) -> bool:
        """Does the mount still disagree about something we changed via CS3?

        eosxd cannot see a reva-side create, rename or delete until its own
        cache expires, so a listing taken from os.listdir keeps showing the old
        state - a renamed notebook kept its old name until the cache turned
        over. Comparing only the paths we touched (rather than whole listings)
        keeps this from tripping over entries the two sources filter
        differently, such as EOS version folders.

        Costs nothing when nothing is pending, which is the steady state.
        """
        pending = [p for p in self._pending_paths if os.path.dirname(p) == os_dir]
        if not pending:
            return False

        now = time.monotonic()
        stale = False
        for path in pending:
            if now - self._pending_paths[path] > MOUNT_LAG_TIMEOUT_SECONDS:
                del self._pending_paths[path]  # give up; stop paying for it
            elif os.path.exists(path) == self.vfs_exists(path):
                del self._pending_paths[path]  # the mount has caught up
            else:
                stale = True
        return stale

    def cs3_entry_names(self, os_dir: str):
        """Names in a directory according to CS3, or None if it cannot say."""
        try:
            return [name for name, _ in self.list_dir(os_dir)]
        except OSError:
            return None

    def _save_file(self, os_path, content, format):
        """Save content of a generic file."""
        bcontent = decode_model_content(content, format, os_path)
        with self.perm_to_403(os_path), locked_to_423(os_path), self.write_lock(os_path) as state:
            if not bcontent and state == LOCK_NO_FILE:
                # New empty file: create it via CS3 (not FUSE) so reva sees it
                # immediately and it can be locked right away.
                self.vfs_touch(os_path)
            else:
                self.write_bytes(os_path, bcontent)

    def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        # New notebooks are ordinary CS3 writes: the write creates the file in
        # reva, and the hybrid_* stat fallbacks cover the FUSE propagation lag.
        bcontent = notebook_bytes(nb, capture_validation_error)
        with self.perm_to_403(os_path), locked_to_423(os_path), self.write_lock(os_path):
            self.write_bytes(os_path, bcontent)

    # Upstream sync FileManagerMixin._read_file, with the isfile gate changed
    # so fresh CS3-created files pass before FUSE catches up, and the bytes
    # read via CS3.
    def _read_file(self, os_path, format, raw=False):
        """Read a non-notebook file."""
        ## replaced os.path.isfile
        if not self.hybrid_isfile(os_path):
            raise HTTPError(400, "Cannot read non-file %s" % os_path)
        with self.perm_to_403(os_path):
            bcontent = self.read_bytes(os_path)
        return encode_model_content(bcontent, format, os_path, raw)

    # Upstream sync LargeFileManager._save_large_file with the builtin open
    # replaced by self.open, so chunked appends go through CS3 instead of
    # mixing CS3 (chunk 1) with FUSE (chunks 2+).
    def _save_large_file(self, os_path, content, format):
        """Save content of a generic file."""
        bcontent = decode_model_content(content, format, os_path)
        ## replaced builtin open with self.open (CS3)
        with self.open(os_path, "ab") as f:
            f.write(bcontent)


class CS3FileManagerMixin(CS3Mixin):
    """
    Mixin for ContentsAPI classes that interact with the filesystem asynchronously.
    """

    @contextmanager
    def open(self, os_path, *args, **kwargs):
        """wrapper around io.open that turns permission errors into 403"""
        # Only writes need the lock; reads of a foreign-locked file are fine
        # (the model is marked read-only instead).
        mode = args[0] if args else kwargs.get("mode", "r")
        writing = any(c in mode for c in "wax")
        with self.perm_to_403(os_path), locked_to_423(os_path):
            with self.write_lock(os_path) if writing else nullcontext():
                with self.cs3open(os_path, *args, **kwargs) as f:
                    yield f


    # Moved from "FileManagerMixin (we only use the Async version)"
    @contextmanager
    def perm_to_403(self, os_path=""):
        """context manager for turning permission errors into 403."""
        try:
            yield
        except OSError as e:
            if e.errno in {errno.EPERM, errno.EACCES}:
                # make 403 error message without root prefix
                # this may not work perfectly on unicode paths on Python 2,
                # but nobody should be doing that anyway.
                if not os_path:
                    os_path = e.filename or "unknown file"
                path = to_api_path(os_path, root=self.root_dir)  # type:ignore[attr-defined]
                raise HTTPError(403, "Permission denied: %s" % path) from e
            else:
                raise

    # Os functionality replaced with CS3 functionality
    def _get_os_path(self, path):
        """Given an API path, return its file system path.

        Parameters
        ----------
        path : str
            The relative API path to the named file.

        Returns
        -------
        path : str
            Native, absolute OS path to for a file.

        Raises
        ------
        404: if path is outside root
        """

        root = self.abs_path(self.root_dir)  # type:ignore[attr-defined]
        if os.path.splitdrive(path)[0]:
            raise HTTPError(404, "%s is not a relative API path" % path)
        os_path = to_os_path(ApiPath(path), root)
        try:
            self.lstat(os_path)
        except OSError:
            # OSError could be FileNotFound, PermissionError, etc.
            # those should raise (or not) elsewhere
            pass
        except ValueError:
            raise HTTPError(404, f"{path} is not a valid path") from None

        if not (self.abs_path(os_path) + os.path.sep).startswith(root):
            raise HTTPError(404, "%s is outside root contents directory" % path)
        return os_path

    # Completely replaced with CS3 functionality (used to call shutil copy)
    async def _copy(self, src, dest):
        """copy src to dest using cs3 filesystem while checking permissions"""
        if not self.access(src, os.R_OK):
            self.log.debug("Source file, %s, is not readable", src, exc_info=True)
            raise PermissionError(errno.EACCES, f"File is not readable: {src}")

        dest_parent = os.path.dirname(dest) or dest
        if not self.access(dest_parent, os.W_OK):
            self.log.debug("Destination directory, %s, is not writable", dest_parent, exc_info=True)
            raise PermissionError(errno.EACCES, f"Destination is not writable: {dest}")

        await self.copyfile(src, dest)

    # Replaced with CS3 functionality
    async def _read_notebook(
        self, os_path, as_version=4, capture_validation_error=None, raw: bool = False
    ):
        """Read a notebook from an os path."""
        answer = await self._read_file(os_path, "text", raw=raw)

        nb = nbformat.reads(
            answer[0],
            as_version=as_version,
            capture_validation_error=capture_validation_error
        )
        return (nb, answer[2]) if raw else nb

    async def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        bcontent = notebook_bytes(nb, capture_validation_error)
        await run_sync(self._write_locked, os_path, bcontent)

    async def _save_file(self, os_path, content, format):
        """Save content of a generic file."""
        bcontent = decode_model_content(content, format, os_path)
        await run_sync(self._write_locked, os_path, bcontent)

    def _write_locked(self, os_path, bcontent):
        with self.perm_to_403(os_path), locked_to_423(os_path), self.write_lock(os_path) as state:
            if not bcontent and state == LOCK_NO_FILE:
                # Zero-byte uploads are unreliable; create new empty files with touch.
                self.vfs_touch(os_path)
            else:
                self.write_bytes(os_path, bcontent)

    # replaced with CS3 functionality
    async def _read_file(  # type: ignore[override]
        self, os_path: str, format: str | None, raw: bool = False
    ) -> tuple[str | bytes, str] | tuple[str | bytes, str, bytes]:
        """Read a non-notebook file.

        Parameters
        ----------
        os_path: str
            The path to be read.
        format: str
            If 'text', the contents will be decoded as UTF-8.
            If 'base64', the raw bytes contents will be encoded as base64.
            If 'byte', the raw bytes contents will be returned.
            If not specified, try to decode as UTF-8, and fall back to base64
        raw: bool
            [Optional] If True, will return as third argument the raw bytes content

        Returns
        -------
        (content, format, byte_content) It returns the content in the given format
        as well as the raw byte content.
        """
        if not self.is_file(os_path):
            raise HTTPError(404, f"File not found: {os_path}")
        with self.perm_to_403(os_path):
            bcontent = await run_sync(self.read_bytes, os_path)
        return encode_model_content(bcontent, format, os_path, raw)
