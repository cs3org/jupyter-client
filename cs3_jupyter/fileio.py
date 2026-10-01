"""
Utilities for file-based Contents/Checkpoints managers.
"""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from base64 import decodebytes, encodebytes
import errno
import os
from contextlib import contextmanager, nullcontext
from datetime import datetime
from functools import cached_property
from typing import Any, Generator, TYPE_CHECKING
import nbformat

from anyio.to_thread import run_sync
from jupyter_server import _tz as tz
from jupyter_server.utils import ApiPath, to_os_path
from tornado.web import HTTPError
from traitlets import default

from .cs3mixin import CS3Mixin
from .cs3vfs.cs3lock import LOCK_NO_FILE, CS3Locks
from .cs3vfs.cs3versions import CS3Versions
from .cs3vfs.cs3vfs import CS3Storage
from .cs3vfs.statuscodehandler import FileLockedError
from .cs3vfs.utils import StatResult
from .documentlocks import DocumentLocks
from .filecheckpoints import CS3FileCheckpoints
from .paths import is_hidden


if TYPE_CHECKING:
    from .cs3vfs.cs3file import CS3File


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
def locked_to_423(path: str):
    """Context manager turning a lock conflict into a 423.

    Upstream save() turns anything that is not an HTTPError into a 500.
    """
    try:
        yield
    except FileLockedError as e:
        # A rejected write does not say who holds the lock; ensure_write_lock does.
        raise HTTPError(423, str(e) if e.holder else f"{path} is locked") from e


@contextmanager
def rename_errors_to_http(old_path: str):
    """Context manager mapping what a rename can run into to HTTP errors."""
    try:
        yield
    except HTTPError:
        raise
    except FileLockedError as e:
        raise HTTPError(423, f"{old_path} is locked") from e
    except FileNotFoundError:
        raise HTTPError(404, f"File or directory does not exist: {old_path}") from None
    except Exception as e:
        raise HTTPError(500, f"Unknown error renaming file: {old_path} {e}") from e


class CS3ContentsMixin(CS3Mixin):
    """What both contents managers share.

    Every write goes through CS3 under our lock, and the model of a file
    someone else has locked is read-only. Like upstream's FileManagerMixin the
    helpers are sync: the hybrid manager calls them directly, the async pure
    manager through run_sync.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        # Built here from this one thread (unless config validation needed them
        # earlier), so run_sync workers never race to build two of each.
        _ = self.document_locks, self.versions

    @cached_property
    def storage(self) -> CS3Storage:
        return CS3Storage(self.session, self.lock_holder, self.lock_value)

    @cached_property
    def locks(self) -> CS3Locks:
        return CS3Locks(self.session, self.storage, self.lock_holder, self.lock_value)

    @cached_property
    def versions(self) -> CS3Versions:
        return CS3Versions(self.session)

    # Locks of documents open in the frontend, driven by the /lock endpoint.
    @cached_property
    def document_locks(self) -> DocumentLocks:
        return DocumentLocks(self.locks, self.lock_expiration, self.log)

    # Reva versions every write, so checkpoints are its file versions.
    @default("checkpoints_class")
    def _checkpoints_class_default(self):
        return CS3FileCheckpoints

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
            with self.locks.write_lock(os_path) if writing else nullcontext():
                with self.storage.open(os_path, *args, **kwargs) as f:
                    yield f

    # Upstream's atomic_writing writes a temporary file on local disk.
    @contextmanager
    def atomic_writing(self, path: str, *, text: bool = True, encoding: str = "utf-8", **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for writing to CS3."""
        mode = "w" if text else "wb"
        with self.open(path, mode, encoding=encoding) as f:
            yield f

    def _write_locked(self, os_path, bcontent):
        """Write a whole file through CS3, under our lock."""
        with self.perm_to_403(os_path), locked_to_423(os_path), self.locks.write_lock(os_path) as state:
            if not bcontent and state == LOCK_NO_FILE:
                # New empty file: zero-byte uploads are unreliable, so touch it
                # instead - via CS3, not FUSE, so reva sees it immediately and
                # it can be locked right away.
                self.storage.touch(os_path)
            else:
                self.storage.write_bytes(os_path, bcontent)

    def _append_bytes(self, os_path, bcontent):
        """Append a chunk of an upload through CS3, like its first chunk."""
        with self.open(os_path, "ab") as f:
            f.write(bcontent)

    def _raise_if_foreign_locked(self, os_path, path):
        """423 if someone else holds the lock on a file.

        Storages that don't enforce locks on delete or rename would let those
        through, so they check first.
        """
        holder = self.locks.foreign_lock_holder(os_path)
        if holder:
            raise HTTPError(423, f"{path} is locked by {holder}")

    # Lock ownership is surfaced in the models so foreign-locked files render
    # read-only. Lock state comes from CS3 GetLock (exact holder match) rather
    # than FUSE xattrs, which lag behind and contain the EOS-encoded holder
    # name. Only checked when content is requested (opening the document), so
    # directory listings - which build each entry with content=False - don't
    # pay one GetLock per file.
    def _mark_read_only_if_foreign_locked(self, model, path):
        if model["writable"] and self.locks.foreign_lock_holder(self._get_os_path(path)):
            model["writable"] = False
        return model

    def _delete(self, os_path, path, is_dir):
        """Delete through CS3; reva's recycle bin replaces send2trash."""
        if is_dir:
            self.log.debug("Removing directory %s", os_path)
            with self.perm_to_403():
                self.storage.rmdir(os_path)
            return
        ## Pre-check so storages that don't enforce locks on delete still refuse.
        self._raise_if_foreign_locked(os_path, path)
        self.log.debug("Unlinking file %s", os_path)
        with self.perm_to_403(), locked_to_423(path):
            ## replaced os.unlink with CS3 remove (carries our lock id)
            self.storage.unlink(os_path)

    # Upstream's, with the stat and the writable flag taken from the hooks below.
    def _base_model(self, path):
        """Build the common base of a contents model"""
        os_path = self._get_os_path(path)
        info = self._model_stat(os_path)

        four_o_four = "file or directory does not exist: %r" % path

        ## replaced is_hidden with the one in paths.py
        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            self.log.info("Refusing to serve hidden file or directory %r, via 404 Error", os_path)
            raise HTTPError(404, four_o_four)

        try:
            # size of file
            size = info.st_size
        except (ValueError, OSError):
            self.log.warning("Unable to get size.")
            size = None

        try:
            last_modified = tz.utcfromtimestamp(info.st_mtime)
        except (ValueError, OSError):
            # Files can rarely have an invalid timestamp
            # https://github.com/jupyter/notebook/issues/2539
            # https://github.com/jupyter/notebook/issues/2757
            # Use the Unix epoch as a fallback so we don't crash.
            self.log.warning("Invalid mtime %s for %s", info.st_mtime, os_path)
            last_modified = datetime(1970, 1, 1, 0, 0, tzinfo=tz.UTC)

        try:
            created = tz.utcfromtimestamp(info.st_ctime)
        except (ValueError, OSError):  # See above
            self.log.warning("Invalid ctime %s for %s", info.st_ctime, os_path)
            created = datetime(1970, 1, 1, 0, 0, tzinfo=tz.UTC)

        # Create the base model.
        model = {}
        model["name"] = path.rsplit("/", 1)[-1]
        model["path"] = path
        model["last_modified"] = last_modified
        model["created"] = created
        model["content"] = None
        model["format"] = None
        model["mimetype"] = None
        model["size"] = size
        model["writable"] = self._model_writable(path, info)
        model["hash"] = None
        model["hash_algorithm"] = None

        return model

    def _model_stat(self, os_path):
        ## replaced os.lstat
        return self.storage.lstat(os_path)

    def _model_writable(self, path, info):
        return self.is_writable(path)


class CS3HybridFileManagerMixin(CS3ContentsMixin):
    """
    Mixin routing all content mutations through CS3 (so reva-side locks are
    meaningful) while keeping the local FUSE mount as a fast path for metadata.
    """

    # FUSE is only a fast path: a file just created via CS3 may not have
    # appeared in the mount yet, so on a local miss fall back to a CS3 stat.
    # This is what prevents the create-then-stat race for new files.
    def hybrid_lstat(self, os_path):
        try:
            return os.lstat(os_path)
        except FileNotFoundError:
            return self.storage.lstat(os_path)

    def hybrid_exists(self, os_path):
        return os.path.exists(os_path) or self.storage.exists(os_path)

    ## `or self.is_*` would ask CS3 whenever the mount answered "no" - including
    ## when the mount has the entry and it simply is not that type. Only a path
    ## the mount has never heard of needs the round-trip.
    def hybrid_isfile(self, os_path):
        if os.path.exists(os_path):
            return os.path.isfile(os_path)
        return self.storage.is_file(os_path)

    def hybrid_isdir(self, os_path):
        if os.path.exists(os_path):
            return os.path.isdir(os_path)
        return self.storage.is_dir(os_path)

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
        stale = False
        for path in self.storage.pending.in_dir(os_dir):
            if os.path.exists(path) == self.storage.exists(path):
                self.storage.pending.settle(path)  # the mount has caught up
            else:
                stale = True
        return stale

    def cs3_entry_names(self, os_dir: str):
        """Names in a directory according to CS3, or None if it cannot say."""
        try:
            return [name for name, _ in self.storage.list_dir(os_dir)]
        except OSError:
            return None

    ## FUSE-first stat with a CS3 fallback; a CS3 stat carries the writable
    ## flag, the mount needs an access check.
    def _model_stat(self, os_path):
        return self.hybrid_lstat(os_path)

    def _model_writable(self, path, info):
        if isinstance(info, StatResult):
            return info.writeable
        return self.is_writable(path)

    def _save_file(self, os_path, content, format):
        """Save content of a generic file."""
        self._write_locked(os_path, decode_model_content(content, format, os_path))

    def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        # New notebooks are ordinary CS3 writes: the write creates the file in
        # reva, and the hybrid_* stat fallbacks cover the FUSE propagation lag.
        self._write_locked(os_path, notebook_bytes(nb, capture_validation_error))

    # Upstream sync FileManagerMixin._read_file, with the isfile gate changed
    # so fresh CS3-created files pass before FUSE catches up, and the bytes
    # read via CS3.
    def _read_file(self, os_path, format, raw=False):
        """Read a non-notebook file."""
        ## replaced os.path.isfile
        if not self.hybrid_isfile(os_path):
            raise HTTPError(400, "Cannot read non-file %s" % os_path)
        with self.perm_to_403(os_path):
            bcontent = self.storage.read_bytes(os_path)
        return encode_model_content(bcontent, format, os_path, raw)

    # Upstream sync LargeFileManager._save_large_file with the builtin open
    # replaced by self.open, so chunked appends go through CS3 instead of
    # mixing CS3 (chunk 1) with FUSE (chunks 2+).
    def _save_large_file(self, os_path, content, format):
        """Save content of a generic file."""
        self._append_bytes(os_path, decode_model_content(content, format, os_path))


class CS3FileManagerMixin(CS3ContentsMixin):
    """
    Mixin for ContentsAPI classes that interact with the filesystem asynchronously.
    """

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

        root = self.root_dir  # type:ignore[attr-defined]
        if os.path.splitdrive(path)[0]:
            raise HTTPError(404, "%s is not a relative API path" % path)
        os_path = to_os_path(ApiPath(path), root)
        ## upstream lstats here only for the ValueError of an invalid path
        ## (an embedded null byte); against CS3 that was a Stat per call.
        if "\0" in os_path:
            raise HTTPError(404, f"{path} is not a valid path")

        if not (os_path + os.path.sep).startswith(root):
            raise HTTPError(404, "%s is outside root contents directory" % path)
        return os_path

    # Completely replaced with CS3 functionality (used to call shutil copy)
    async def _copy(self, src, dest):
        """copy src to dest using cs3 filesystem while checking permissions"""
        if not self.storage.access(src, os.R_OK):
            self.log.debug("Source file, %s, is not readable", src, exc_info=True)
            raise PermissionError(errno.EACCES, f"File is not readable: {src}")

        dest_parent = os.path.dirname(dest) or dest
        if not self.storage.access(dest_parent, os.W_OK):
            self.log.debug("Destination directory, %s, is not writable", dest_parent, exc_info=True)
            raise PermissionError(errno.EACCES, f"Destination is not writable: {dest}")

        await run_sync(self.storage.copyfile, src, dest)

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
        if not self.storage.is_file(os_path):
            raise HTTPError(404, f"File not found: {os_path}")
        with self.perm_to_403(os_path):
            bcontent = await run_sync(self.storage.read_bytes, os_path)
        return encode_model_content(bcontent, format, os_path, raw)
