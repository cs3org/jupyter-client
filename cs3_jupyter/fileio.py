"""
Utilities for file-based Contents/Checkpoints managers.
"""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from base64 import b64decode, decodebytes, encodebytes
import errno
import os
from contextlib import contextmanager
from typing import Generator, TYPE_CHECKING
import nbformat



from tornado.web import HTTPError
from traitlets.config.configurable import LoggingConfigurable
from anyio.to_thread import run_sync

from jupyter_server.utils import ApiPath, to_api_path, to_os_path
from .cs3mixin import CS3Mixin
from .cs3vfs.cs3lock import LOCK_NO_FILE


if TYPE_CHECKING:
    from .cs3vfs.cs3vfs import CS3File


class CS3HybridFileManagerMixin(CS3Mixin, LoggingConfigurable):
    """
    Mixin routing all content mutations through CS3 (so reva-side locks are
    meaningful) while keeping the local FUSE mount as a fast path for metadata.
    """

    @contextmanager
    def open(self, os_path, *args, **kwargs):
        """wrapper around io.open that turns permission errors into 403"""
        # Only writes need the lock; reads of a foreign-locked file are fine
        # (the model is marked read-only instead).
        mode = args[0] if args else kwargs.get("mode", "r")
        if any(c in mode for c in "wax"):
            self.ensure_write_lock(os_path)
        with self.perm_to_403(os_path), self.cs3open(os_path, *args, **kwargs) as f:
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

    def hybrid_isfile(self, os_path):
        return os.path.isfile(os_path) or self.is_file(os_path)

    def hybrid_isdir(self, os_path):
        return os.path.isdir(os_path) or self.is_dir(os_path)

    def _save_file(self, os_path, content, format):
        """Save content of a generic file."""
        state = self.ensure_write_lock(os_path)
        if (content == "" or content is None) and state == LOCK_NO_FILE:
            # New empty file: create it via CS3 (not FUSE) so reva sees it
            # immediately and it can be locked right away.
            self.vfs_touch(os_path)
            self.lock_after_create(os_path)
            return
        if format not in {"text", "base64"}:
            raise HTTPError(
                400,
                "Must specify format of file contents as 'text' or 'base64'",
            )
        try:
            if format == "text":
                bcontent = content.encode("utf8")
            else:
                b64_bytes = content.encode("ascii")
                bcontent = decodebytes(b64_bytes)
        except Exception as e:
            raise HTTPError(400, f"Encoding error saving {os_path}: {e}") from e

        with self.atomic_writing(os_path, text=False) as f:
            f.write(bcontent)
        if state == LOCK_NO_FILE:
            self.lock_after_create(os_path)

    def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        # New notebooks are ordinary CS3 writes: the write creates the file in
        # reva, and the hybrid_* stat fallbacks cover the FUSE propagation lag.
        state = self.ensure_write_lock(os_path)
        with self.atomic_writing(os_path, encoding="utf-8") as f:
            nbformat.write(
                nb,
                f,
                version=nbformat.NO_CONVERT,
                capture_validation_error=capture_validation_error,
            )
        if state == LOCK_NO_FILE:
            self.lock_after_create(os_path)

    # Copied from upstream sync FileManagerMixin._read_file; only the isfile
    # gate is changed so fresh CS3-created files pass before FUSE catches up
    # (the body already reads via self.open -> CS3).
    def _read_file(self, os_path, format, raw=False):
        """Read a non-notebook file."""
        ## replaced os.path.isfile
        if not self.hybrid_isfile(os_path):
            raise HTTPError(400, "Cannot read non-file %s" % os_path)

        with self.open(os_path, "rb") as f:
            bcontent = f.read()

        if format == "byte":
            # Not for http response but internal use
            return (bcontent, "byte", bcontent) if raw else (bcontent, "byte")

        if format is None or format == "text":
            # Try to interpret as unicode if format is unknown or if unicode
            # was explicitly requested.
            try:
                return (
                    (bcontent.decode("utf8"), "text", bcontent)
                    if raw
                    else (bcontent.decode("utf8"), "text")
                )
            except UnicodeError as e:
                if format == "text":
                    raise HTTPError(
                        400,
                        "%s is not UTF-8 encoded" % os_path,
                        reason="bad format",
                    ) from e
        return (
            (encodebytes(bcontent).decode("ascii"), "base64", bcontent)
            if raw
            else (encodebytes(bcontent).decode("ascii"), "base64")
        )

    # Copied from upstream sync LargeFileManager._save_large_file with the
    # builtin open replaced by self.open, so chunked appends go through CS3
    # instead of mixing CS3 (chunk 1) with FUSE (chunks 2+).
    def _save_large_file(self, os_path, content, format):
        """Save content of a generic file."""
        if format not in {"text", "base64"}:
            raise HTTPError(
                400,
                "Must specify format of file contents as 'text' or 'base64'",
            )
        try:
            if format == "text":
                bcontent = content.encode("utf8")
            else:
                b64_bytes = content.encode("ascii")
                bcontent = b64decode(b64_bytes)
        except Exception as e:
            raise HTTPError(400, f"Encoding error saving {os_path}: {e}") from e

        with self.perm_to_403(os_path):
            ## replaced builtin open with self.open (CS3)
            with self.open(os_path, "ab") as f:
                f.write(bcontent)



class CS3FileManagerMixin(CS3Mixin, LoggingConfigurable):
    """
    Mixin for ContentsAPI classes that interact with the filesystem asynchronously.
    """

    @contextmanager
    def open(self, os_path, *args, **kwargs):
        """wrapper around io.open that turns permission errors into 403"""
        # Only writes need the lock; reads of a foreign-locked file are fine
        # (the model is marked read-only instead).
        mode = args[0] if args else kwargs.get("mode", "r")
        if any(c in mode for c in "wax"):
            self.ensure_write_lock(os_path)
        with self.perm_to_403(os_path), self.cs3open(os_path, *args, **kwargs) as f:
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

    # We use this instead of atomic writing, let reva handle it
    @contextmanager
    def writing(self, path: str, *, text: bool = True, encoding: str = "utf-8", **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for writing to CS3."""
        mode = "w" if text else "wb"
        with self.open(path, mode, encoding=encoding) as f:
            yield f

    async def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        nb_text = nbformat.writes(
            nb,
            version=nbformat.NO_CONVERT,
            capture_validation_error=capture_validation_error,
        )
        state = await run_sync(self.ensure_write_lock, os_path)
        await run_sync(self.vfs_save_file, os_path, nb_text, "text")
        if state == LOCK_NO_FILE:
            await run_sync(self.lock_after_create, os_path)

    async def _save_file(self, os_path, content, format):
        """Save content of a generic file."""
        if format not in {"text", "base64"}:
            raise HTTPError(
                400,
                "Must specify format of file contents as 'text' or 'base64'",
            )
        state = await run_sync(self.ensure_write_lock, os_path)
        if (content == "" or content is None) and state == LOCK_NO_FILE:
            # Zero-byte uploads are unreliable; create new empty files with touch.
            await run_sync(self.vfs_touch, os_path)
        else:
            await run_sync(self.vfs_save_file, os_path, content, format)
        if state == LOCK_NO_FILE:
            await run_sync(self.lock_after_create, os_path)

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

        return self.read_file(os_path, format, raw)
