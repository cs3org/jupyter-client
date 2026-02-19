"""
Utilities for file-based Contents/Checkpoints managers.
"""
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import annotations

from base64 import decodebytes
import errno
import os
from contextlib import contextmanager
from typing import Generator
import nbformat

from tornado.web import HTTPError
from traitlets.config.configurable import LoggingConfigurable
from anyio.to_thread import run_sync

from jupyter_server.utils import ApiPath, to_api_path, to_os_path
from .cs3mixin import CS3Mixin

class CS3FileManagerMixin(CS3Mixin, LoggingConfigurable):
    """
    Mixin for ContentsAPI classes that interact with the filesystem asynchronously.
    """

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
        if not self.access(src, os.W_OK):
            if self.log:
                self.log.debug("Source file, %s, is not writable", src, exc_info=True)
            raise PermissionError(errno.EACCES, f"File is not writable: {src}")

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
    def writing(self, path: str, text: bool = True, encoding: str = "utf-8", **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for writing to CS3."""
        mode = "w" if text else "wb"
        with self.open(path, mode, encoding) as f:
            yield f

    # Replaced atomic writing since we let reva handle this
    async def _save_notebook(self, os_path, nb, capture_validation_error=None):
        """Save a notebook to an os_path."""
        with self.writing(os_path, encoding="utf-8") as f:
            f.write(nbformat.writes(
                nb,
                version=nbformat.NO_CONVERT,
                capture_validation_error=capture_validation_error
            ))

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

    # replaced atomic writing since we let reva handle this
    async def _save_file(self, os_path, content, format):
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
                bcontent = decodebytes(b64_bytes)
        except Exception as e:
            raise HTTPError(400, f"Encoding error saving {os_path}: {e}") from e

        with self.writing(os_path, text=False) as f:
            await run_sync(f.write, bcontent)
