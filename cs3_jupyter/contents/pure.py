"""The pure CS3 contents manager: every read, write and stat goes through CS3."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import errno
import os
import stat

import nbformat
from anyio.to_thread import run_sync
from jupyter_server import _tz as tz
from jupyter_server.services.contents.manager import copy_pat
from jupyter_server.utils import ApiPath, to_os_path
from tornado import web
from tornado.web import HTTPError
from traitlets import default, validate

from .common import CS3ContentsMixin, decode_model_content, encode_model_content, notebook_bytes
from .paths import is_file_hidden, is_hidden
from .upstream import UpstreamLargeFileManager


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


'''
These are the modifications to the upstream Jupyter FileContentsManager that need significant changes
to work with CS3 filesystem or contain unecessary functionality for our use case - here it is not as simple
as replacing os calls with self.<method> calls.
'''
class CS3FileContentsManager(CS3FileManagerMixin):
    """An async file contents manager."""

    # Upstream uses os.getcwd
    # Different implementation
    @default("root_dir")
    def _default_root_dir(self):
        return self.root_path

    # We cannot validate before starting if it's a virtual filesystem.
    @validate("root_dir")
    def _validate_root_dir(self, proposal):
        return proposal.value

    # We cannot validate before starting if it's a virtual filesystem.
    @validate("preferred_dir")
    def _validate_preferred_dir(self, proposal):
        return proposal.value

    # Upstream uses os.access; caching is left to the TTL stat cache under
    # storage.access, which - unlike a per-path dict - expires and is invalidated
    # by mutations.
    def is_writable(self, path):
        """Does the API style path correspond to a writable directory or file?"""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        try:
            return self.storage.access(os_path, os.W_OK)
        except OSError:
            self.log.error("Failed to check write permissions on %s", os_path)
            return False

    # See CS3ContentsMixin._mark_read_only_if_foreign_locked.
    async def _notebook_model(self, path, content=True, require_hash=False):
        model = await super()._notebook_model(path, content=content, require_hash=require_hash)
        if content:
            await run_sync(self._mark_read_only_if_foreign_locked, model, path)
        return model

    async def _file_model(self, path, content=True, format=None, require_hash=False):
        model = await super()._file_model(path, content=content, format=format, require_hash=require_hash)
        if content:
            await run_sync(self._mark_read_only_if_foreign_locked, model, path)
        return model

    # Upstream is significantly more complex due to os functionality
    # handling trashbin, and the windows/mac specifics.
    async def delete_file(self, path):
        """Delete file at path."""
        path = path.strip("/")
        os_path = self._get_os_path(path)

        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, f"Cannot delete file or directory {os_path!r}")
        if not await self.exists(path):
            raise web.HTTPError(404, "File or directory does not exist: %s" % os_path)
        await run_sync(self._delete, os_path, path, self.storage.is_dir(os_path))

    async def _dir_model(self, path, content=True):
        """Build a model for a directory

        if content is requested, will include a listing of the directory
        """
        os_path = self._get_os_path(path)

        four_o_four = "directory does not exist: %r" % path
        ## Replaced os.path.isdir
        if not self.storage.is_dir(os_path):
            raise web.HTTPError(404, four_o_four)
        ## replaced is_hidden with implementation above
        elif not self.allow_hidden and is_hidden(os_path, self.root_dir):
            self.log.info("Refusing to serve hidden directory %r, via 404 Error", os_path)
            raise web.HTTPError(404, four_o_four)

        model = self._base_model(path)
        model["type"] = "directory"
        model["size"] = None
        if content:
            model["content"] = contents = []
            os_dir = self._get_os_path(path)
            ## replaced os.listdir
            dir_contents = await run_sync(self.storage.list_dir, os_dir)

            for dir_name, stat_info in dir_contents:
                try:
                    os_path = os.path.join(os_dir, dir_name)
                except UnicodeDecodeError as e:
                    # skip over broken symlinks in listing
                    if e.errno == errno.ENOENT:
                        self.log.warning("%s doesn't exist", os_path)
                    elif e.errno != errno.EACCES:  # Don't provide clues about protected files
                        self.log.warning("Error stat-ing %s: %r", os_path, e)
                    continue

                if (
                    not stat.S_ISLNK(stat_info.st_mode)
                    and not stat.S_ISREG(stat_info.st_mode)
                    and not stat.S_ISDIR(stat_info.st_mode)
                ):
                    self.log.debug("%s not a regular file", os_path)
                    continue

                try:
                    if self.should_list(dir_name) and (
                        ## replaced is_file_hidden with implementation above class
                        self.allow_hidden or not is_file_hidden(os_path, stat_res=stat_info)
                    ):
                        resource_model = {
                            "name": dir_name,
                            "path": f"{path}/{dir_name}",
                            "last_modified": tz.utcfromtimestamp(stat_info.st_mtime),
                            "created": tz.utcfromtimestamp(stat_info.st_ctime),
                            "size": stat_info.st_size,
                            "writable": stat_info.writeable,
                        }

                        if stat.S_ISDIR(stat_info.st_mode):
                            resource_model["type"] = "directory"
                        contents.append(resource_model)
                except OSError as e:
                    # ELOOP: recursive symlink, also don't show failure due to permissions
                    if e.errno not in [errno.ELOOP, errno.EACCES]:
                        self.log.warning(
                            "Unknown error checking if file %r is hidden",
                            os_path,
                            exc_info=True,
                        )

            model["format"] = "json"

        return model

    # Upstream uses AsyncContentsManager.copy which makes it is impossible to
    # to replace the os function in the super class (AsyncContentsManager - manager.py)
    # this needs to be fixed in upstream...
    # FIXME: This function is largely copied from upstream (with the AsyncContentsManager's
    # copy function inside).
    async def copy(self, from_path, to_path=None):
        """
        Copy an existing file or directory and return its new model.
        If to_path not specified, it will be the parent directory of from_path.
        If copying a file and to_path is a directory, filename/directoryname will increment `from_path-Copy#.ext`.
        Considering multi-part extensions, the Copy# part will be placed before the first dot for all the extensions except `ipynb`.
        For easier manual searching in case of notebooks, the Copy# part will be placed before the last dot.
        from_path must be a full path to a file or directory.
        """
        to_path_original = str(to_path)
        path = from_path.strip("/")
        if to_path is not None:
            to_path = to_path.strip("/")

        if "/" in path:
            from_dir, from_name = path.rsplit("/", 1)
        else:
            from_dir = ""
            from_name = path

        ## content=False: this model is only consulted for its type - fetching
        ## the bytes here meant every copy read the whole file for nothing.
        model = await self.get(path, content=False)
        # limit the size of folders being copied to prevent a timeout error
        if model["type"] == "directory":
            await self.check_folder_size(path)
        else:
            # Copied from AsyncContentManager and OS functionality replaced with cs3_fs functionality
            is_destination_specified = to_path is not None
            if not is_destination_specified:
                to_path = from_dir
            if await self.dir_exists(to_path):
                name = copy_pat.sub(".", from_name)
                to_name = await self.increment_filename(name, to_path, insert="-Copy")
                to_path = f"{to_path}/{to_name}"
            elif is_destination_specified:
                if "/" in to_path:
                    to_dir, to_name = to_path.rsplit("/", 1)
                    if not await self.dir_exists(to_dir):
                        raise HTTPError(404, "No such parent directory: %s to copy file in" % to_dir)
            else:
                raise HTTPError(404, "No such directory: %s" % to_path)

            src_os_path = self._get_os_path(path)
            dest_os_path = self._get_os_path(to_path)

            with self.perm_to_403(dest_os_path):
                await self._copy(src_os_path, dest_os_path)

            model = await self.get(to_path, content=False)
            self.emit(data={"action": "copy", "path": to_path, "source_path": from_path})
            return model

        is_destination_specified = to_path is not None
        to_name = copy_pat.sub(".", from_name)
        if not is_destination_specified:
            to_path = from_dir
        if await self.exists(to_path):
            name = copy_pat.sub(".", from_name)
            to_name = await self.increment_filename(name, to_path, insert="-Copy")
        to_path = f"{to_path}/{to_name}"

        return await self._copy_dir(
            from_path=from_path,
            to_path_original=to_path_original,
            to_name=to_name,
            to_path=to_path,
        )

    # Upstream uses subprocess to call du command to get directory size
    # and upstream is significantly more complex so no point in pushing this upstream
    async def _get_dir_size(self, path: str = ".") -> str:
        return await run_sync(self.storage.get_dir_size, str(path))  # type:ignore[return-value]


class CS3LargeFileManager(CS3FileContentsManager, UpstreamLargeFileManager):
    """Pure CS3: every read, write and stat goes through the CS3 APIs."""
