"""Upstream (Large)FileContentsManager methods with the os calls replaced by CS3 calls."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations


import typing as t

from anyio.to_thread import run_sync
from tornado import web
from jupyter_server.services.contents.filemanager import AsyncFileContentsManager, FileContentsManager

from .common import decode_model_content, rename_errors_to_http
# These path checks HAVE to replace the upstream equivalents, which stat the local disk
from .paths import is_hidden, naive_same_file

'''
These are modifications to the upstream Jupyter FileManager to handle large file uploads
with the os functionality replaced with "self.<method>" calls (in this case inhereted from CS3FileContentsManager),
some functions have been reimplemented in pure.py where the implementation differs more significantly and we can't
simply replace os calls.

See comments with ## in the class below for lines that have been changed from upstream.
'''
class UpstreamFileManager(AsyncFileContentsManager, FileContentsManager):
    """An async file contents manager."""

    async def get(self, path, content=True, type=None, format=None, require_hash=False):
        """Takes a path for an entity and returns its model

        Parameters
        ----------
        path : str
            the API path that describes the relative path for the target
        content : bool
            Whether to include the contents in the reply
        type : str, optional
            The requested type - 'file', 'notebook', or 'directory'.
            Will raise HTTPError 400 if the content doesn't match.
        format : str, optional
            The requested format for file contents. 'text' or 'base64'.
            Ignored if this returns a notebook or directory model.
        require_hash: bool, optional
            Whether to include the hash of the file contents.

        Returns
        -------
        model : dict
            the contents model. If content=True, returns the contents
            of the file or directory as well.
        """
        ## self.exists is now async
        if not await self.exists(path):
            raise web.HTTPError(404, "No such file or directory: %s" % path)

        os_path = self._get_os_path(path)
        ## replaced os.path.isdir
        if self.storage.is_dir(os_path):
            if type not in (None, "directory"):
                raise web.HTTPError(
                    400,
                    f"{path} is a directory, not a {type}",
                    reason="bad type",
                )
            model = await self._dir_model(path, content=content)
        elif type == "notebook" or (type is None and path.endswith(".ipynb")):
            model = await self._notebook_model(path, content=content, require_hash=require_hash)
        else:
            if type == "directory":
                raise web.HTTPError(400, "%s is not a directory" % path, reason="bad type")
            model = await self._file_model(
                path, content=content, format=format, require_hash=require_hash
            )
        self.emit(data={"action": "get", "path": path})
        return model

    async def _save_directory(self, os_path, model, path=""):
        """create a directory"""
        ## replaced is_hidden with the one in paths.py
        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, "Cannot create hidden directory %r" % os_path)
        ## replaced os.path.exists (now async)
        ## os_path is already an OS path: self.exists() takes an API path and
        ## would re-prefix it with root_dir, so it can never match.
        if not await run_sync(self.storage.exists, os_path):
            with self.perm_to_403():
                # replaced os.mkdir
                await run_sync(self.storage.mkdir, os_path)
        ## replaced os.path.isdir
        elif not self.storage.is_dir(os_path):
            raise web.HTTPError(400, "Not a directory: %s" % (os_path))
        else:
            self.log.debug("Directory %r already exists", os_path)

    async def rename_file(self, old_path, new_path):
        """Rename a file."""
        old_path = old_path.strip("/")
        new_path = new_path.strip("/")
        if new_path == old_path:
            return

        new_os_path = self._get_os_path(new_path)
        old_os_path = self._get_os_path(old_path)

        ## replaced is_hidden with the one in paths.py
        if not self.allow_hidden and (
            is_hidden(old_os_path, self.root_dir) or is_hidden(new_os_path, self.root_dir)
        ):
            raise web.HTTPError(400, f"Cannot rename file or directory {old_os_path!r}")

        ## replaced os.path.exists (now async) and samefile
        ## new_os_path is an OS path - see _save_directory, self.exists() would
        ## re-prefix it and silently never fire, pushing the clash to the storage.
        if await run_sync(self.storage.exists, new_os_path) and not naive_same_file(
            old_os_path, new_os_path
        ):
            raise web.HTTPError(409, "File already exists: %s" % new_path)

        ## Pre-check so storages that don't enforce locks on rename still refuse.
        await run_sync(self._raise_if_foreign_locked, old_os_path, old_path)

        with rename_errors_to_http(old_path), self.perm_to_403():
            ## replaced shutil.move
            await run_sync(self.storage.rename, old_os_path, new_os_path)

    async def _copy_dir(
        self, from_path: str, to_path_original: str, to_name: str, to_path: str
    ) -> dict[str, t.Any]:
        """
        handles copying directories
        returns the model for the copied directory
        """
        try:
            os_from_path = self._get_os_path(from_path.strip("/"))
            os_to_path = f'{self._get_os_path(to_path_original.strip("/"))}/{to_name}'
            ## Replaced shutil.copytree with cs3_fs copy_tree
            await run_sync(self.storage.copy_tree, os_from_path, os_to_path)
            model = await self.get(to_path, content=False)
        except OSError as err:
            self.log.error(f"OSError in _copy_dir: {err}")
            raise web.HTTPError(
                400,
                f"Can't copy '{from_path}' into read-only Folder '{to_path}'",
            ) from err

        return model  # type:ignore[no-any-return]

    async def dir_exists(self, path):
        """Does a directory exist at the given path"""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        ## replaced os.path.isdir
        return self.storage.is_dir(os_path)

    async def file_exists(self, path):
        """Does a file exist at the given path"""
        path = path.strip("/")
        os_path = self._get_os_path(path)
        ## replaced os.path.isfile
        return self.storage.is_file(os_path)


    # Function replaced - upstream uses jupyter.core.paths.exists which uses os.path.exists
    async def exists(self, path):
        """Does a file or directory exist at the given path?

        Like os.path.exists

        Parameters
        ----------
        path : str
            The API path of a file or directory to check for.

        Returns
        -------
        exists : bool
            Whether the target exists.
        """
        return await self.file_exists(path) or await self.dir_exists(path)


'''
These are the methods with modifications to the upstream Jupyter LargeFileManager to handle large file uploads
with the os functionality replaced by "self.open" and "self.write".

See comments with ## in the class below for lines that have been changed from upstream.
'''
class UpstreamLargeFileManager(UpstreamFileManager):
    """Handle large file upload asynchronously"""

    # Copied from upstream AsyncLargeFileManager.save. We cannot inherit
    # AsyncLargeFileManager: its chunk == 1 `super()._save_file(...)` would resolve
    # to the local-disk mixin instead of the CS3 one, silently writing to disk.
    async def save(self, model, path=""):
        """Save the file model and return the model with no content."""
        chunk = model.get("chunk", None)
        if chunk is not None:
            path = path.strip("/")

            if chunk == 1:
                self.run_pre_save_hooks(model=model, path=path)

            if "type" not in model:
                raise web.HTTPError(400, "No file type provided")
            if model["type"] != "file":
                raise web.HTTPError(
                    400,
                    'File type "{}" is not supported for large file transfer'.format(model["type"]),
                )
            if "content" not in model and model["type"] != "directory":
                raise web.HTTPError(400, "No file content provided")

            os_path = self._get_os_path(path)
            if chunk == -1:
                self.log.debug(f"Saving last chunk of file {os_path}")
            else:
                self.log.debug(f"Saving chunk {chunk} of file {os_path}")

            try:
                if chunk == 1:
                    ## self._save_file instead of super()._save_file so the first
                    ## chunk goes through the CS3 write (with locking).
                    await self._save_file(os_path, model["content"], model.get("format"))
                else:
                    await self._save_large_file(os_path, model["content"], model.get("format"))
            except web.HTTPError:
                raise
            except Exception as e:
                self.log.error("Error while saving file: %s %s", path, e, exc_info=True)
                raise web.HTTPError(500, f"Unexpected error while saving file: {path} {e}") from e

            model = await self.get(path, content=False)

            # Last chunk
            if chunk == -1:
                self.run_post_save_hooks(model=model, os_path=os_path)

            self.emit(data={"action": "save", "path": path})
            return model
        else:
            return await super().save(model, path)

    # Upstream uses os.open and f.write, we need to use cs3_fs
    # upstream also takes into account if it's a symlink, we skip that here
    async def _save_large_file(self, os_path, content, format):
        """Save content of a generic file."""
        bcontent = decode_model_content(content, format, os_path)
        ## This part is replaced to use cs3mixin open and write; the whole
        ## read-modify-write append runs off the event loop.
        await run_sync(self._append_bytes, os_path, bcontent)
