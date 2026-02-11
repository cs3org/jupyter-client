"""A contents manager that uses the local file system for storage."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations


import typing as t
from datetime import datetime

from jupyter_core.utils import run_sync
from anyio.to_thread import run_sync
from tornado import web
from jupyter_server import _tz as tz
from jupyter_server.services.contents.filemanager import AsyncFileContentsManager, FileContentsManager

# Note this is an import from our own file - these functions/methods HAVE to overload the upstream equivalents
from .filemanager import  is_file_hidden, is_hidden, naive_same_file

'''
These are modifications to the upstream Jupyter FileManager to handle large file uploads
with the os functionality replaced with "self.<method>" calls (in this case inhereted from CS3FileContentsManager),
some functions have been reimplemented in filemanager.py where the implementation differs more significantly and we can't
simply replace os calls.

See comments with ## in the class below for lines that have been changed from upstream.
'''
class UpstreamFileManager(AsyncFileContentsManager, FileContentsManager):
    """An async file contents manager."""

    # Inherit from FileContentsManager but we need to replace os functions.
    def _base_model(self, path):
        """Build the common base of a contents model"""
        os_path = self._get_os_path(path)
        ## replaced os.lstat
        info = self.lstat(os_path)

        four_o_four = "file or directory does not exist: %r" % path

        ## replaced is_hidden with implementation above class
        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            self.log.info("Refusing to serve hidden file or directory %r, via 404 Error", os_path)
            raise web.HTTPError(404, four_o_four)

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
        model["writable"] = self.is_writable(path)
        model["hash"] = None
        model["hash_algorithm"] = None

        return model

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
        if self.is_dir(os_path):
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
        ## replaced is_hidden with implementation above class
        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, "Cannot create hidden directory %r" % os_path)
        ## replaced os.path.exists (now async)
        if not await self.exists(os_path):
            with self.perm_to_403():
                # replaced os.mkdir
                await run_sync(self.mkdir, os_path)
        ## replaced os.path.isdir
        elif not self.is_dir(os_path):
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
        
        ## replaced is_hidden with implementation above class
        if not self.allow_hidden and (
            is_hidden(old_os_path, self.root_dir) or is_hidden(new_os_path, self.root_dir)
        ):
            raise web.HTTPError(400, f"Cannot rename file or directory {old_os_path!r}")

        ## replaced os.path.exists (now async) and samefile
        if await self.exists(new_os_path) and not naive_same_file(old_os_path, new_os_path):
            raise web.HTTPError(409, "File already exists: %s" % new_path)
        
        try:
            with self.perm_to_403():
                ## replaced shutil.move
                await run_sync(self.move, old_os_path, new_os_path)
        except web.HTTPError:
            raise
        except Exception as e:
            raise web.HTTPError(500, f"Unknown error renaming file: {old_path} {e}") from e
        
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
            await self.copy_tree(os_from_path, os_to_path)
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
        return self.is_dir(os_path)
    
    async def file_exists(self, path):
        """Does a file exist at the given path"""
        path = path.strip("/")
        os_path = self._get_os_path(path)
        ## replaced os.path.isfile
        return self.is_file(os_path)
    

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