"""The CS3 contents managers: methods that differ significantly from upstream."""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import annotations

import errno
import os
import shutil
import stat

import nbformat
from anyio.to_thread import run_sync
from jupyter_server import _tz as tz
from jupyter_server.services.contents.manager import copy_pat
from tornado import web
from tornado.web import HTTPError
from traitlets import default, validate

from .fileio import CS3FileManagerMixin, CS3HybridFileManagerMixin, locked_to_423, rename_errors_to_http
from .paths import is_file_hidden, is_hidden, naive_same_file


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
        return self.get_user_path()

    # We cannot validate before starting if it's a virtual filesystem.
    @validate("root_dir")
    def _validate_root_dir(self, proposal):
        return proposal.value

    # We cannot validate before starting if it's a virtual filesystem.
    @validate("preferred_dir")
    def _validate_preferred_dir(self, proposal):
        return proposal.value

    # Upstream uses os.access; caching is left to the TTL stat cache under
    # self.access, which - unlike a per-path dict - expires and is invalidated
    # by mutations.
    def is_writable(self, path):
        """Does the API style path correspond to a writable directory or file?"""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        try:
            return self.access(os_path, os.W_OK)
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
        await run_sync(self._delete, os_path, path, self.is_dir(os_path))

    async def _dir_model(self, path, content=True):
        """Build a model for a directory

        if content is requested, will include a listing of the directory
        """
        os_path = self._get_os_path(path)

        four_o_four = "directory does not exist: %r" % path
        ## Replaced os.path.isdir
        if not self.is_dir(os_path):
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
            dir_contents = await run_sync(self.list_dir, os_dir)

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
        return await run_sync(self.get_dir_size, str(path))  # type:ignore[return-value]

class CS3HybridFileManager(CS3HybridFileManagerMixin):
    """A sync contents manager: CS3 for all mutations, FUSE as a metadata fast path.

    Methods below are copies of the upstream sync FileContentsManager methods
    with the os calls swapped for the hybrid_* helpers (FUSE-first, CS3
    fallback) and mutations routed through CS3 so they carry/respect locks.
    """

    def is_writable(self, path):
        """Does the API style path correspond to a writable directory or file?"""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        try:
            ## fall back to CS3 for files not yet visible in the mount
            if not os.path.exists(os_path):
                return self.access(os_path, os.W_OK)
            return os.access(os_path, os.W_OK)
        except OSError:
            self.log.error("Failed to check write permissions on %s", os_path)
            return False

    def file_exists(self, path):
        """Returns True if the file exists, else returns False."""
        path = path.strip("/")
        os_path = self._get_os_path(path)
        ## replaced os.path.isfile
        return self.hybrid_isfile(os_path)

    def dir_exists(self, path):
        """Does the API-style path refer to an extant directory?"""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        ## replaced os.path.isdir
        return self.hybrid_isdir(os_path)

    def exists(self, path):
        """Returns True if the path exists, else returns False."""
        path = path.strip("/")
        os_path = self._get_os_path(path=path)
        ## replaced os.path.exists
        return self.hybrid_exists(os_path)

    def get(self, path, content=True, type=None, format=None, require_hash=False):
        """Takes a path for an entity and returns its model"""
        path = path.strip("/")
        os_path = self._get_os_path(path)
        four_o_four = "file or directory does not exist: %r" % path

        if not self.exists(path):
            raise web.HTTPError(404, four_o_four)

        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            self.log.info("Refusing to serve hidden file or directory %r, via 404 Error", os_path)
            raise web.HTTPError(404, four_o_four)

        ## replaced os.path.isdir
        if self.hybrid_isdir(os_path):
            if type not in (None, "directory"):
                raise web.HTTPError(
                    400,
                    f"{path} is a directory, not a {type}",
                    reason="bad type",
                )
            model = self._dir_model(path, content=content)
        elif type == "notebook" or (type is None and path.endswith(".ipynb")):
            model = self._notebook_model(path, content=content, require_hash=require_hash)
        else:
            if type == "directory":
                raise web.HTTPError(400, "%s is not a directory" % path, reason="bad type")
            model = self._file_model(
                path, content=content, format=format, require_hash=require_hash
            )
        self.emit(data={"action": "get", "path": path})
        return model

    def _dir_model(self, path, content=True):
        """Build a model for a directory.

        The mount is the fast path, but it cannot see what we changed through
        CS3 until its own cache expires - so a notebook renamed via reva kept
        being listed under its old name. While the mount still disagrees about
        something we touched (or has not seen the directory at all), the names
        come from CS3; each entry's model is then built by self.get, which is
        already FUSE-first with a CS3 fallback.
        """
        os_path = self._get_os_path(path)
        if not self.hybrid_isdir(os_path):
            raise web.HTTPError(404, "directory does not exist: %r" % path)

        if os.path.isdir(os_path) and not self.mount_lags_in(os_path):
            return super()._dir_model(path, content=content)

        names = self.cs3_entry_names(os_path)
        if names is None:
            ## CS3 cannot list it: a stale view from the mount still beats none,
            ## and an empty model is the last resort for a directory it has not
            ## seen either (the window right after a create).
            if os.path.isdir(os_path):
                return super()._dir_model(path, content=content)
            names = []

        model = self._base_model(path)
        model["type"] = "directory"
        model["size"] = None
        if content:
            model["content"] = contents = []
            for name in names:
                if not self.should_list(name):
                    continue
                if not self.allow_hidden and is_file_hidden(os.path.join(os_path, name)):
                    continue
                try:
                    contents.append(self.get(path=f"{path}/{name}", content=False))
                except web.HTTPError:
                    continue  # vanished between the listing and the stat
            model["format"] = "json"
        return model

    def _save_directory(self, os_path, model, path=""):
        """create a directory"""
        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, "Cannot create directory %r" % os_path)
        if not self.hybrid_exists(os_path):
            with self.perm_to_403():
                ## replaced os.mkdir with CS3 mkdir
                self.mkdir(os_path)
        elif not self.hybrid_isdir(os_path):
            raise web.HTTPError(400, "Not a directory: %s" % (os_path))
        else:
            self.log.debug("Directory %r already exists", os_path)

    def save(self, model, path=""):
        """Save the file model and return the model with no content."""
        # Chunked uploads are handled by LargeFileManager (next in the MRO);
        # its low-level writes resolve back to the CS3 open/append.
        if model.get("chunk") is not None:
            return super().save(model, path)

        path = path.strip("/")

        self.run_pre_save_hooks(model=model, path=path)

        if "type" not in model:
            raise web.HTTPError(400, "No file type provided")
        if "content" not in model and model["type"] != "directory":
            raise web.HTTPError(400, "No file content provided")
        os_path = self._get_os_path(path)

        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, f"Cannot create file or directory {os_path!r}")

        self.log.debug("Saving %s", os_path)

        validation_error: dict = {}
        try:
            if model["type"] == "notebook":
                nb = nbformat.from_dict(model["content"])
                self.check_and_sign(nb, path)
                self._save_notebook(os_path, nb, capture_validation_error=validation_error)
                ## upstream creates a checkpoint here; reva versions files for
                ## us and list_checkpoints is async, so the block is dropped
                ## (it never ran anyway - the coroutine was always truthy).
            elif model["type"] == "file":
                self._save_file(os_path, model["content"], model.get("format"))
            elif model["type"] == "directory":
                self._save_directory(os_path, model, path)
            else:
                raise web.HTTPError(400, "Unhandled contents type: %s" % model["type"])
        except web.HTTPError:
            raise
        except Exception as e:
            self.log.error("Error while saving file: %s %s", path, e, exc_info=True)
            raise web.HTTPError(500, f"Unexpected error while saving file: {path} {e}") from e

        validation_message = None
        if model["type"] == "notebook":
            self.validate_notebook_model(model, validation_error=validation_error)
            validation_message = model.get("message", None)

        model = self.get(path, content=False)
        if validation_message:
            model["message"] = validation_message

        self.run_post_save_hooks(model=model, os_path=os_path)
        self.emit(data={"action": "save", "path": path})
        return model

    def rename_file(self, old_path, new_path):
        """Rename a file."""
        old_path = old_path.strip("/")
        new_path = new_path.strip("/")
        if new_path == old_path:
            return

        new_os_path = self._get_os_path(new_path)
        old_os_path = self._get_os_path(old_path)

        if not self.allow_hidden and (
            is_hidden(old_os_path, self.root_dir) or is_hidden(new_os_path, self.root_dir)
        ):
            raise web.HTTPError(400, f"Cannot rename file or directory {old_os_path!r}")

        ## replaced os.path.exists and samefile
        if self.hybrid_exists(new_os_path) and not naive_same_file(old_os_path, new_os_path):
            raise web.HTTPError(409, "File already exists: %s" % new_path)

        is_dir = self.hybrid_isdir(old_os_path)

        ## Pre-check so storages that don't enforce locks on rename still refuse.
        ## Containers cannot hold EOS locks, so asking about one is a wasted RPC.
        if not is_dir:
            self._raise_if_foreign_locked(old_os_path, old_path)

        with rename_errors_to_http(old_path), self.perm_to_403():
            if is_dir and os.path.isdir(old_os_path):
                ## Nothing to arbitrate on a container: one rename syscall
                ## on the mount beats a Move RPC carrying a lock id that
                ## could never apply. Files keep going through CS3, where
                ## the lock id is what permits the move.
                shutil.move(old_os_path, new_os_path)
                self.invalidate_stat(old_os_path, new_os_path)
            else:
                ## replaced shutil.move with CS3 rename (carries our lock id)
                self.move(old_os_path, new_os_path)

    def delete_file(self, path):
        """Delete file at path."""
        path = path.strip("/")
        os_path = self._get_os_path(path)

        if not self.allow_hidden and is_hidden(os_path, self.root_dir):
            raise web.HTTPError(400, f"Cannot delete file or directory {os_path!r}")
        if not self.hybrid_exists(os_path):
            raise web.HTTPError(404, "file or directory does not exist: %r" % path)

        self._delete(os_path, path, self.hybrid_isdir(os_path))

    # See CS3ContentsMixin._mark_read_only_if_foreign_locked.
    def _notebook_model(self, path, content=True, require_hash=False):
        model = super()._notebook_model(path, content=content, require_hash=require_hash)
        if content:
            self._mark_read_only_if_foreign_locked(model, path)
        return model

    def _file_model(self, path, content=True, format=None, require_hash=False):
        model = super()._file_model(path, content=content, format=format, require_hash=require_hash)
        if content:
            self._mark_read_only_if_foreign_locked(model, path)
        return model

    def copy(self, from_path, to_path=None):
        """Copy a file with a server-side CS3 copy, or a directory via upstream.

        Upstream sends files to ContentsManager.copy, which is get(content=True)
        followed by save(): the whole file is pulled into a model, JSON-encoded,
        and written back through the lock funnel - and save() then locks the
        destination through lock_after_create with nothing to release it, so
        copies came back read-only until lock_expiration (300s) elapsed.

        copyfile_sync streams it in the storage instead, and leaves no lock: a
        copy destination is a new file, not an open document. Writes still
        respect locks - the streamed write carries no lock id, so reva refuses
        it if something else holds the destination.
        """
        path = from_path.strip("/")
        if self.dir_exists(path):
            ## directories keep upstream's tree copy
            return super().copy(from_path, to_path)

        if to_path is not None:
            to_path = to_path.strip("/")

        from_dir, from_name = path.rsplit("/", 1) if "/" in path else ("", path)

        ## naming below is upstream's ContentsManager.copy, verbatim
        is_destination_specified = to_path is not None
        if not is_destination_specified:
            to_path = from_dir
        if self.dir_exists(to_path):
            name = copy_pat.sub(".", from_name)
            to_name = self.increment_filename(name, to_path, insert="-Copy")
            to_path = f"{to_path}/{to_name}"
        elif is_destination_specified:
            if "/" in to_path:
                to_dir, _ = to_path.rsplit("/", 1)
                if not self.dir_exists(to_dir):
                    raise web.HTTPError(
                        404, "No such parent directory: %s to copy file in" % to_dir
                    )
        else:
            raise web.HTTPError(404, "No such directory: %s" % to_path)

        src_os_path = self._get_os_path(path)
        dest_os_path = self._get_os_path(to_path)

        ## A copy needs no lock of its own: the destination is a new file, and
        ## reading a locked source is allowed (the model just renders read-only).
        ## The one exception is an explicit destination that already exists,
        ## which would clobber it. Costs nothing when the name was incremented -
        ## increment_filename just cached that same negative stat.
        if self.hybrid_isfile(dest_os_path):
            self._raise_if_foreign_locked(dest_os_path, to_path)

        with locked_to_423(to_path), self.perm_to_403(dest_os_path):
            if os.path.exists(src_os_path):
                ## folder copies already go through the mount (copytree);
                ## a single file has no reason to cost RPCs either
                shutil.copyfile(src_os_path, dest_os_path)
                self.invalidate_stat(dest_os_path)
            else:
                ## source created through CS3 and not in the mount yet;
                ## copyfile_sync notes the change itself
                self.copyfile_sync(src_os_path, dest_os_path)

        model = self.get(to_path, content=False)
        self.emit(data={"action": "copy", "path": to_path, "source_path": from_path})
        return model

    # Upstream shells out to `du -s` against the mount, which is slow on EOS.
    # One CS3 stat carries the tree size in its opaque metadata instead.
    def _get_dir_size(self, path: str = ".") -> str:
        return self.get_dir_size(str(path))  # type:ignore[return-value]
