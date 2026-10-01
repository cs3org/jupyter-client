"""
Checkpoints backed by reva's file versions.
"""

import datetime

from tornado.web import HTTPError
from jupyter_server.services.contents.checkpoints import AsyncCheckpoints

from .cs3vfs.statuscodehandler import FileLockedError


class CS3FileCheckpoints(AsyncCheckpoints):
    """Reva versions every write, so checkpoints are just those versions.

    Creating, renaming and deleting checkpoints have nothing to do. Listing
    and restoring go through the contents manager - the one passed in, or
    self.parent for the methods that do not get it - so they share its CS3
    client and stat cache instead of opening a second connection.
    """

    # If available, the CS3 backend/Reva creates checkpoints automatically,
    # meaning we don't need to create checkpoints and this has no real
    # action except for listing the existing checkpoints.
    async def create_checkpoint(self, contents_mgr, path):
        """Create a checkpoint."""
        os_path = contents_mgr._get_os_path(path)
        versions = contents_mgr.versions.list_file_versions(os_path)
        if versions:
            return self._checkpoint_model(max(versions, key=lambda v: v.mtime))
        # Return a mock checkpoint if no versions exist, otherwise jupyter won't open the file.
        return self._get_mock_checkpoint(path, contents_mgr)

    # Restore reva checkpoint
    async def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """Restore a checkpoint."""
        # "0_0" is the placeholder for files without reva versions yet;
        # there is nothing to restore.
        if checkpoint_id == "0_0":
            return None
        os_path = contents_mgr._get_os_path(path)
        try:
            contents_mgr.versions.restore_file_version(os_path, checkpoint_id)
        except FileLockedError as e:
            raise HTTPError(423, f"{path} is locked") from e
        return None

    # Renaming checkpoints is not needed, reva handles versioning.
    async def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """Rename a checkpoint from old_path to new_path."""
        return None

    # Deleting checkpoints is not needed, reva handles versioning.
    async def delete_checkpoint(self, checkpoint_id, path):
        """delete a file's checkpoint"""
        return None

    # Reva handles versioning, so list all versions as checkpoints.
    async def list_checkpoints(self, path):
        """list the checkpoints for a given file"""
        contents_mgr = self.parent
        os_path = contents_mgr._get_os_path(path)
        try:
            contents_mgr.storage.lstat(os_path)
        ## We need to return a checkpoint here even if the file doesn't exist
        except OSError:
            return [self._get_mock_checkpoint(path, contents_mgr)]
        checkpoints = [self._checkpoint_model(v) for v in contents_mgr.versions.list_file_versions(os_path)]
        # Return a mock checkpoint if no versions exist, otherwise jupyter will panic.
        return checkpoints or [self._get_mock_checkpoint(path, contents_mgr)]

    @staticmethod
    def _checkpoint_model(version):
        # Versions carry 'Key', 'Size', 'Mtime' and 'Etag'.
        return {"id": version.key, "last_modified": str(version.mtime)}

    # Copied from SWAN upstream to create a mock checkpoint when no versions exist
    def _get_mock_checkpoint(self, path, contents_mgr):
        src_path = contents_mgr._get_os_path(path=path)
        # Jupyter will try create a checkpoint for a new file
        # and then list checkpoints, so we need to return something valid
        # even if the file isn't there yet in Reva.
        try:
            mtime = contents_mgr.storage.lstat(src_path).st_mtime
        except Exception:
            mtime = datetime.datetime.now().timestamp()
        ts = datetime.datetime.fromtimestamp(mtime)
        return dict(
                id = "0_0",
                last_modified = ts.strftime('%Y-%m-%dT%H:%M:%S')
            )

    # Error Handling
    def no_such_checkpoint(self, path, checkpoint_id):
        raise HTTPError(404, f"Checkpoint does not exist: {path}@{checkpoint_id}")
