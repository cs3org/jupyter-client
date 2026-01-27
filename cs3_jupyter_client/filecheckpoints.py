"""
File-based Checkpoints implementations.
"""

from tornado.web import HTTPError
from traitlets import Unicode
import datetime
from jupyter_server.services.contents.checkpoints import (
    AsyncCheckpoints,
)
from .fileio import CS3FileManagerMixin



class CS3FileCheckpoints(CS3FileManagerMixin,AsyncCheckpoints):

    root_dir = Unicode(config=True)

    def _root_dir_default(self):
        return self.get_user_path()

    # If available, the CS3 backend/Reva creates checkpoints automatically,
    # meaning we don't need to create checkpoints and this has no real 
    # action except for listing the existing checkpoints.
    async def create_checkpoint(self, contents_mgr, path):
        """Create a checkpoint."""
        os_path = contents_mgr._get_os_path(path)
        versions = contents_mgr.list_file_versions(os_path)
        # The versions have the following keys:
        # 'Key', 'Size', 'Mtime', 'Etag'
        # and we need to find the latest version based on Mtime
        if versions:
            latest_version = max(versions, key=lambda v: v.mtime)
            checkpoint_id = latest_version.key
            last_modified = str(latest_version.mtime)
            return {
                "id": checkpoint_id,
                "last_modified": last_modified,
            }
        # Return a mock checkpoint if no versions exist, otherwise jupyter won't open the file.
        else:
            return self._get_mock_checkpoint(path, contents_mgr)

            

    # Restore reva checkpoint
    async def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """Restore a checkpoint."""
        os_path = contents_mgr._get_os_path(path)
        contents_mgr.restore_file_version(os_path, checkpoint_id)
        return None
    
    # No need to construct info dict, reva handles versioning.
    async def checkpoint_model(self, checkpoint_id, os_path):
        """construct the info dict for a given checkpoint"""
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
    # FIXME: we want to decouple this from CS3FileManagerMixin but 
    # that would require an extra parameter "contents_mgr" in the function signature.
    async def list_checkpoints(self, path):
        """list the checkpoints for a given file"""
        os_path = self._get_os_path(path)
        try:
            self.lstat(os_path)
        ## We need to return a checkpoint here even if the file doesn't exist
        except OSError:
            return [self._get_mock_checkpoint(path, self)]
        versions = self.list_file_versions(os_path)
        checkpoints = []
        for version in versions:
            checkpoint_id = version.key
            last_modified = str(version.mtime)
            checkpoints.append({
                "id": checkpoint_id,
                "last_modified": last_modified,
            })
        if not checkpoints:
            # Return a mock checkpoint if no versions exist, otherwise jupyter will panic.
            checkpoints.append(self._get_mock_checkpoint(path, self))
        return checkpoints
    
    # a path for a checkpoint is just the file path in reva
    def checkpoint_path(self, checkpoint_id, path):
        """find the path to a checkpoint"""
        return path
    
    # Copied from SWAN upstream to create a mock checkpoint when no versions exist
    def _get_mock_checkpoint(self, path, contents_mgr):
        src_path = contents_mgr._get_os_path(path=path)
        # Jupyter will try create a checkpoint for a new file
        # and then list checkpoints, so we need to return something valid
        # even if the file isn't there yet in Reva.
        try:
            mtime = contents_mgr.lstat(src_path).st_mtime
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
