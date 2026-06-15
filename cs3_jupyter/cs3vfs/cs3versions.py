"""
CS3FileVersions provides methods for listing
and restoring file versions in a CS3 storage system.

Authors: Rasmus Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

from .utils import resource_from_path, retry_on_auth_failure
from typing import Generator

class CS3FileVersions:
    """
    CS3FileVersions provides methods for listing
    and restoring file versions in a CS3 storage system.
    """
    @retry_on_auth_failure
    def list_file_versions(self, path: str) -> Generator["FileVersion", any, any]:  # noqa: F821
        """List file versions"""
        try:
            resource = resource_from_path(path)
            result = self.client.checkpoint.list_file_versions(
                self.auth.get_token(),
                resource
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def restore_file_version(self, path: str, key: str) -> None:
        """Restore a file version."""
        try:
            resource = resource_from_path(path)
            self.client.checkpoint.restore_file_version(
                self.auth.get_token(),
                resource,
                key
            )
        except Exception as e:
            self.status_handler.handle_errors(e)
