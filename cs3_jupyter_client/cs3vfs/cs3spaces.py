"""
CS3 operations for spaces.

Authors: Rasmus Oscar Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""


from .utils import resource_from_path
from typing import List
from .utils import retry_on_auth_failure

class CS3Spaces:
    """
    CS3 operations for spaces.
    """

    @retry_on_auth_failure
    def get_quota(self, path: str) -> 'QuotaResponse':  # noqa: F821
        """Get resource quota."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.get_quota(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

        return result

    @retry_on_auth_failure
    def list_spaces(self) -> List[dict]:
        """List existing spaces."""
        filter = self.client.space.create_storage_space_filter(filter_type = "TYPE_SPACE_TYPE", space_type = "project")
        try:
            result = self.client.space.list_storage_spaces(
                self.auth.get_token(),
                [filter]
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)
