"""
CS3 operations for sharing.

Authors: Rasmus Oscar Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

from typing import List, Optional
from tornado import web
from .utils import resource_from_path, retry_on_auth_failure


class CS3Sharing:
    """
    CS3 operations for sharing.
    """
    @retry_on_auth_failure
    def create_share(self, path: str, opaque_id: str, idp: str, role: str, grantee_type: str) -> None:
        """Create a share for a given resource to a target user."""
        try:
            resource = resource_from_path(path)
            # We need the resource info for creating the share
            resource_info = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            if resource_info is None:
                raise web.HTTPError(404, "Resource not found: %s" % path)
            share = self.client.share.create_share(
                self.auth.get_token(),
                resource_info,
                opaque_id,
                idp,
                role,
                grantee_type
            )
            return share
        except Exception as e:
            self.status_handler.handle_errors(e)

    # This is when we want to list shares for a specific resource, such as
    # who we have shared a specific file with.
    @retry_on_auth_failure
    def list_existing_shares_by_resource(self, path) -> List[dict]:
        """List existing shares for a given resource."""
        resource = resource_from_path(path)
        # We need to use the resource ID filter to get shares for a specific resource the path is not enough.
        try:
            resource_info = self.client.file.stat(self.auth.get_token(), resource)
            filter = self.client.share.create_share_filter(filter_type="TYPE_RESOURCE_ID", resource_id=resource_info.id)
            result = self.client.share.list_existing_shares(
                self.auth.get_token(),
                filter_list=[filter]
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    # This is when we want to list "shared by me" shares
    def list_existing_shares_by_creator(self, creator_idp: str, creator_opaque_id: str) -> List[dict]:
        """List existing shares created by a user."""
        filter = self.client.share.create_share_filter(filter_type="TYPE_CREATOR", creator_opaque_id=creator_opaque_id, creator_idp=creator_idp)
        try:
            result = self.client.share.list_existing_shares(
                self.auth.get_token(),
                [filter]
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def remove_share(self, share_id: str) -> None:
        """Remove a share by its ID."""
        try:
            self.client.share.remove_share(
                self.auth.get_token(),
                opaque_id=share_id
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def update_share(self, share_id: str, role: str = None, display_name: str = None) -> None:
        """Update a shares role/display name by using its unique ID."""
        try:
            share = self.client.share.update_share(
                self.auth.get_token(),
                role=role,
                opaque_id=share_id,
                display_name=display_name,
            )
        except Exception as e:
            self.status_handler.handle_errors(e)
        return share

    @retry_on_auth_failure
    def list_received_existing_shares(self) -> List[dict]:
        """List existing received shares."""
        try:
            result = self.client.share.list_received_existing_shares(
                self.auth.get_token()
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def update_received_share(self, share_id: str, hidden: bool) -> None:
        """Update a received shares state by using its unique ID."""
        if not hidden:
            state = "SHARE_STATE_ACCEPTED"
        else:
            state = "SHARE_STATE_REJECTED"
        try:
            self.client.share.update_received_share(
                self.auth.get_token(),
                share_id,
                state
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def create_public_share(self, path: str, role: str, password: str = None, expiration: str = None,
                            description: str = None, internal: bool = False, notify_uploads: bool = False,
                            notify_uploads_extra_recipients: Optional[list] = None) -> dict:
        """Create a public share for a given resource."""
        try:
            resource = resource_from_path(path)
            resource_info = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            if resource_info is None:
                raise web.HTTPError(404, "Resource not found: %s" % path)
            share = self.client.share.create_public_share(
                self.auth.get_token(),
                resource_info,
                role=role,
                password=password,
                expiration=expiration,
                description=description,
                internal=internal,
                notify_uploads=notify_uploads,
                notify_uploads_extra_recipients=notify_uploads_extra_recipients
            )
            return share
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def list_existing_public_shares_by_creator(self, creator_idp: str, creator_opaque_id: str) -> List[dict]:
        """List existing public shares by creator."""
        filter = self.client.share.create_public_share_filter(filter_type="TYPE_CREATOR", creator_idp=creator_idp, creator_opaque_id=creator_opaque_id)
        try:
            result = self.client.share.list_existing_public_shares(
                self.auth.get_token(),
                [filter]
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def list_existing_public_shares_by_resource(self, path: str) -> List[dict]:
        """List existing public shares for a given resource."""
        resource = resource_from_path(path)
        try:
            resource_info = self.client.file.stat(self.auth.get_token(), resource)
            filter = self.client.share.create_public_share_filter(filter_type="TYPE_RESOURCE_ID", resource_id=resource_info.id)
            result = self.client.share.list_existing_public_shares(
                self.auth.get_token(),
                filter_list=[filter]
            )
            return result if result is not None else []
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def update_public_share(self, share_id: str, type: str,role: str = None, password: str = None,
                            expiration: str = None, description: str = None,
                            notify_uploads: bool = None, display_name: str = None,  notify_uploads_extra_recipients: Optional[list] = None) -> None:
        """Update a public share by its ID."""
        try:
            share = self.client.share.update_public_share(
                self.auth.get_token(),
                type=type,
                role=role,
                opaque_id=share_id,
                password=password,
                expiration=expiration,
                description=description,
                notify_uploads=notify_uploads,
                display_name=display_name,
                notify_uploads_extra_recipients=notify_uploads_extra_recipients
            )
        except Exception as e:
            self.log.error("Error updating public share:", e)
            self.status_handler.handle_errors(e)
        return share

    @retry_on_auth_failure
    def remove_public_share(self, share_id: str) -> None:
        """Remove a public share by its ID."""
        try:
            self.client.share.remove_public_share(
                self.auth.get_token(),
                opaque_id=share_id
            )
        except Exception as e:
            self.status_handler.handle_errors(e)
