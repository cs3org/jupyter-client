# sharing/handlers.py

from tornado import web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from google.protobuf.json_format import MessageToDict
from ..cs3vfs.statuscodehandler import ErrorToHttpCode
from .utils import cache_get_user_info

class _CS3HandlerBase(APIHandler):

    @property
    def cs3(self):
        return self.settings["cs3_sharing_client"]


class SharesHandler(_CS3HandlerBase):

    @web.authenticated
    async def post(self):
        """
        Create a share for a resource.
        :query param path: path to the resource(REQUIRED).
        :field opaque_id: Opaque group/user id, (REQUIRED).
        :field idp: Identity provider, (REQUIRED).
        :field role: Role to assign to the grantee, VIEWER or EDITOR (REQUIRED).
        :field grantee_type: Type of grantee, USER or GROUP (REQUIRED).
        """
        # Get the resource path from query parameters
        path = self.get_query_argument("path", default="")
        # Get other parameters from the request body
        body = self.get_json_body() or {}
        opaque_id = body.get("opaque_id", "")
        idp = body.get("idp", "")
        role = body.get("role", "")
        grantee_type = body.get("grantee_type", "USER")

        self.log.info(f"Creating share for path: {path} to {grantee_type} {opaque_id} with role {role}")
        try:
            share = self.cs3.create_share(path, opaque_id, idp, role, grantee_type)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return

        share = MessageToDict(share, preserving_proto_field_name=True)

        self.set_status(201)
        self.write({"created": True, "data": body, "path": path, "share": share})

    @web.authenticated
    async def put(self):
        """
        Update a share for a resource.
        :query param share_id: The ID of the share to update (REQUIRED).
        :field role: Role to update the share, VIEWER or EDITOR (REQUIRED).
        :field display_name: new display name.
        """
        # Get the resource path from query parameters
        share_id = self.get_query_argument("share_id", default=None)
        body = self.get_json_body() or {}
        role = body.get("role", None)
        display_name = body.get("display_name", None)

        self.log.info(f"Updating share: {share_id} with role {role} and display name {display_name}")
        try:
            share = self.cs3.update_share(share_id, role=role, display_name=display_name)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return

        share = MessageToDict(share, preserving_proto_field_name=True)
        self.set_status(200)
        self.write({"updated": True, "data": body, "share": share})

    @web.authenticated
    async def delete(self):
        """
        Remove a share for a resource.
        :field share_id: The ID of the share to remove (REQUIRED).
        """
        # Get the resource path from query parameters
        share_id = self.get_query_argument("share_id", default=None)
        try:
            self.cs3.remove_share(share_id)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return

        self.set_status(204)

class LinkHandler(_CS3HandlerBase):

    @web.authenticated
    async def post(self):
        """
        Create a public link for a resource.
        :query param path: path to the resource(REQUIRED).
        :field role: Role to assign to the grantee, VIEWER or EDITOR (REQUIRED)
        :field password: Password to access the share.
        :field expiration: Expiration timestamp for the share.
        :field description: Description for the share.
        :field internal: Internal share flag.
        :field notify_upload: Notify upload flag.
        :field notify_uploads_extra_recipients: List of extra recipients to notify on upload.
        """
        # Get the resource path from query parameters
        path = self.get_query_argument("path", default="")
        # Get other parameters from the request body
        body = self.get_json_body() or {}
        role = body.get("role", "")
        password = body.get("password", None)
        expiration = body.get("expiration", None)
        description = body.get("description", None)
        internal = body.get("internal", False)
        notify_uploads = body.get("notify_uploads", False)
        notify_uploads_extra_recipients = body.get("notify_uploads_extra_recipients", None)

        self.log.info(f"Creating public share for path: {path} with role {role}")
        try:
            share = self.cs3.create_public_share(
                path,
                role,
                password=password,
                expiration=expiration,
                description=description,
                internal=internal,
                notify_uploads=notify_uploads,
                notify_uploads_extra_recipients=notify_uploads_extra_recipients
            )
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return

        share = MessageToDict(share, preserving_proto_field_name=True)

        self.set_status(201)
        self.write({"created": True, "data": body, "path": path, "share": share})

    @web.authenticated
    async def put(self):
        """
        Update a public link for a resource.
        :query param opaque_id: The ID of the share to update (REQUIRED).
        :field type: Type of update to perform TYPE_PERMISSIONS, TYPE_PASSWORD, TYPE_EXPIRATION, TYPE_DISPLAYNAME,
                        TYPE_DESCRIPTION, TYPE_NOTIFYUPLOADS, TYPE_NOTIFYUPLOADSEXTRARECIPIENTS (REQUIRED).
        :field role: Role to assign to the grantee, VIEWER or EDITOR (REQUIRED).
        :field opaque_id: Opaque share id (REQUIRED).
        :field display_name: Display name for the share.
        :field description: Description for the share.
        :field notify_uploads: Notify uploads flag.
        :field expiration: Expiration timestamp for the share.
        :field notify_uploads_extra_recipients: List of extra recipients to notify on upload.
        :field password: Password to access the share.
        """
        # Get the share_id from query parameters
        share_id = self.get_query_argument("share_id", default="")
        # Get other parameters from the request body
        body = self.get_json_body() or {}
        type = body.get("type", "")
        role = body.get("role", "")
        password = body.get("password", None)
        expiration = body.get("expiration", None)
        description = body.get("description", None)
        display_name = body.get("display_name", None)
        notify_uploads = body.get("notify_uploads", False)
        notify_uploads_extra_recipients = body.get("notify_uploads_extra_recipients", None)

        self.log.info(f"Updating public share: {share_id} with type {type} role {role}")
        try:
            share = self.cs3.update_public_share(
                share_id,
                type=type,
                role=role,
                password=password,
                expiration=expiration,
                description=description,
                notify_uploads=notify_uploads,
                display_name=display_name,
                notify_uploads_extra_recipients=notify_uploads_extra_recipients
            )
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        share = MessageToDict(share, preserving_proto_field_name=True)

        self.set_status(200)
        self.write({"updated": True, "data": body, "share": share})

    @web.authenticated
    async def delete(self):
        """
        Remove a share for a resource.
        :field share_id: The ID of the share to remove (REQUIRED).
        """
        # Get the resource path from query parameters
        share_id = self.get_query_argument("share_id", default=None)

        try:
            self.cs3.remove_public_share(share_id)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return

        self.set_status(204)

class SharedWithMeHandler(_CS3HandlerBase):
    @web.authenticated
    async def get(self):
        try:
            shares, _ = self.cs3.list_received_existing_shares()
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        shares_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in shares
        ]
        for s in shares_list:
            uid = s["received_share"]["share"]["creator"]["opaque_id"]
            if not uid:
                continue
            info = cache_get_user_info(self.cs3, user_id=uid, log=self.log)
            if info is not None:
                s["creator_user_info"] = info

        self.set_header("Content-Type", "application/json")
        self.write({"shares": shares_list})


class SharedByMeHandler(_CS3HandlerBase):
    """
    Handler for retrieving shares created by the user, both regular and public shares.
    """
    @web.authenticated
    async def get(self):
        try:
            shares, _ = self.cs3.list_existing_shares_by_creator(self.cs3.user_idp, self.cs3.user_opaque_id)
            public_shares, _ = self.cs3.list_existing_public_shares_by_creator(self.cs3.user_idp, self.cs3.user_opaque_id)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        shares_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in shares
        ]
        for s in shares_list:
            if s["share"]["grantee"]["type"] == "GRANTEE_TYPE_USER":
                uid = s["share"]["grantee"]["user_id"]["opaque_id"]
                info = cache_get_user_info(self.cs3, user_id=uid, log=self.log)
                if info is not None:
                    s["grantee_user_info"] = info

        public_shares_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in public_shares
        ]
        self.set_header("Content-Type", "application/json")
        self.write({"shares": shares_list, "public_shares": public_shares_list})


class SharedByResourceHandler(_CS3HandlerBase):
    """
    Handler for retrieving regular and public shares created by the user for a specific resource.
    query param path: path to the resource (REQUIRED).
    """
    @web.authenticated
    async def get(self):
        path = self.get_query_argument("path", default="")
        try:
            shares, _ = self.cs3.list_existing_shares_by_resource(path)
            public_shares, _ = self.cs3.list_existing_public_shares_by_resource(path)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        shares_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in shares
        ]

        for s in shares_list:
            if s["share"]["grantee"]["type"] == "GRANTEE_TYPE_USER":
                uid = s["share"]["grantee"]["user_id"]["opaque_id"]
                info = cache_get_user_info(self.cs3, user_id=uid, log=self.log)
                if info is not None:
                    s["grantee_user_info"] = info

        public_shares_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in public_shares
        ]
        self.set_header("Content-Type", "application/json")
        self.write({"shares": shares_list, "public_shares": public_shares_list})

class FindUsersHandler(_CS3HandlerBase):
    """
    Handler for finding users.
    :query search: The query string for TYPE_QUERY filter.
    :field user_type: The user type for TYPE_USER_TYPE filter. Supported types: USER_TYPE_PRIMARY,
        USER_TYPE_SECONDARY, USER_TYPE_SERVICE, USER_TYPE_GUEST, USER_TYPE_FEDERATED, USER_TYPE_LIGHTWEIGHT,
        USER_TYPE_SPACE_OWNER.
    """
    @web.authenticated
    async def get(self):
        search = self.get_query_argument("search", default="")
        user_type = self.get_query_argument("type", default=None)
        try:
            users = self.cs3.find_users(search, user_type=user_type)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        users_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in users
        ]
        self.set_header("Content-Type", "application/json")
        self.write({"search": search, "items": users_list})

class FindGroupsHandler(_CS3HandlerBase):
    """
    Handler for finding groups.
    :query search: The query string for TYPE_QUERY filter.
    """
    @web.authenticated
    async def get(self):
        search = self.get_query_argument("search", default="")
        try:
            # We don't use GROUP_TYPE_FEDERATED, all groups are regular groups.
            groups = self.cs3.find_groups(search, "GROUP_TYPE_REGULAR")
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        groups_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in groups
        ]
        self.set_header("Content-Type", "application/json")
        self.write({"search": search, "items": groups_list})

class GetQuotaHandler(_CS3HandlerBase):
    """
    Handler for retrieving quota information for the user.
    """
    @web.authenticated
    async def get(self):
        path = self.get_query_argument("path", default="")
        try:
            quota = self.cs3.get_quota(path)
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        quota_dict = MessageToDict(quota, preserving_proto_field_name=True)
        self.set_header("Content-Type", "application/json")
        self.write({"quota": quota_dict})

class GetSpaceHandler(_CS3HandlerBase):
    """
    Handler for retrieving space information for the user.
    """
    @web.authenticated
    async def get(self):
        try:
            spaces = self.cs3.list_spaces()
        except Exception as e:
            http_code = ErrorToHttpCode().map_exception_to_http_code(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        spaces_list = [
            MessageToDict(s, preserving_proto_field_name=True)
            for s in spaces
        ]
        self.set_header("Content-Type", "application/json")
        self.write({"spaces": spaces_list})


default_handlers = [
    (url_path_join("share", "share"), SharesHandler),
    (url_path_join("share", "link"), LinkHandler),
    (url_path_join("share", "getSharedByMe"), SharedByMeHandler),
    (url_path_join("share", "getSharedWithMe"), SharedWithMeHandler),
    (url_path_join("share", "getSharedByResource"), SharedByResourceHandler),
]
