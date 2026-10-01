from tornado import web
from jupyter_server.utils import url_path_join
from .base import CS3HandlerBase
from .utils import cache_get_user_info, proto_dict, proto_dicts


def _with_user_info(cs3, shares, key, user_id_of, log):
    """Share dicts, each with its user's info under `key` when there is one.

    Runs off the event loop: every user not cached yet costs a GetUser call.
    """
    share_dicts = proto_dicts(shares)
    for s in share_dicts:
        user_id = user_id_of(s)
        if not user_id:
            continue
        info = cache_get_user_info(cs3, user_id=user_id, log=log)
        if info is not None:
            s[key] = info
    return share_dicts


def _grantee_user_id(share):
    grantee = share["share"]["grantee"]
    if grantee["type"] == "GRANTEE_TYPE_USER":
        return grantee["user_id"]["opaque_id"]
    return None


def _creator_user_id(received):
    return received["received_share"]["share"]["creator"]["opaque_id"]


class SharesHandler(CS3HandlerBase):

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
        path = self.get_query_argument("path", default="")
        body = self.get_json_body() or {}
        opaque_id = body.get("opaque_id", "")
        idp = body.get("idp", "")
        role = body.get("role", "")
        grantee_type = body.get("grantee_type", "USER")

        self.log.info(f"Creating share for path: {path} to {grantee_type} {opaque_id} with role {role}")
        share = await self.call(self.cs3.create_share, path, opaque_id, idp, role, grantee_type)
        self.set_status(201)
        self.write({"created": True, "data": body, "path": path, "share": proto_dict(share)})

    @web.authenticated
    async def put(self):
        """
        Update a share for a resource.
        :query param share_id: The ID of the share to update (REQUIRED).
        :field role: Role to update the share, VIEWER or EDITOR (REQUIRED).
        :field display_name: new display name.
        """
        share_id = self.get_query_argument("share_id", default=None)
        body = self.get_json_body() or {}
        role = body.get("role", None)
        display_name = body.get("display_name", None)

        self.log.info(f"Updating share: {share_id} with role {role} and display name {display_name}")
        share = await self.call(self.cs3.update_share, share_id, role=role, display_name=display_name)
        self.write({"updated": True, "data": body, "share": proto_dict(share)})

    @web.authenticated
    async def delete(self):
        """
        Remove a share for a resource.
        :field share_id: The ID of the share to remove (REQUIRED).
        """
        share_id = self.get_query_argument("share_id", default=None)
        await self.call(self.cs3.remove_share, share_id)
        self.set_status(204)


class LinkHandler(CS3HandlerBase):

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
        path = self.get_query_argument("path", default="")
        body = self.get_json_body() or {}
        role = body.get("role", "")

        self.log.info(f"Creating public share for path: {path} with role {role}")
        share = await self.call(
            self.cs3.create_public_share,
            path,
            role,
            password=body.get("password", None),
            expiration=body.get("expiration", None),
            description=body.get("description", None),
            internal=body.get("internal", False),
            notify_uploads=body.get("notify_uploads", False),
            notify_uploads_extra_recipients=body.get("notify_uploads_extra_recipients", None),
        )
        self.set_status(201)
        self.write({"created": True, "data": body, "path": path, "share": proto_dict(share)})

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
        share_id = self.get_query_argument("share_id", default="")
        body = self.get_json_body() or {}
        type = body.get("type", "")
        role = body.get("role", "")

        self.log.info(f"Updating public share: {share_id} with type {type} role {role}")
        share = await self.call(
            self.cs3.update_public_share,
            share_id,
            type=type,
            role=role,
            password=body.get("password", None),
            expiration=body.get("expiration", None),
            description=body.get("description", None),
            notify_uploads=body.get("notify_uploads", False),
            display_name=body.get("display_name", None),
            notify_uploads_extra_recipients=body.get("notify_uploads_extra_recipients", None),
        )
        self.write({"updated": True, "data": body, "share": proto_dict(share)})

    @web.authenticated
    async def delete(self):
        """
        Remove a share for a resource.
        :field share_id: The ID of the share to remove (REQUIRED).
        """
        share_id = self.get_query_argument("share_id", default=None)
        await self.call(self.cs3.remove_public_share, share_id)
        self.set_status(204)


class SharedWithMeHandler(CS3HandlerBase):
    @web.authenticated
    async def get(self):
        def fetch():
            shares, _ = self.cs3.list_received_existing_shares()
            return _with_user_info(self.cs3, shares, "creator_user_info", _creator_user_id, self.log)

        self.write({"shares": await self.call(fetch)})


class SharedByMeHandler(CS3HandlerBase):
    """
    Handler for retrieving shares created by the user, both regular and public shares.
    """
    @web.authenticated
    async def get(self):
        def fetch():
            idp, opaque_id = self.cs3.user_idp, self.cs3.user_opaque_id
            shares, _ = self.cs3.list_existing_shares_by_creator(idp, opaque_id)
            public_shares, _ = self.cs3.list_existing_public_shares_by_creator(idp, opaque_id)
            return (
                _with_user_info(self.cs3, shares, "grantee_user_info", _grantee_user_id, self.log),
                proto_dicts(public_shares),
            )

        shares, public_shares = await self.call(fetch)
        self.write({"shares": shares, "public_shares": public_shares})


class SharedByResourceHandler(CS3HandlerBase):
    """
    Handler for retrieving regular and public shares created by the user for a specific resource.
    query param path: path to the resource (REQUIRED).
    """
    @web.authenticated
    async def get(self):
        path = self.get_query_argument("path", default="")

        def fetch():
            shares, _ = self.cs3.list_existing_shares_by_resource(path)
            public_shares, _ = self.cs3.list_existing_public_shares_by_resource(path)
            return (
                _with_user_info(self.cs3, shares, "grantee_user_info", _grantee_user_id, self.log),
                proto_dicts(public_shares),
            )

        shares, public_shares = await self.call(fetch)
        self.write({"shares": shares, "public_shares": public_shares})


default_handlers = [
    (url_path_join("share", "share"), SharesHandler),
    (url_path_join("share", "link"), LinkHandler),
    (url_path_join("share", "getSharedByMe"), SharedByMeHandler),
    (url_path_join("share", "getSharedWithMe"), SharedWithMeHandler),
    (url_path_join("share", "getSharedByResource"), SharedByResourceHandler),
]
