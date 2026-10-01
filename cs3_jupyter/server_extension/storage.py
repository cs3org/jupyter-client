from tornado import web
from jupyter_server.utils import url_path_join
from .base import CS3HandlerBase
from .utils import proto_dict, proto_dicts


class GetMyQuotaHandler(CS3HandlerBase):
    """
    Handler for retrieving the current user's quota.
    """
    @web.authenticated
    async def get(self):
        path = self.cs3.storage_path_from_user(self.cs3.client_id)
        quota = await self.call(self.cs3.get_quota, path)
        self.write({"quota": proto_dict(quota)})


class GetSpaceHandler(CS3HandlerBase):
    """
    Handler for retrieving space information for the user.
    """
    @web.authenticated
    async def get(self):
        spaces = await self.call(self.cs3.list_spaces)
        self.write({"spaces": proto_dicts(spaces)})


default_handlers = [
    (url_path_join("quota", "me"), GetMyQuotaHandler),
    (url_path_join("spaces"), GetSpaceHandler),
]
