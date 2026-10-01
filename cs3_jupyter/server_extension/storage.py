from tornado import web
from jupyter_server.utils import url_path_join
from .base import CS3HandlerBase
from .utils import proto_dict, proto_dicts


class GetQuotaHandler(CS3HandlerBase):
    """
    Handler for retrieving quota information for the user.
    """
    @web.authenticated
    async def get(self):
        path = self.get_query_argument("path", default="")
        quota = await self.call(self.cs3.spaces.get_quota, path)
        self.write({"quota": proto_dict(quota)})


class GetSpaceHandler(CS3HandlerBase):
    """
    Handler for retrieving space information for the user.
    """
    @web.authenticated
    async def get(self):
        spaces = await self.call(self.cs3.spaces.list_spaces)
        self.write({"spaces": proto_dicts(spaces)})


default_handlers = [
    (url_path_join("quota"), GetQuotaHandler),
    (url_path_join("space", "list"), GetSpaceHandler),
]
