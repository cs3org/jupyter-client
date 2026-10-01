from tornado import web
from jupyter_server.utils import url_path_join
from google.protobuf.json_format import MessageToDict
from ..cs3vfs.statuscodehandler import http_status_for
from .base import CS3HandlerBase


class GetQuotaHandler(CS3HandlerBase):
    """
    Handler for retrieving quota information for the user.
    """
    @web.authenticated
    async def get(self):
        path = self.get_query_argument("path", default="")
        try:
            quota = self.cs3.get_quota(path)
        except Exception as e:
            http_code = http_status_for(e)
            self.set_status(http_code)
            self.write({"error": str(e)})
            return
        quota_dict = MessageToDict(quota, preserving_proto_field_name=True)
        self.set_header("Content-Type", "application/json")
        self.write({"quota": quota_dict})


class GetSpaceHandler(CS3HandlerBase):
    """
    Handler for retrieving space information for the user.
    """
    @web.authenticated
    async def get(self):
        try:
            spaces = self.cs3.list_spaces()
        except Exception as e:
            http_code = http_status_for(e)
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
    (url_path_join("quota"), GetQuotaHandler),
    (url_path_join("space", "list"), GetSpaceHandler),
]
