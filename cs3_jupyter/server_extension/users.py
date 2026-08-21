from tornado import web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from google.protobuf.json_format import MessageToDict
from ..cs3vfs.statuscodehandler import ErrorToHttpCode


class FindUsersHandler(APIHandler):
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
        cm = self.contents_manager
        try:
            users = cm.find_users(search, user_type=user_type)
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


class FindGroupsHandler(APIHandler):
    """
    Handler for finding groups.
    :query search: The query string for TYPE_QUERY filter.
    """
    @web.authenticated
    async def get(self):
        search = self.get_query_argument("search", default="")
        cm = self.contents_manager
        try:
            # We don't use GROUP_TYPE_FEDERATED, all groups are regular groups.
            groups = cm.find_groups(search, "GROUP_TYPE_REGULAR")
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


default_handlers = [
    (url_path_join("find", "users"), FindUsersHandler),
    (url_path_join("find", "groups"), FindGroupsHandler),
]
