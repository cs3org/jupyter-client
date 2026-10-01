from tornado import web
from jupyter_server.utils import url_path_join
from .base import CS3HandlerBase
from .utils import proto_dicts


class FindUsersHandler(CS3HandlerBase):
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
        users = await self.call(self.cs3.users.find_users, search, user_type=user_type)
        self.write({"search": search, "items": proto_dicts(users)})


class FindGroupsHandler(CS3HandlerBase):
    """
    Handler for finding groups.
    :query search: The query string for TYPE_QUERY filter.
    """
    @web.authenticated
    async def get(self):
        search = self.get_query_argument("search", default="")
        # We don't use GROUP_TYPE_FEDERATED, all groups are regular groups.
        groups = await self.call(self.cs3.groups.find_groups, search, "GROUP_TYPE_REGULAR")
        self.write({"search": search, "items": proto_dicts(groups)})


default_handlers = [
    (url_path_join("find", "users"), FindUsersHandler),
    (url_path_join("find", "groups"), FindGroupsHandler),
]
