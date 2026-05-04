from tornado import web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join


class LockHandler(APIHandler):
    """
    Handler for managing the lock on a document.

    GET    /lock                          return server lock expiration
    POST   /lock?path=...&session_id=...  acquire a lock
    DELETE /lock?path=...&session_id=...  release the lock
    """
    @web.authenticated
    async def get(self):
        cm = self.contents_manager
        self.set_status(200)
        self.write({"expiration": cm.lock_expiration})

    @web.authenticated
    async def post(self):
        cm = self.contents_manager
        path = self.get_query_argument("path", default="")
        session_id = self.get_query_argument("session_id", default="")
        async with cm._lock_mutex:
            acquired = cm.acquire_or_refresh_lock(path)
            count = cm.session_tracker.user_seen(path, session_id) if acquired else 0
        self.set_status(200)
        if not acquired:
            self.write({"locked": False, "read_only": True, "path": path})
        else:
            self.write({"locked": True, "read_only": False, "path": path, "count": count})

    @web.authenticated
    async def delete(self):
        cm = self.contents_manager
        path = self.get_query_argument("path", default="")
        session_id = self.get_query_argument("session_id", default="")
        async with cm._lock_mutex:
            count = cm.session_tracker.user_left(path, session_id)
            if count == 0:
                try:
                    cm.unlock(path, cm.lock_app_name, cm.lock_value)
                    self.set_status(200)
                    self.write({"unlocked": True, "path": path, "count": 0})
                except Exception as e:
                    self.set_status(500)
                    self.write({"error": str(e), "path": path, "count": 0})
            else:
                self.set_status(200)
                self.write({"unlocked": False, "path": path, "count": count})


default_handlers = [
    (url_path_join("lock"), LockHandler),
]
