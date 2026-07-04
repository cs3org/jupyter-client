from anyio.to_thread import run_sync
from tornado import web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join

from ..cs3vfs.cs3lock import LOCK_FOREIGN, LOCK_NO_FILE
from ..cs3vfs.statuscodehandler import FileLockedError


class LockHandler(APIHandler):
    """
    Handler for managing the lock on a document.

    GET    /lock                          return server lock expiration
    POST   /lock?path=...&session_id=...  acquire/refresh a lock (document open + heartbeat)
    DELETE /lock?path=...&session_id=...  release the lock (document close)
    """
    async def _lock_args(self):
        path = self.get_query_argument("path", default="")
        session_id = self.get_query_argument("session_id", default="")
        if not path or not session_id:
            raise web.HTTPError(400, "Both 'path' and 'session_id' query arguments are required")
        # The tracker and all CS3 calls use the storage path as the canonical key.
        # _get_os_path can stat over gRPC, so keep it off the event loop.
        os_path = await run_sync(self.contents_manager._get_os_path, path.strip("/"))
        return path, os_path, session_id

    @web.authenticated
    async def get(self):
        cm = self.contents_manager
        self.set_status(200)
        self.write({"expiration": cm.lock_expiration})

    @web.authenticated
    async def post(self):
        cm = self.contents_manager
        path, os_path, session_id = await self._lock_args()
        async with cm._lock_mutex:
            state = await run_sync(cm.acquire_or_refresh_lock, os_path)
            if state == LOCK_NO_FILE:
                raise web.HTTPError(404, f"No such file: {path}")
            if state == LOCK_FOREIGN:
                holder = await run_sync(cm.foreign_lock_holder, os_path)
                self.set_status(200)
                self.write({"locked": False, "read_only": True, "path": path, "holder": holder})
                return
            count = cm.session_tracker.user_seen(os_path, session_id)
        cm.ensure_lock_refresher()
        self.set_status(200)
        self.write({"locked": True, "read_only": False, "path": path, "count": count})

    @web.authenticated
    async def delete(self):
        cm = self.contents_manager
        path, os_path, session_id = await self._lock_args()
        async with cm._lock_mutex:
            count = cm.session_tracker.user_left(os_path, session_id)
            if count is None:
                raise web.HTTPError(404, f"No tracked session {session_id} for {path}")
            if count == 0:
                try:
                    await run_sync(cm.unlock, os_path, cm.lock_holder, cm.lock_value)
                except FileLockedError:
                    # The lock is no longer ours (expired and taken over) - nothing to release.
                    raise web.HTTPError(409, f"Lock on {path} is held by another application")
                except FileNotFoundError:
                    pass
                self.set_status(200)
                self.write({"unlocked": True, "path": path, "count": 0})
            else:
                self.set_status(200)
                self.write({"unlocked": False, "path": path, "count": count})


default_handlers = [
    (url_path_join("lock"), LockHandler),
]
