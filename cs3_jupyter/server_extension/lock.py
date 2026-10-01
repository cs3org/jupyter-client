from anyio.to_thread import run_sync
from tornado import web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join

from ..cs3vfs.statuscodehandler import FileLockedError


class LockHandler(APIHandler):
    """
    Handler for managing the lock on a document.

    GET    /lock                          return server lock expiration
    POST   /lock?path=...&session_id=...  acquire/refresh a lock (document open + heartbeat)
    DELETE /lock?path=...&session_id=...  release the lock (document close)

    The locks belong to the contents manager's DocumentLocks: saves take the
    same locks, so this endpoint only works with a CS3 contents manager.
    """

    @property
    def document_locks(self):
        return self.contents_manager.document_locks

    async def _lock_args(self):
        path = self.get_query_argument("path", default="")
        session_id = self.get_query_argument("session_id", default="")
        if not path or not session_id:
            raise web.HTTPError(400, "Both 'path' and 'session_id' query arguments are required")
        # The tracker and all CS3 calls use the storage path as the canonical key.
        # _get_os_path can touch the storage, so keep it off the event loop.
        os_path = await run_sync(self.contents_manager._get_os_path, path.strip("/"))
        return path, os_path, session_id

    @web.authenticated
    async def get(self):
        self.write({"expiration": self.document_locks.expiration})

    @web.authenticated
    async def post(self):
        path, os_path, session_id = await self._lock_args()
        try:
            result = await self.document_locks.open(os_path, session_id)
        except FileNotFoundError:
            raise web.HTTPError(404, f"No such file: {path}") from None
        self.write({**result, "path": path})

    @web.authenticated
    async def delete(self):
        path, os_path, session_id = await self._lock_args()
        try:
            result = await self.document_locks.close(os_path, session_id)
        except FileLockedError as e:
            # The lock is no longer ours (expired and taken over) - nothing to release.
            raise web.HTTPError(409, f"Lock on {path} is held by another application") from e
        if result is None:
            raise web.HTTPError(404, f"No tracked session {session_id} for {path}")
        self.write({**result, "path": path})


default_handlers = [
    (url_path_join("lock"), LockHandler),
]
