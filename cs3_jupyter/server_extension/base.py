import functools
import json
from typing import Any, Callable, TypeVar

from anyio.to_thread import run_sync
from jupyter_server.base.handlers import APIHandler
from tornado import web

from ..cs3vfs.statuscodehandler import http_status_for

T = TypeVar("T")


class CS3CallError(web.HTTPError):
    """A failed CS3 call, carrying the HTTP status its exception maps to."""

    def __init__(self, status_code: int, message: str):
        # An argument, not the format string: messages carry paths, which may contain '%'.
        super().__init__(status_code, "%s", message)
        self.message = message


class CS3HandlerBase(APIHandler):
    """Base for handlers that talk to CS3 through the extension's own client.

    The client is created at extension load (see _load_jupyter_server_extension),
    so these endpoints work whatever contents manager the server runs.
    """

    @property
    def cs3(self):
        return self.settings["cs3_sharing_client"]

    async def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a blocking CS3 call off the event loop.

        A failure is raised as CS3CallError with the status the exception maps
        to, and written as {"error": ...} by write_error.
        """
        try:
            return await run_sync(functools.partial(fn, *args, **kwargs))
        except Exception as e:
            raise CS3CallError(http_status_for(e), str(e)) from e

    def write_error(self, status_code: int, **kwargs: Any) -> None:
        exc = kwargs.get("exc_info", (None, None, None))[1]
        if isinstance(exc, CS3CallError):
            # The shape the frontend reads (data.error).
            self.set_header("Content-Type", "application/json")
            self.finish(json.dumps({"error": exc.message}))
            return
        super().write_error(status_code, **kwargs)
