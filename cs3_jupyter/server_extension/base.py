from jupyter_server.base.handlers import APIHandler


class CS3HandlerBase(APIHandler):
    """Base for handlers that talk to CS3 through the extension's own client.

    The client is created at extension load (see _load_jupyter_server_extension),
    so these endpoints work whatever contents manager the server runs.
    """

    @property
    def cs3(self):
        return self.settings["cs3_sharing_client"]
