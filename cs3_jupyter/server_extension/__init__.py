# sharing/__init__.py
from .sharing import default_handlers
from jupyter_server.utils import url_path_join
from ..sharing_client import CS3SharingClient


def _load_jupyter_server_extension(serverapp):
    sharing_client = CS3SharingClient(parent=serverapp)
    # Make the sharing client available to the handlers
    serverapp.web_app.settings["cs3_sharing_client"] = sharing_client

    setup_handlers(serverapp.web_app)
    serverapp.log.info("sharing extension loaded")


def setup_handlers(web_app):
    base_url = web_app.settings["base_url"]
    host_pattern = ".*$"

    handlers = []
    for url, class_ in default_handlers:
        handlers.append((url_path_join(base_url, url), class_))

    web_app.add_handlers(host_pattern, handlers)
