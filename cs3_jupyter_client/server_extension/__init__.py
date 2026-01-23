# sharing/__init__.py
from .sharing import default_handlers
from jupyter_server.utils import url_path_join


def _jupyter_server_extension_points():
    return [{"module": "cs3_jupyter_client.server_extension"}]

def _load_jupyter_server_extension(serverapp):
    # Called when the extension loads; attach handlers here.
    setup_handlers(serverapp.web_app)
    serverapp.log.info("sharing extension loaded")


def setup_handlers(web_app):
    base_url = web_app.settings["base_url"]
    host_pattern = ".*$"

    handlers = []
    for url, class_ in default_handlers:
        handlers.append((url_path_join(base_url, url), class_))

    web_app.add_handlers(host_pattern, handlers)
