from jupyter_server.utils import url_path_join

from .sharing import default_handlers as sharing_handlers
from .lock import default_handlers as lock_handlers
from .users import default_handlers as users_handlers
from .storage import default_handlers as storage_handlers


default_handlers = [
    *sharing_handlers,
    *lock_handlers,
    *users_handlers,
    *storage_handlers,
]


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
