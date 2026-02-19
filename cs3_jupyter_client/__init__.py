def _jupyter_server_extension_points():
    return [{
        "module": "cs3_jupyter_client.server_extension"
    }]


def _jupyter_labextension_paths():
    return [{
        "src": "labextension",
        "dest": "@cs3org/cs3-jupyter-client",
    }]
