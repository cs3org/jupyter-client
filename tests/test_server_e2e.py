"""Both contents managers behind a real jupyter server, driven through REST.

The offline stand-in for a smoke test against reva: the same requests the
frontend makes, end to end, against the in-memory fake backend. For the
hybrid manager the server's root_dir is the "FUSE mount", which never sees
what is written through CS3.
"""

import base64
import json

import nbformat
import pytest
from tornado.httpclient import HTTPClientError

from cs3_jupyter.cs3largefilemanager import CS3HybridLargeFileManager, CS3LargeFileManager

from conftest import DIR

pytest_plugins = ["pytest_jupyter.jupyter_server"]

MANAGERS = {"pure": CS3LargeFileManager, "hybrid": CS3HybridLargeFileManager}


@pytest.fixture(params=sorted(MANAGERS))
def manager_class(request):
    return MANAGERS[request.param]


@pytest.fixture(autouse=True)
def storage(patch_cs3, jp_root_dir):
    patch_cs3.files[str(jp_root_dir)] = DIR
    return patch_cs3


@pytest.fixture
def jp_server_config(manager_class, jp_root_dir):
    return {
        "ServerApp": {
            "contents_manager_class": manager_class,
            "jpserver_extensions": {"cs3_jupyter.server_extension": True},
        },
        "CS3Mixin": {"root_path": str(jp_root_dir)},
    }


async def api(jp_fetch, method, endpoint, body=None, **params):
    kwargs = {"method": method, "params": params}
    if body is not None:
        kwargs["body"] = json.dumps(body)
    elif method in ("POST", "PUT", "PATCH"):
        kwargs["body"] = ""
    r = await jp_fetch(*endpoint.split("/"), **kwargs)
    return json.loads(r.body) if r.body else None


async def test_document_lifecycle(jp_fetch, jp_root_dir, storage):
    root = str(jp_root_dir)
    nb = nbformat.v4.new_notebook()
    nb.cells.append(nbformat.v4.new_code_cell("1 + 1"))

    # create and read back a notebook
    await api(jp_fetch, "PUT", "api/contents/nb.ipynb", {"type": "notebook", "content": nb})
    model = await api(jp_fetch, "GET", "api/contents/nb.ipynb")
    assert model["content"]["cells"][0]["source"] == "1 + 1"
    assert model["writable"] is True

    # open it in the frontend: our lock, counted per session
    lock = await api(jp_fetch, "POST", "lock", path="nb.ipynb", session_id="s1")
    assert lock["locked"] is True and lock["count"] == 1

    # a new empty file, then rename, copy and delete
    await api(jp_fetch, "PUT", "api/contents/empty.txt", {"type": "file", "content": "", "format": "text"})
    assert storage.files[f"{root}/empty.txt"] == b""
    await api(jp_fetch, "PATCH", "api/contents/empty.txt", {"path": "renamed.txt"})
    copy = await api(jp_fetch, "POST", "api/contents", {"copy_from": "renamed.txt"})
    assert copy["name"] == "renamed-Copy1.txt"
    await jp_fetch("api", "contents", "renamed.txt", method="DELETE")

    # a chunked upload
    data = bytes(range(256)) * 4
    for chunk, part in ((1, data[:500]), (2, data[500:900]), (-1, data[900:])):
        content = base64.b64encode(part).decode()
        await api(jp_fetch, "PUT", "api/contents/blob.bin",
                  {"type": "file", "format": "base64", "content": content, "chunk": chunk})
    assert storage.files[f"{root}/blob.bin"] == data

    # the listing shows what CS3 has, mount or no mount
    listing = await api(jp_fetch, "GET", "api/contents")
    assert {e["name"] for e in listing["content"]} == {"nb.ipynb", "renamed-Copy1.txt", "blob.bin"}

    # close it in the frontend: the last session out releases the lock
    unlock = await api(jp_fetch, "DELETE", "lock", path="nb.ipynb", session_id="s1")
    assert unlock["unlocked"] is True
    assert f"{root}/nb.ipynb" not in storage.locks


async def test_a_file_someone_else_holds(jp_fetch, jp_root_dir, storage):
    path = f"{jp_root_dir}/theirs.txt"
    storage.put(path, b"theirs", lock={"app_name": "collabora", "lock_id": "x"})

    model = await api(jp_fetch, "GET", "api/contents/theirs.txt")
    assert model["writable"] is False

    lock = await api(jp_fetch, "POST", "lock", path="theirs.txt", session_id="s1")
    assert lock == {"locked": False, "read_only": True, "holder": "collabora", "path": "theirs.txt"}

    for method, body in (("PUT", {"type": "file", "content": "mine", "format": "text"}), ("DELETE", None)):
        with pytest.raises(HTTPClientError) as exc:
            await api(jp_fetch, method, "api/contents/theirs.txt", body)
        assert exc.value.code == 423
    assert storage.files[path] == b"theirs"
