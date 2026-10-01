"""Lock endpoint semantics, exercised through a real jupyter server."""

import json

import pytest
from tornado.httpclient import HTTPClientError

from jupyter_server.services.contents.largefilemanager import AsyncLargeFileManager

from cs3_jupyter.documentlocks import DocumentLocks

from conftest import FakeLocks

pytest_plugins = ["pytest_jupyter.jupyter_server"]


class FakeLockCM(AsyncLargeFileManager):
    """Default contents manager with the one attribute the handler needs."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.locks = FakeLocks()
        self.document_locks = DocumentLocks(self.locks, expiration=300)


@pytest.fixture(autouse=True)
def offline_cs3(patch_cs3):
    """The extension builds its own CS3 client at load: keep it off the network."""


@pytest.fixture
def jp_server_config():
    return {
        "ServerApp": {
            "contents_manager_class": FakeLockCM,
            "jpserver_extensions": {"cs3_jupyter.server_extension": True},
        }
    }


async def test_get_returns_expiration(jp_fetch):
    r = await jp_fetch("lock")
    assert json.loads(r.body) == {"expiration": 300}


async def test_post_requires_path_and_session(jp_fetch):
    with pytest.raises(HTTPClientError) as exc:
        await jp_fetch("lock", method="POST", body="", params={"path": "f.txt"})
    assert exc.value.code == 400


async def test_post_acquires_and_counts_sessions(jp_fetch, jp_serverapp):
    r = await jp_fetch("lock", method="POST", body="",
                       params={"path": "f.txt", "session_id": "s1"})
    assert json.loads(r.body)["locked"] is True
    assert json.loads(r.body)["count"] == 1

    r = await jp_fetch("lock", method="POST", body="",
                       params={"path": "f.txt", "session_id": "s2"})
    assert json.loads(r.body)["count"] == 2
    assert jp_serverapp.contents_manager.document_locks._refresher is not None


async def test_post_foreign_reports_read_only_with_holder(jp_fetch, jp_serverapp):
    jp_serverapp.contents_manager.locks.state = "foreign"
    r = await jp_fetch("lock", method="POST", body="",
                       params={"path": "f.txt", "session_id": "s1"})
    body = json.loads(r.body)
    assert body["locked"] is False
    assert body["read_only"] is True
    assert body["holder"] == "collabora"


async def test_post_missing_file_404(jp_fetch, jp_serverapp):
    jp_serverapp.contents_manager.locks.state = "no_file"
    with pytest.raises(HTTPClientError) as exc:
        await jp_fetch("lock", method="POST", body="",
                       params={"path": "nope.txt", "session_id": "s1"})
    assert exc.value.code == 404


async def test_delete_unknown_session_404_and_no_unlock(jp_fetch, jp_serverapp):
    cm = jp_serverapp.contents_manager
    await jp_fetch("lock", method="POST", body="",
                   params={"path": "f.txt", "session_id": "s1"})
    with pytest.raises(HTTPClientError) as exc:
        await jp_fetch("lock", method="DELETE",
                       params={"path": "f.txt", "session_id": "bogus"})
    assert exc.value.code == 404
    assert cm.locks.unlocked == []


async def test_delete_last_session_unlocks(jp_fetch, jp_serverapp):
    cm = jp_serverapp.contents_manager
    await jp_fetch("lock", method="POST", body="",
                   params={"path": "f.txt", "session_id": "s1"})
    await jp_fetch("lock", method="POST", body="",
                   params={"path": "f.txt", "session_id": "s2"})

    r = await jp_fetch("lock", method="DELETE",
                       params={"path": "f.txt", "session_id": "s1"})
    assert json.loads(r.body)["unlocked"] is False
    assert cm.locks.unlocked == []

    r = await jp_fetch("lock", method="DELETE",
                       params={"path": "f.txt", "session_id": "s2"})
    assert json.loads(r.body)["unlocked"] is True
    assert cm.locks.unlocked == [cm._get_os_path("f.txt")]  # the storage path, not the API path
