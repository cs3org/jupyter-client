"""Lock endpoint semantics, exercised through a real jupyter server."""

import asyncio
import json

import pytest
from tornado.httpclient import HTTPClientError

from jupyter_server.services.contents.largefilemanager import AsyncLargeFileManager

from cs3_jupyter_client.sessiontracker import SessionTracker

pytest_plugins = ["pytest_jupyter.jupyter_server"]


class FakeLockCM(AsyncLargeFileManager):
    """Default contents manager with the lock surface the handler needs."""

    lock_expiration = 300
    lock_value = "jupyter_rtc_lock"
    lock_holder = "jupyter-rtc-test"
    _lock_mutex = asyncio.Lock()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session_tracker = SessionTracker(heartbeat_timeout_seconds=300)
        self.acquire_result = "held"
        self.unlock_calls = []
        self.refresher_started = False

    def acquire_or_refresh_lock(self, path):
        return self.acquire_result

    def foreign_lock_holder(self, path):
        return "collabora"

    def unlock(self, path, app_name, lock_id):
        self.unlock_calls.append((path, app_name, lock_id))

    def ensure_lock_refresher(self):
        self.refresher_started = True


@pytest.fixture
def jp_server_config():
    return {
        "ServerApp": {
            "contents_manager_class": FakeLockCM,
            "jpserver_extensions": {"cs3_jupyter_client.server_extension": True},
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
    assert jp_serverapp.contents_manager.refresher_started is True


async def test_post_foreign_reports_read_only_with_holder(jp_fetch, jp_serverapp):
    jp_serverapp.contents_manager.acquire_result = "foreign"
    r = await jp_fetch("lock", method="POST", body="",
                       params={"path": "f.txt", "session_id": "s1"})
    body = json.loads(r.body)
    assert body["locked"] is False
    assert body["read_only"] is True
    assert body["holder"] == "collabora"


async def test_post_missing_file_404(jp_fetch, jp_serverapp):
    jp_serverapp.contents_manager.acquire_result = "no_file"
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
    assert cm.unlock_calls == []


async def test_delete_last_session_unlocks(jp_fetch, jp_serverapp):
    cm = jp_serverapp.contents_manager
    await jp_fetch("lock", method="POST", body="",
                   params={"path": "f.txt", "session_id": "s1"})
    await jp_fetch("lock", method="POST", body="",
                   params={"path": "f.txt", "session_id": "s2"})

    r = await jp_fetch("lock", method="DELETE",
                       params={"path": "f.txt", "session_id": "s1"})
    assert json.loads(r.body)["unlocked"] is False
    assert cm.unlock_calls == []

    r = await jp_fetch("lock", method="DELETE",
                       params={"path": "f.txt", "session_id": "s2"})
    assert json.loads(r.body)["unlocked"] is True
    assert len(cm.unlock_calls) == 1
    assert cm.unlock_calls[0][1] == "jupyter-rtc-test"
