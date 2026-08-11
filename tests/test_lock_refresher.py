"""Background refresher: keeps live sessions' locks fresh, releases stale ones."""

import asyncio
import logging
import time

import pytest

from cs3_jupyter.cs3mixin import CS3Mixin
from cs3_jupyter.sessiontracker import SessionTracker


class Stub:
    lock_expiration = 30
    lock_holder = "jupyter-rtc-test"
    lock_value = "jupyter_rtc_lock"
    _lock_mutex = asyncio.Lock()
    log = logging.getLogger("test")

    def __init__(self):
        self.session_tracker = SessionTracker(heartbeat_timeout_seconds=0.01)
        self.unlocked = []
        self.refreshed = []

    def unlock(self, path, app_name, lock_id):
        self.unlocked.append(path)

    def refresh_lock(self, path, app_name, lock_id, existing_lock_id):
        self.refreshed.append(path)


def test_refresh_loop_unlocks_stale_and_exits(monkeypatch):
    async def instant_sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)

    stub = Stub()
    stub.session_tracker.user_seen("/stale", "s1")
    time.sleep(0.02)  # let the only session go stale

    async def run():
        # iteration 1: sweep finds /stale expired -> unlock; iteration 2: idle -> exit
        await CS3Mixin._lock_refresh_loop(stub)

    asyncio.run(run())
    assert stub.unlocked == ["/stale"]
    assert stub.refreshed == []


def test_refresh_loop_refreshes_live_sessions(monkeypatch):
    sleeps = 0

    async def counting_sleep(_):
        nonlocal sleeps
        sleeps += 1
        if sleeps >= 3:
            raise asyncio.CancelledError  # stop the otherwise-endless loop

    monkeypatch.setattr(asyncio, "sleep", counting_sleep)

    stub = Stub()
    stub.session_tracker._timeout = 60  # keep the session live
    stub.session_tracker.user_seen("/live", "s1")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(CS3Mixin._lock_refresh_loop(stub))
    assert stub.refreshed == ["/live", "/live"]
    assert stub.unlocked == []
