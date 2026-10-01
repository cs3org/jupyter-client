"""DocumentLocks: session counting, and the background refresher that keeps
live sessions' locks fresh and releases stale ones."""

import asyncio
import time

import pytest

from cs3_jupyter.cs3api.errors import FileLockedError
from cs3_jupyter.documentlocks import DocumentLocks

from conftest import FakeLocks


@pytest.fixture
def locks():
    return FakeLocks()


def test_open_counts_sessions_and_close_unlocks_after_the_last(locks):
    async def run():
        docs = DocumentLocks(locks, expiration=300)
        assert await docs.open("/f", "s1") == {"locked": True, "read_only": False, "count": 1}
        assert (await docs.open("/f", "s2"))["count"] == 2
        assert await docs.close("/f", "s1") == {"unlocked": False, "count": 1}
        assert locks.unlocked == []
        assert await docs.close("/f", "s2") == {"unlocked": True, "count": 0}
        assert locks.unlocked == ["/f"]
        docs._refresher.cancel()

    asyncio.run(run())


def test_open_of_a_foreign_locked_file_is_read_only_and_untracked(locks):
    locks.state = "foreign"

    async def run():
        docs = DocumentLocks(locks, expiration=300)
        result = await docs.open("/f", "s1")
        assert result == {"locked": False, "read_only": True, "holder": "collabora"}
        assert docs._refresher is None
        # never counted, so closing it cannot unlock anything
        assert await docs.close("/f", "s1") is None

    asyncio.run(run())


def test_open_of_a_missing_file_raises(locks):
    locks.state = "no_file"
    with pytest.raises(FileNotFoundError):
        asyncio.run(DocumentLocks(locks, expiration=300).open("/nope", "s1"))


def test_close_of_a_lock_taken_over_raises(locks):
    locks.unlock_error = FileLockedError("not the holder")

    async def run():
        docs = DocumentLocks(locks, expiration=300)
        await docs.open("/f", "s1")
        docs._refresher.cancel()
        with pytest.raises(FileLockedError):
            await docs.close("/f", "s1")

    asyncio.run(run())


def test_refresh_loop_unlocks_stale_and_exits(monkeypatch, locks):
    async def instant_sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)

    docs = DocumentLocks(locks, expiration=30)
    docs.sessions._timeout = 0.01
    docs.sessions.user_seen("/stale", "s1")
    time.sleep(0.02)  # let the only session go stale

    # iteration 1: sweep finds /stale expired -> unlock; iteration 2: idle -> exit
    asyncio.run(docs._refresh_loop())
    assert locks.unlocked == ["/stale"]
    assert locks.refreshed == []


def test_refresh_loop_refreshes_live_sessions(monkeypatch, locks):
    sleeps = 0

    async def counting_sleep(_):
        nonlocal sleeps
        sleeps += 1
        if sleeps >= 3:
            raise asyncio.CancelledError  # stop the otherwise-endless loop

    monkeypatch.setattr(asyncio, "sleep", counting_sleep)

    docs = DocumentLocks(locks, expiration=60)
    docs.sessions.user_seen("/live", "s1")

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(docs._refresh_loop())
    assert locks.refreshed == ["/live", "/live"]
    assert locks.unlocked == []
