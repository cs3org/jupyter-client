import logging

import pytest
from tornado.web import HTTPError

from cs3_jupyter_client.cs3vfs.cs3lock import (
    CS3Lock,
    LOCK_FOREIGN,
    LOCK_HELD,
    LOCK_NO_FILE,
)
from cs3_jupyter_client.cs3vfs.statuscodehandler import StatusCodeHandler

from conftest import FakeAuth, FakeCS3


class LockOwner(CS3Lock):
    """Minimal object exercising CS3Lock against the fake backend."""

    def __init__(self, fake, holder):
        self.client = type("C", (), {"file": fake})()
        self.auth = FakeAuth()
        self.status_handler = StatusCodeHandler()
        self.lock_holder = holder
        self.lock_value = "jupyter_rtc_lock"
        self.log = logging.getLogger("test")

    def _refresh_auth(self):
        pass


@pytest.fixture
def fake():
    return FakeCS3()


@pytest.fixture
def owner(fake):
    return LockOwner(fake, "jupyter-rtc-rwelande")


def test_acquire_unlocked_file(fake, owner):
    fake.put("/f")
    assert owner.acquire_or_refresh_lock("/f") == LOCK_HELD
    assert fake.locks["/f"]["app_name"] == "jupyter-rtc-rwelande"


def test_acquire_already_ours_refreshes(fake, owner):
    fake.put("/f", lock={"app_name": "jupyter-rtc-rwelande", "lock_id": "jupyter_rtc_lock"})
    assert owner.acquire_or_refresh_lock("/f") == LOCK_HELD


def test_acquire_foreign(fake, owner):
    fake.put("/f", lock={"app_name": "collabora", "lock_id": "x"})
    assert owner.acquire_or_refresh_lock("/f") == LOCK_FOREIGN


def test_acquire_missing_file(owner):
    assert owner.acquire_or_refresh_lock("/nope") == LOCK_NO_FILE


def test_ensure_write_lock_raises_423_with_holder(fake, owner):
    fake.put("/f", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        owner.ensure_write_lock("/f")
    assert exc.value.status_code == 423
    assert "collabora" in str(exc.value)


def test_ensure_write_lock_passes_through_no_file(owner):
    assert owner.ensure_write_lock("/nope") == LOCK_NO_FILE


def test_lock_after_create_best_effort(fake, owner):
    fake.put("/f")
    owner.lock_after_create("/f")
    assert fake.locks["/f"]["app_name"] == "jupyter-rtc-rwelande"
    # losing the race only warns
    fake.locks["/f"] = {"app_name": "collabora", "lock_id": "x"}
    owner.lock_after_create("/f")
    assert fake.locks["/f"]["app_name"] == "collabora"


def test_foreign_lock_holder_exact_match(fake):
    """The shared base name must not be confused with a per-user holder."""
    fake.put("/f", lock={"app_name": "jupyter-rtc", "lock_id": "jupyter_rtc_lock"})
    per_user = LockOwner(fake, "jupyter-rtc-rwelande")
    shared = LockOwner(fake, "jupyter-rtc")
    # substring matching would wrongly claim this lock as ours
    assert per_user.foreign_lock_holder("/f") == "jupyter-rtc"
    assert shared.foreign_lock_holder("/f") is None
    assert per_user.foreign_lock_holder("/unlocked-or-missing") is None


def test_shared_holder_migration_makes_servers_coholders(fake):
    """With the per-user suffix off, two servers can use each other's locks."""
    a = LockOwner(fake, "jupyter-rtc")
    b = LockOwner(fake, "jupyter-rtc")
    fake.put("/f")
    assert a.acquire_or_refresh_lock("/f") == LOCK_HELD
    assert b.acquire_or_refresh_lock("/f") == LOCK_HELD  # refresh, same holder
    b.unlock("/f", b.lock_holder, b.lock_value)
    assert "/f" not in fake.locks


def test_lock_holder_property_suffix():
    from cs3_jupyter_client.cs3mixin import CS3Mixin

    holder = CS3Mixin.lock_holder.fget

    class Stub:
        lock_app_name = "jupyter-rtc"
        client_id = "rwelande"
        lock_holder_suffix_client_id = True

    s = Stub()
    assert holder(s) == "jupyter-rtc-rwelande"
    s.lock_holder_suffix_client_id = False
    assert holder(s) == "jupyter-rtc"
    # no client_id configured -> no dangling separator
    s.lock_holder_suffix_client_id = True
    s.client_id = ""
    assert holder(s) == "jupyter-rtc"
