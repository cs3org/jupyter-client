import logging
from types import SimpleNamespace

import pytest

from cs3_jupyter.cs3vfs.cs3lock import (
    CS3Locks,
    LOCK_FOREIGN,
    LOCK_HELD,
    LOCK_NO_FILE,
)

from cs3_jupyter.cs3vfs.statuscodehandler import FileLockedError

from conftest import FakeAuth, FakeCS3


class FakeSession:
    """A CS3Session over the fake backend, with no token file behind it."""

    def __init__(self, fake):
        self.client = SimpleNamespace(file=fake)
        self.auth = FakeAuth()
        self.log = logging.getLogger("test")

    def refresh_auth(self):
        pass


def locks_for(fake, holder):
    """CS3Locks taken as `holder`, against the fake backend."""
    storage = SimpleNamespace(exists=lambda path: path in fake.files)
    return CS3Locks(FakeSession(fake), storage, holder, "jupyter_rtc_lock")


@pytest.fixture
def fake():
    return FakeCS3()


@pytest.fixture
def owner(fake):
    return locks_for(fake, "jupyter-rtc-rwelande")


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


@pytest.mark.parametrize("missing_file_error", ["unknown", "not_found"])
def test_acquire_missing_file(fake, owner, missing_file_error):
    """Regression: EOS reports a missing file as a generic internal error.

    Creating a new notebook 500'd because that escaped as an unhandled
    OSError instead of being reported as "the file is not there yet".
    """
    fake.missing_file_error = missing_file_error
    assert owner.acquire_or_refresh_lock("/nope") == LOCK_NO_FILE


def test_acquire_existing_unlocked_file(fake, owner):
    """RefreshLock on an unlocked file fails; SetLock must still win it."""
    fake.put("/f")
    assert "/f" not in fake.locks
    assert owner.acquire_or_refresh_lock("/f") == LOCK_HELD


def test_acquire_reraises_real_storage_errors(fake, owner, monkeypatch):
    """An internal error on an existing file is a genuine failure, not NO_FILE."""
    fake.put("/f")

    def boom(*args, **kwargs):
        raise OSError("storage on fire")

    monkeypatch.setattr(owner, "set_lock", boom)
    with pytest.raises(OSError, match="storage on fire"):
        owner.acquire_or_refresh_lock("/f")


def test_ensure_write_lock_raises_with_holder(fake, owner):
    fake.put("/f", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(FileLockedError) as exc:
        owner.ensure_write_lock("/f")
    assert exc.value.holder == "collabora"
    assert "collabora" in str(exc.value)


def test_ensure_write_lock_passes_through_no_file(owner):
    assert owner.ensure_write_lock("/nope") == LOCK_NO_FILE


def test_foreign_lock_holder_survives_unreadable_lock_state(fake, owner, monkeypatch):
    """An unreadable lock must not fail the read it was only annotating."""
    fake.put("/f")

    def boom(*args, **kwargs):
        raise OSError("storage on fire")

    monkeypatch.setattr(owner, "get_lock", boom)
    assert owner.foreign_lock_holder("/f") is None


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
    per_user = locks_for(fake, "jupyter-rtc-rwelande")
    shared = locks_for(fake, "jupyter-rtc")
    # substring matching would wrongly claim this lock as ours
    assert per_user.foreign_lock_holder("/f") == "jupyter-rtc"
    assert shared.foreign_lock_holder("/f") is None
    assert per_user.foreign_lock_holder("/unlocked-or-missing") is None


def test_shared_holder_migration_makes_servers_coholders(fake):
    """With the per-user suffix off, two servers can use each other's locks."""
    a = locks_for(fake, "jupyter-rtc")
    b = locks_for(fake, "jupyter-rtc")
    fake.put("/f")
    assert a.acquire_or_refresh_lock("/f") == LOCK_HELD
    assert b.acquire_or_refresh_lock("/f") == LOCK_HELD  # refresh, same holder
    b.unlock("/f")
    assert "/f" not in fake.locks


def test_lock_holder_property_suffix():
    from cs3_jupyter.cs3mixin import CS3Mixin

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
