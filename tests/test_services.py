"""How the CS3 services are composed: one session each, only what is used."""

import logging

import pytest

from cs3_jupyter.cs3largefilemanager import CS3LargeFileManager
from cs3_jupyter.cs3api.errors import FileLockedError
from cs3_jupyter.server_extension.client import CS3SharingClient

from conftest import DIR, make_manager

ROOT = "/fakeroot"


@pytest.fixture
def manager(patch_cs3):
    patch_cs3.files[ROOT] = DIR
    return make_manager(CS3LargeFileManager, patch_cs3, root_path=ROOT)


def test_a_managers_services_share_its_session(manager):
    session = manager.session
    assert manager.storage.session is session
    assert manager.locks.session is session
    assert manager.versions.session is session
    assert manager.locks.storage is manager.storage  # one stat cache
    assert manager.document_locks._locks is manager.locks


def test_the_extension_client_builds_only_what_its_handlers_use(patch_cs3):
    client = CS3SharingClient(log=logging.getLogger("test"))
    assert {"sharing", "users", "groups", "spaces"} <= vars(client).keys()
    for unused in ("storage", "locks", "document_locks"):
        assert not hasattr(client, unused)


def test_copy_onto_a_foreign_locked_file_raises_a_lock_error(manager, patch_cs3):
    """It used to be translated twice, ending up a plain OSError."""
    patch_cs3.put(f"{ROOT}/a.txt", b"x")
    patch_cs3.put(f"{ROOT}/b.txt", b"theirs", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(FileLockedError):
        manager.storage.copyfile(f"{ROOT}/a.txt", f"{ROOT}/b.txt")
    assert patch_cs3.files[f"{ROOT}/b.txt"] == b"theirs"
