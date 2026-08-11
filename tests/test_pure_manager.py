"""Pure CS3 manager: lock-aware save funnel, chunked upload, read-only models."""

import asyncio

import pytest
from cs3client.exceptions import AlreadyExistsException
from tornado.web import HTTPError

from cs3_jupyter.cs3largefilemanager import CS3LargeFileManager

from conftest import DIR, make_manager

ROOT = "/fakeroot"


@pytest.fixture
def manager(patch_cs3):
    patch_cs3.put(ROOT)  # root exists...
    patch_cs3.files[ROOT] = DIR  # ...as a directory
    return make_manager(CS3LargeFileManager, patch_cs3, root_path=ROOT)


@pytest.fixture
def strict_manager(manager, patch_cs3):
    """A manager whose backend refuses to clobber, the way reva/EOS does.

    The shared fake lets make_dir and rename_file overwrite silently, which
    hides guards that never fire. These tests need the storage to object.
    """
    plain_make_dir = patch_cs3.make_dir
    plain_rename = patch_cs3.rename_file

    def make_dir(token, resource):
        if resource._abs_path in patch_cs3.files:
            raise AlreadyExistsException("exists")
        plain_make_dir(token, resource)

    def rename_file(token, resource, newresource, lock_id=None):
        if newresource._abs_path in patch_cs3.files:
            raise AlreadyExistsException("exists")
        plain_rename(token, resource, newresource, lock_id=lock_id)

    patch_cs3.make_dir = make_dir
    patch_cs3.rename_file = rename_file
    return manager


def test_save_new_file_creates_and_locks(manager, patch_cs3):
    result = asyncio.run(
        manager.save({"type": "file", "content": "hello", "format": "text"}, "f.txt")
    )
    assert patch_cs3.files[f"{ROOT}/f.txt"] == b"hello"
    assert patch_cs3.locks[f"{ROOT}/f.txt"]["app_name"] == manager.lock_holder
    assert result["writable"] is True


def test_save_new_empty_file_uses_touch(manager, patch_cs3):
    asyncio.run(manager.save({"type": "file", "content": "", "format": "text"}, "e.txt"))
    assert patch_cs3.files[f"{ROOT}/e.txt"] == b""
    assert patch_cs3.locks[f"{ROOT}/e.txt"]["app_name"] == manager.lock_holder


def test_save_foreign_locked_rejected(manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/f.txt", b"theirs", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        asyncio.run(
            manager.save({"type": "file", "content": "mine", "format": "text"}, "f.txt")
        )
    assert exc.value.status_code == 423
    assert patch_cs3.files[f"{ROOT}/f.txt"] == b"theirs"


def test_model_read_only_when_foreign_locked(manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/f.txt", b"x", lock={"app_name": "collabora", "lock_id": "x"})
    model = asyncio.run(manager.get("f.txt", content=True))
    assert model["writable"] is False


def test_delete_foreign_locked_rejected(manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/f.txt", b"x", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        asyncio.run(manager.delete_file("f.txt"))
    assert exc.value.status_code == 423


def test_rename_foreign_locked_rejected(manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/f.txt", b"x", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        asyncio.run(manager.rename_file("f.txt", "g.txt"))
    assert exc.value.status_code == 423


def test_mkdir_over_existing_directory_is_a_noop(strict_manager, patch_cs3):
    """Regression: _save_directory fed self.exists() an OS path.

    exists() takes an API path and re-prefixes it with root_dir, so the guard
    never matched and every mkdir was pushed to the storage. reva answers
    ALREADY_EXISTS, which surfaced as a 500 instead of a quiet no-op.
    """
    patch_cs3.files[f"{ROOT}/existing"] = DIR
    asyncio.run(strict_manager.save({"type": "directory"}, "existing"))
    assert patch_cs3.files[f"{ROOT}/existing"] is DIR


def test_mkdir_over_existing_file_is_400(strict_manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/f.txt", b"x")
    with pytest.raises(HTTPError) as exc:
        asyncio.run(strict_manager.save({"type": "directory"}, "f.txt"))
    assert exc.value.status_code == 400
    assert "Not a directory" in exc.value.log_message


def test_rename_onto_existing_is_409_not_500(strict_manager, patch_cs3):
    """The clash must be caught here, not bubble up as a storage error."""
    patch_cs3.put(f"{ROOT}/a.txt", b"a")
    patch_cs3.put(f"{ROOT}/b.txt", b"b")
    with pytest.raises(HTTPError) as exc:
        asyncio.run(strict_manager.rename_file("a.txt", "b.txt"))
    assert exc.value.status_code == 409
    assert patch_cs3.files[f"{ROOT}/a.txt"] == b"a"  # source untouched


def test_rename_to_free_name_succeeds(strict_manager, patch_cs3):
    patch_cs3.put(f"{ROOT}/a.txt", b"a")
    asyncio.run(strict_manager.rename_file("a.txt", "fresh.txt"))
    assert patch_cs3.files[f"{ROOT}/fresh.txt"] == b"a"
    assert f"{ROOT}/a.txt" not in patch_cs3.files


def test_chunked_upload_assembles_file(manager, patch_cs3):
    """Regression: without a chunk-aware save every chunk truncate-overwrote."""
    asyncio.run(manager.save({"type": "file", "content": "aa", "format": "text", "chunk": 1}, "big.txt"))
    asyncio.run(manager.save({"type": "file", "content": "bb", "format": "text", "chunk": 2}, "big.txt"))
    asyncio.run(manager.save({"type": "file", "content": "cc", "format": "text", "chunk": -1}, "big.txt"))
    assert patch_cs3.files[f"{ROOT}/big.txt"] == b"aabbcc"
    # the upload holds our lock from chunk 1 onwards
    assert patch_cs3.locks[f"{ROOT}/big.txt"]["app_name"] == manager.lock_holder
