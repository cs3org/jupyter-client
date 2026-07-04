"""Pure CS3 manager: lock-aware save funnel, chunked upload, read-only models."""

import asyncio

import pytest
from tornado.web import HTTPError

from cs3_jupyter_client.cs3largefilemanager import CS3LargeFileManager

from conftest import make_manager

ROOT = "/fakeroot"


@pytest.fixture
def manager(patch_cs3):
    patch_cs3.put(ROOT)  # root exists...
    patch_cs3.files[ROOT] = __import__("conftest").DIR  # ...as a directory
    return make_manager(CS3LargeFileManager, patch_cs3, root_path=ROOT)


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


def test_chunked_upload_assembles_file(manager, patch_cs3):
    """Regression: without a chunk-aware save every chunk truncate-overwrote."""
    asyncio.run(manager.save({"type": "file", "content": "aa", "format": "text", "chunk": 1}, "big.txt"))
    asyncio.run(manager.save({"type": "file", "content": "bb", "format": "text", "chunk": 2}, "big.txt"))
    asyncio.run(manager.save({"type": "file", "content": "cc", "format": "text", "chunk": -1}, "big.txt"))
    assert patch_cs3.files[f"{ROOT}/big.txt"] == b"aabbcc"
    # the upload holds our lock from chunk 1 onwards
    assert patch_cs3.locks[f"{ROOT}/big.txt"]["app_name"] == manager.lock_holder
