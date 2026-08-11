"""Hybrid manager: CS3 is the source of truth, FUSE only a fast path.

The fake FUSE mount (root_dir tmpdir) deliberately lags: files created via
CS3 never appear on disk, reproducing the propagation delay of a real mount.
"""

import os

import nbformat
import pytest
from tornado.web import HTTPError

from cs3_jupyter.cs3largefilemanager import CS3HybridLargeFileManager

from conftest import make_manager


@pytest.fixture
def fuse_dir(tmp_path):
    return tmp_path


@pytest.fixture
def manager(patch_cs3, fuse_dir):
    return make_manager(CS3HybridLargeFileManager, patch_cs3, root_dir=str(fuse_dir))


def notebook_model():
    nb = nbformat.v4.new_notebook()
    return {"type": "notebook", "content": nbformat.v4.new_notebook(), "format": "json"}, nb


def test_new_notebook_created_via_cs3_and_locked(manager, patch_cs3, fuse_dir):
    model, _ = notebook_model()
    result = manager.save(model, "nb.ipynb")

    os_path = f"{fuse_dir}/nb.ipynb"
    assert os_path in patch_cs3.files  # created in the storage...
    assert not os.path.exists(os_path)  # ...even though FUSE hasn't caught up
    assert patch_cs3.locks[os_path]["app_name"] == manager.lock_holder
    # the post-save get succeeded despite the FUSE miss
    assert result["type"] == "notebook"
    assert result["writable"] is True


def test_new_empty_file_touched_via_cs3_and_locked(manager, patch_cs3, fuse_dir):
    manager.save({"type": "file", "content": "", "format": "text"}, "empty.txt")
    os_path = f"{fuse_dir}/empty.txt"
    assert patch_cs3.files[os_path] == b""
    assert not os.path.exists(os_path)
    assert patch_cs3.locks[os_path]["app_name"] == manager.lock_holder


def test_get_content_falls_back_to_cs3(manager, patch_cs3, fuse_dir):
    patch_cs3.put(f"{fuse_dir}/only-in-cs3.txt", b"hello")
    model = manager.get("only-in-cs3.txt", content=True)
    assert model["content"] == "hello"
    assert model["writable"] is True


def test_save_foreign_locked_rejected_before_write(manager, patch_cs3, fuse_dir):
    os_path = f"{fuse_dir}/doc.txt"
    patch_cs3.put(os_path, b"theirs", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        manager.save({"type": "file", "content": "mine", "format": "text"}, "doc.txt")
    assert exc.value.status_code == 423
    assert patch_cs3.files[os_path] == b"theirs"


def test_model_read_only_when_foreign_locked(manager, patch_cs3, fuse_dir):
    patch_cs3.put(f"{fuse_dir}/doc.txt", b"x", lock={"app_name": "collabora", "lock_id": "x"})
    assert manager.get("doc.txt", content=True)["writable"] is False


def test_delete_foreign_locked_rejected(manager, patch_cs3, fuse_dir):
    os_path = f"{fuse_dir}/doc.txt"
    patch_cs3.put(os_path, b"x", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        manager.delete_file("doc.txt")
    assert exc.value.status_code == 423
    assert os_path in patch_cs3.files


def test_rename_foreign_locked_rejected(manager, patch_cs3, fuse_dir):
    patch_cs3.put(f"{fuse_dir}/doc.txt", b"x", lock={"app_name": "collabora", "lock_id": "x"})
    with pytest.raises(HTTPError) as exc:
        manager.rename_file("doc.txt", "new.txt")
    assert exc.value.status_code == 423


def test_rename_of_own_locked_file_carries_lock(manager, patch_cs3, fuse_dir):
    old, new = f"{fuse_dir}/a.txt", f"{fuse_dir}/b.txt"
    patch_cs3.put(old, b"x", lock={"app_name": manager.lock_holder, "lock_id": manager.lock_value})
    manager.rename_file("a.txt", "b.txt")
    assert new in patch_cs3.files and old not in patch_cs3.files
    assert patch_cs3.locks[new]["app_name"] == manager.lock_holder


def test_delete_goes_through_cs3(manager, patch_cs3, fuse_dir):
    os_path = f"{fuse_dir}/gone.txt"
    patch_cs3.put(os_path, b"x")
    manager.delete_file("gone.txt")
    assert os_path not in patch_cs3.files


def test_new_directory_created_via_cs3(manager, patch_cs3, fuse_dir):
    result = manager.save({"type": "directory"}, "newdir")
    os_path = f"{fuse_dir}/newdir"
    assert os_path in patch_cs3.files
    assert not os.path.exists(os_path)
    # the post-create get serves an (empty) directory model during the window
    assert result["type"] == "directory"


def test_chunked_upload_appends_via_cs3(manager, patch_cs3, fuse_dir):
    os_path = f"{fuse_dir}/big.txt"
    manager.save({"type": "file", "content": "aa", "format": "text", "chunk": 1}, "big.txt")
    manager.save({"type": "file", "content": "bb", "format": "text", "chunk": 2}, "big.txt")
    manager.save({"type": "file", "content": "cc", "format": "text", "chunk": -1}, "big.txt")
    assert patch_cs3.files[os_path] == b"aabbcc"
    assert not os.path.exists(os_path)  # never touched the mount
