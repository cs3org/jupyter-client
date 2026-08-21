"""Hybrid manager: CS3 is the source of truth, FUSE only a fast path.

The fake FUSE mount (root_dir tmpdir) deliberately lags: files created via
CS3 never appear on disk, reproducing the propagation delay of a real mount.
"""

import os

import nbformat
import pytest
from tornado.web import HTTPError

from cs3_jupyter.cs3largefilemanager import CS3HybridLargeFileManager

from conftest import DIR, make_manager


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


def test_model_build_stats_once(manager, patch_cs3, fuse_dir):
    """Building one model asked "does it exist?", "is it a dir?" and "stat it".

    That is the same Stat three times, and the file browser repeats it for
    every entry on every refresh - it was 78% of all traffic.
    """
    patch_cs3.put(f"{fuse_dir}/only-in-cs3.txt", b"hello")
    patch_cs3.calls.clear()
    manager.get("only-in-cs3.txt", content=False)
    assert patch_cs3.calls["stat"] == 1


def test_save_acquires_the_lock_once(manager, patch_cs3, fuse_dir):
    """_save_* took the lock, then atomic_writing -> open() took it again."""
    os_path = f"{fuse_dir}/doc.txt"
    patch_cs3.put(os_path, b"old", lock={"app_name": manager.lock_holder, "lock_id": manager.lock_value})
    patch_cs3.calls.clear()

    manager.save({"type": "file", "content": "new", "format": "text"}, "doc.txt")

    assert patch_cs3.calls["refresh_lock"] == 1
    assert patch_cs3.files[os_path] == b"new"


def test_save_invalidates_the_cached_stat(manager, patch_cs3, fuse_dir):
    """The post-save model must carry the new size, not the cached one."""
    patch_cs3.put(f"{fuse_dir}/doc.txt", b"old")
    manager.get("doc.txt", content=False)  # warms the cache at size 3

    model = manager.save({"type": "file", "content": "longer", "format": "text"}, "doc.txt")

    assert model["size"] == len("longer")


def test_rename_invalidates_both_paths(manager, patch_cs3, fuse_dir):
    patch_cs3.put(f"{fuse_dir}/a.txt", b"x")
    assert manager.file_exists("a.txt")

    manager.rename_file("a.txt", "b.txt")

    assert manager.file_exists("b.txt")
    assert not manager.file_exists("a.txt")


def test_delete_invalidates_the_cached_stat(manager, patch_cs3, fuse_dir):
    patch_cs3.put(f"{fuse_dir}/gone.txt", b"x")
    assert manager.file_exists("gone.txt")

    manager.delete_file("gone.txt")

    assert not manager.file_exists("gone.txt")


def test_directory_rename_skips_the_lock_lookup(manager, patch_cs3, fuse_dir):
    """Containers cannot hold EOS locks, so GetLock is a wasted round-trip."""
    patch_cs3.files[f"{fuse_dir}/d"] = DIR
    patch_cs3.calls.clear()

    manager.rename_file("d", "d2")

    assert patch_cs3.calls["get_lock"] == 0
    assert f"{fuse_dir}/d2" in patch_cs3.files


def test_copy_streams_server_side_without_locking(manager, patch_cs3, fuse_dir):
    """A copy destination is a new file, not an open document: no lock on it."""
    patch_cs3.put(f"{fuse_dir}/a.txt", b"x")

    model = manager.copy("a.txt")

    assert patch_cs3.files[f"{fuse_dir}/a-Copy1.txt"] == b"x"
    assert model["path"] == "a-Copy1.txt"
    assert patch_cs3.locks == {}
    assert patch_cs3.calls["set_lock"] == 0


def test_chunked_upload_appends_via_cs3(manager, patch_cs3, fuse_dir):
    os_path = f"{fuse_dir}/big.txt"
    manager.save({"type": "file", "content": "aa", "format": "text", "chunk": 1}, "big.txt")
    manager.save({"type": "file", "content": "bb", "format": "text", "chunk": 2}, "big.txt")
    manager.save({"type": "file", "content": "cc", "format": "text", "chunk": -1}, "big.txt")
    assert patch_cs3.files[os_path] == b"aabbcc"
    assert not os.path.exists(os_path)  # never touched the mount


def test_copy_of_a_mounted_file_costs_nothing(manager, patch_cs3, fuse_dir):
    """A copy takes no lock, so a source in the mount needs no RPC at all."""
    (fuse_dir / "a.txt").write_bytes(b"x")
    patch_cs3.calls.clear()

    model = manager.copy("a.txt")

    assert (fuse_dir / "a-Copy1.txt").read_bytes() == b"x"
    assert model["path"] == "a-Copy1.txt"
    ## the single Stat is increment_filename proving "-Copy1" is free: the mount
    ## cannot prove a name absent, and picking one that exists only in CS3 would
    ## clobber it. No content transfer, no lock RPCs.
    assert dict(patch_cs3.calls) == {"stat": 1}, dict(patch_cs3.calls)


def test_copy_onto_foreign_locked_destination_rejected(manager, patch_cs3, fuse_dir):
    """The one case a copy can clobber: an explicit, already-locked destination."""
    (fuse_dir / "a.txt").write_bytes(b"mine")
    (fuse_dir / "taken.txt").write_bytes(b"theirs")
    patch_cs3.put(f"{fuse_dir}/taken.txt", b"theirs",
                  lock={"app_name": "collabora", "lock_id": "x"})

    with pytest.raises(HTTPError) as exc:
        manager.copy("a.txt", "taken.txt")

    assert exc.value.status_code == 423
    assert (fuse_dir / "taken.txt").read_bytes() == b"theirs"


def test_directory_rename_uses_the_mount(manager, patch_cs3, fuse_dir):
    """A container holds no lock, so the rename is a syscall, not a Move RPC."""
    (fuse_dir / "d").mkdir()
    (fuse_dir / "d" / "inner.txt").write_bytes(b"x")
    patch_cs3.calls.clear()

    manager.rename_file("d", "d2")

    assert (fuse_dir / "d2" / "inner.txt").read_bytes() == b"x"
    assert not (fuse_dir / "d").exists()
    assert patch_cs3.calls["rename_file"] == 0
    assert patch_cs3.calls["get_lock"] == 0


def test_file_rename_still_carries_the_lock(manager, patch_cs3, fuse_dir):
    """Files keep going through CS3: the lock id is what permits the move."""
    old = f"{fuse_dir}/a.txt"
    patch_cs3.put(old, b"x", lock={"app_name": manager.lock_holder, "lock_id": manager.lock_value})

    manager.rename_file("a.txt", "b.txt")

    assert f"{fuse_dir}/b.txt" in patch_cs3.files
    assert patch_cs3.calls["rename_file"] == 1


def test_directory_delete_sends_no_lock_id(patch_cs3, fuse_dir, manager):
    """One Delete for the whole subtree, without a lock a container cannot hold."""
    seen = {}
    plain = patch_cs3.remove_file

    def spy(token, resource, lock_id=None):
        seen["lock_id"] = lock_id
        return plain(token, resource, lock_id=lock_id)

    patch_cs3.remove_file = spy
    patch_cs3.files[f"{fuse_dir}/gone"] = DIR

    manager.delete_file("gone")

    assert seen["lock_id"] is None
    assert f"{fuse_dir}/gone" not in patch_cs3.files


def test_listing_shows_a_cs3_rename_before_the_mount_catches_up(manager, patch_cs3, fuse_dir):
    """The rename happens in reva; os.listdir keeps showing the old name.

    Regression: a renamed notebook kept its default name in the explorer until
    eosxd's directory cache turned over.
    """
    patch_cs3.files[str(fuse_dir)] = DIR
    patch_cs3.put(f"{fuse_dir}/Untitled.ipynb", b"x")
    (fuse_dir / "Untitled.ipynb").write_bytes(b"x")  # mount has caught up

    manager.rename_file("Untitled.ipynb", "Analysis.ipynb")

    assert os.listdir(fuse_dir) == ["Untitled.ipynb"]  # mount is behind
    assert [e["name"] for e in manager.get("", content=True)["content"]] == ["Analysis.ipynb"]


def test_listing_returns_to_the_mount_once_it_agrees(manager, patch_cs3, fuse_dir):
    patch_cs3.files[str(fuse_dir)] = DIR
    patch_cs3.put(f"{fuse_dir}/a.txt", b"x")
    (fuse_dir / "a.txt").write_bytes(b"x")
    manager.rename_file("a.txt", "b.txt")
    os.rename(fuse_dir / "a.txt", fuse_dir / "b.txt")  # eosxd catches up

    manager.get("", content=True)  # notices they agree, drops the pending paths
    patch_cs3.calls.clear()
    listing = [e["name"] for e in manager.get("", content=True)["content"]]

    assert listing == ["b.txt"]
    assert sum(patch_cs3.calls.values()) == 0, dict(patch_cs3.calls)


def test_listing_falls_back_to_the_mount_when_cs3_cannot_list(manager, patch_cs3, fuse_dir):
    """A stale view beats no view: never serve an empty directory on an error."""
    (fuse_dir / "a.txt").write_bytes(b"x")
    patch_cs3.put(f"{fuse_dir}/a.txt", b"x")
    manager.rename_file("a.txt", "b.txt")
    # the parent is not a listable container in CS3, so list_dir fails

    assert [e["name"] for e in manager.get("", content=True)["content"]] == ["a.txt"]
