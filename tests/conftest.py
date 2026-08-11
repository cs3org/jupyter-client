"""Shared fakes: an in-memory CS3 backend with reva/EOS-like lock semantics."""

import logging
import time
from types import SimpleNamespace

import pytest

import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr
from cs3client.exceptions import (
    AlreadyExistsException,
    FileLockedException,
    NotFoundException,
    UnknownException,
)

import cs3_jupyter.cs3mixin as cs3mixin_module

DIR = object()  # sentinel value marking a directory entry


def _info(path, entry):
    is_dir = entry is DIR
    return SimpleNamespace(
        path=path,
        size=0 if is_dir else len(entry),
        mtime=SimpleNamespace(seconds=int(time.time()), nanos=0),
        type=(
            cs3spr.ResourceType.RESOURCE_TYPE_CONTAINER
            if is_dir
            else cs3spr.ResourceType.RESOURCE_TYPE_FILE
        ),
        permission_set=SimpleNamespace(create_container=True, delete=True),
    )


class FakeCS3(object):
    """Stands in for cs3client's `client.file`.

    Lock semantics mirror reva's EOS driver: writes are gated on the lock
    *holder* (app name) only, Unlock/RefreshLock additionally require the
    matching lock id, SetLock conflicts when a lock exists, and GetLock
    raises NotFound when there is no lock.

    Crucially, lock operations on a file that does not exist raise
    UnknownException by default, not NotFoundException: the EOS driver wraps
    the not-found, so the storage provider's type switch falls through to a
    generic internal error. Set `missing_file_error = "not_found"` to emulate
    a driver that reports it properly.
    """

    def __init__(self):
        self.files = {}  # path -> bytes | DIR
        self.locks = {}  # path -> {"app_name", "lock_id"}
        self.missing_file_error = "unknown"

    # -- helpers used by tests --------------------------------------------
    def put(self, path, content=b"", lock=None):
        self.files[path] = content
        if lock:
            self.locks[path] = lock

    def _missing_for_lock_op(self, operation):
        if self.missing_file_error == "not_found":
            return NotFoundException("not found")
        return UnknownException(
            f'Unknown Error: operation="{operation}" status_code="15" '
            f'message="error {operation}: path not resolvable"'
        )

    def _check_write(self, path, app_name):
        lock = self.locks.get(path)
        if lock and lock["app_name"] != app_name:
            raise FileLockedException("Lock mismatch")

    # -- file API ----------------------------------------------------------
    def stat(self, token, resource):
        path = resource._abs_path
        if path not in self.files:
            raise NotFoundException("not found")
        return _info(path, self.files[path])

    def touch_file(self, token, resource):
        path = resource._abs_path
        if path in self.files:
            raise AlreadyExistsException("exists")
        self.files[path] = b""

    def write_file(self, token, resource, content, size, app_name=None, lock_id=None):
        path = resource._abs_path
        self._check_write(path, app_name)
        if not isinstance(content, (bytes, str)):
            content = b"".join(content)  # streamed generator
        if isinstance(content, str):
            content = content.encode()
        self.files[path] = content

    def read_file(self, token, resource, lock_id=None):
        path = resource._abs_path
        if path not in self.files:
            raise NotFoundException("not found")

        def chunks():
            yield self.files[path]

        return chunks()

    def remove_file(self, token, resource, lock_id=None):
        path = resource._abs_path
        if path not in self.files:
            raise NotFoundException("not found")
        lock = self.locks.get(path)
        if lock and lock["lock_id"] != lock_id:
            raise FileLockedException("Lock mismatch")
        del self.files[path]
        self.locks.pop(path, None)

    def rename_file(self, token, resource, newresource, lock_id=None):
        path, new_path = resource._abs_path, newresource._abs_path
        if path not in self.files:
            raise NotFoundException("not found")
        lock = self.locks.get(path)
        if lock and lock["lock_id"] != lock_id:
            raise FileLockedException("Lock mismatch")
        self.files[new_path] = self.files.pop(path)
        if path in self.locks:
            self.locks[new_path] = self.locks.pop(path)

    def make_dir(self, token, resource):
        self.files[resource._abs_path] = DIR

    def list_dir(self, token, resource):
        path = resource._abs_path.rstrip("/")
        if self.files.get(path) is not DIR:
            raise NotFoundException("not found")
        prefix = path + "/"
        return [
            _info(p, entry)
            for p, entry in self.files.items()
            if p.startswith(prefix) and "/" not in p[len(prefix):]
        ]

    # -- lock API ----------------------------------------------------------
    def set_lock(self, token, resource, app_name, lock_id):
        path = resource._abs_path
        if path not in self.files:
            raise self._missing_for_lock_op("setting lock")
        if path in self.locks:
            # errtypes.Conflict -> FAILED_PRECONDITION
            raise FileLockedException("already locked")
        self.locks[path] = {"app_name": app_name, "lock_id": lock_id}

    def get_lock(self, token, resource):
        path = resource._abs_path
        if path not in self.files:
            raise self._missing_for_lock_op("getting lock")
        if path not in self.locks:
            raise NotFoundException("no lock")
        return dict(self.locks[path])

    def refresh_lock(self, token, resource, app_name, lock_id, existing_lock_id=None):
        path = resource._abs_path
        if path not in self.files:
            raise self._missing_for_lock_op("refreshing lock")
        lock = self.locks.get(path)
        # errtypes.BadRequest ("not locked" / "not the holder") ->
        # FAILED_PRECONDITION
        if lock is None or lock["app_name"] != app_name:
            raise FileLockedException("not the holder")
        if existing_lock_id and lock["lock_id"] != existing_lock_id:
            raise FileLockedException("lock id mismatch")
        self.locks[path] = {"app_name": app_name, "lock_id": lock_id}

    def unlock(self, token, resource, app_name, lock_id):
        path = resource._abs_path
        if path not in self.files:
            raise self._missing_for_lock_op("unlocking")
        lock = self.locks.get(path)
        if lock is None:
            raise FileLockedException("file was not locked")
        if lock["app_name"] != app_name or lock["lock_id"] != lock_id:
            raise FileLockedException("not the holder")
        del self.locks[path]


class FakeAuth:
    def __init__(self, client=None):
        pass

    def set_client_id(self, client_id):
        pass

    def set_client_secret(self, secret):
        pass

    def get_token(self):
        return ("x-access-token", "fake-token")


@pytest.fixture
def fake_cs3():
    return FakeCS3()


@pytest.fixture
def patch_cs3(monkeypatch, fake_cs3):
    """Make CS3Mixin subclasses instantiable offline, backed by fake_cs3."""
    monkeypatch.setattr(
        cs3mixin_module,
        "CS3Client",
        lambda config, name, log: SimpleNamespace(file=fake_cs3),
    )
    monkeypatch.setattr(cs3mixin_module, "Auth", FakeAuth)
    return fake_cs3


def make_manager(cls, fake_cs3, **traits):
    """Instantiate a contents manager against the fake backend, muting events."""
    manager = cls(log=logging.getLogger("test"), **traits)
    manager.emit = lambda data: None
    return manager
