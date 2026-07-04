import asyncio
from typing import Optional, Union
import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr
from cs3client.exceptions import NotFoundException
from tornado.web import HTTPError
from .statuscodehandler import FileLockedError
from .utils import retry_on_auth_failure, resource_from_path

# Result states of acquire_or_refresh_lock.
LOCK_HELD = "held"
LOCK_FOREIGN = "foreign"
LOCK_NO_FILE = "no_file"


class CS3Lock:

    # Serializes reva-lock state with session-tracker state. The POST
    # (acquire+user_seen) and DELETE (user_left+unlock) flows in the lock handler
    # each touch two pieces of state that must move together; without this mutex a
    # DELETE can decide to unlock between a concurrent POST's successful refresh
    # and its user_seen increment, leaving the joining session believing it holds
    # a lock that has already been released.
    _lock_mutex = asyncio.Lock()

    def acquire_or_refresh_lock(self, path: str) -> str:
        """Try to set or refresh our lock on the given path.

        Returns LOCK_HELD if we now hold the lock, LOCK_FOREIGN if it is held
        by another holder, LOCK_NO_FILE if the file does not exist (yet).
        """
        # If the lock is ours the refresh will succeed; on anything else
        # (unlocked, expired, foreign holder) reva answers FAILED_PRECONDITION,
        # surfaced as FileLockedError. Then SetLock succeeds only if unlocked.
        try:
            self.refresh_lock(path, self.lock_holder, self.lock_value, self.lock_value)
            return LOCK_HELD
        except FileNotFoundError:
            return LOCK_NO_FILE
        except FileLockedError:
            pass
        try:
            self.set_lock(path, self.lock_holder, self.lock_value)
            return LOCK_HELD
        except FileNotFoundError:
            return LOCK_NO_FILE
        except FileLockedError:
            return LOCK_FOREIGN

    def ensure_write_lock(self, os_path: str) -> str:
        """Acquire/refresh our lock before a write; 423 if a foreign holder has it.

        The pre-write check is the only protection on storages that do not
        enforce locks on upload (e.g. cephmount, localfs).
        """
        state = self.acquire_or_refresh_lock(os_path)
        if state == LOCK_FOREIGN:
            holder = self.foreign_lock_holder(os_path) or "another application"
            raise HTTPError(423, f"{os_path} is locked by {holder}")
        return state

    def lock_after_create(self, os_path: str) -> None:
        """Best-effort lock of a file we just created.

        A file cannot be locked before it exists; if another holder wins the
        race in between, the next write fails cleanly with 423.
        """
        try:
            self.set_lock(os_path, self.lock_holder, self.lock_value)
        except (FileLockedError, FileNotFoundError) as e:
            self.log.warning(f"Could not lock newly created file {os_path}: {e}")

    def foreign_lock_holder(self, path: str) -> Optional[str]:
        """Return the holder's app name if the path is locked by someone else, else None."""
        lock = self.get_lock(path)
        if lock is None:
            return None
        holder = lock.get("app_name") if isinstance(lock, dict) else getattr(lock, "app_name", "")
        if holder != self.lock_holder:
            return holder or "unknown"
        return None

    @retry_on_auth_failure
    def unlock(self, path: str, app_name: str, lock_id: Union[str, int]) -> None:
        """Unlock a file."""
        try:
            resource = resource_from_path(path)
            return self.client.file.unlock(
                self.auth.get_token(),
                resource,
                app_name,
                lock_id
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def get_lock(self, path: str) -> Union[cs3spr.Lock, dict, None]:
        """Get locks for a file."""
        try:
            resource = resource_from_path(path)
            return self.client.file.get_lock(
                self.auth.get_token(),
                resource
            )
        # GetLock returns NotFound if there's no lock (or no file).
        except NotFoundException:
            return None
        except FileNotFoundError:
            return None
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def refresh_lock(self, path: str, app_name: str, lock_id: Union[str, int], existing_lock_id: Union[str, int]) -> None:
        """Refresh a file lock."""
        try:
            resource = resource_from_path(path)
            return self.client.file.refresh_lock(
                self.auth.get_token(),
                resource,
                app_name,
                lock_id,
                existing_lock_id
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def set_lock(self, path: str, app_name: str, lock_id: Union[str, int]) -> None:
        """Set a file lock."""
        try:
            resource = resource_from_path(path)
            return self.client.file.set_lock(
                self.auth.get_token(),
                resource,
                app_name,
                lock_id,
            )
        except Exception as e:
            self.status_handler.handle_errors(e)
