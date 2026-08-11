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
        # RefreshLock only succeeds when we already hold the lock. Every other
        # outcome is reported inconsistently: "not locked" and "not the holder"
        # come back as FAILED_PRECONDITION, but a file that does not exist
        # surfaces as an internal error, because the EOS driver wraps - and
        # thereby hides - the not-found from the storage provider's type
        # switch. So treat any failure as "we do not hold it" and let SetLock,
        # which is authoritative, decide.
        try:
            self.refresh_lock(path, self.lock_holder, self.lock_value, self.lock_value)
            return LOCK_HELD
        except OSError as e:
            self.log.debug(f"Could not refresh lock on {path}, trying to set it: {e}")

        try:
            self.set_lock(path, self.lock_holder, self.lock_value)
            return LOCK_HELD
        except FileLockedError:
            return LOCK_FOREIGN
        except FileNotFoundError:
            return LOCK_NO_FILE
        except PermissionError:
            raise
        except OSError:
            # SetLock hides a missing file the same way. A file that is not
            # there yet is the normal case for a first save (it gets locked
            # after creation), anything else is a genuine failure.
            if not self.vfs_exists(path):
                return LOCK_NO_FILE
            raise

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
        except OSError as e:
            # Best effort by design: the content is already written, and a
            # subsequent save fails cleanly with 423 if someone else won.
            self.log.warning(f"Could not lock newly created file {os_path}: {e}")

    def foreign_lock_holder(self, path: str) -> Optional[str]:
        """Return the holder's app name if the path is locked by someone else, else None."""
        try:
            lock = self.get_lock(path)
        except OSError as e:
            # Lock state is advisory here (the storage enforces its own), so
            # an unreadable lock must not fail the read or the delete.
            self.log.warning(f"Could not read lock state of {path}: {e}")
            return None
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
