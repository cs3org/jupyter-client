from contextlib import contextmanager
from typing import Iterator, Optional, Union
import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr
from cs3client.exceptions import NotFoundException
from .statuscodehandler import FileLockedError, translate_cs3_error
from .utils import retry_on_auth_failure, resource_from_path

# Result states of acquire_or_refresh_lock.
LOCK_HELD = "held"
LOCK_FOREIGN = "foreign"
LOCK_NO_FILE = "no_file"


class CS3Lock:
    """Locks taken with our own holder (lock_holder) and lock id (lock_value)."""

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
            self.refresh_lock(path)
            return LOCK_HELD
        except OSError as e:
            self.log.debug(f"Could not refresh lock on {path}, trying to set it: {e}")

        try:
            self.set_lock(path)
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
        """Acquire/refresh our lock before a write; FileLockedError if a foreign holder has it.

        The pre-write check is the only protection on storages that do not
        enforce locks on upload (e.g. cephmount, localfs).
        """
        state = self.acquire_or_refresh_lock(os_path)
        if state == LOCK_FOREIGN:
            holder = self.foreign_lock_holder(os_path) or "another application"
            raise FileLockedError(f"{os_path} is locked by {holder}", holder=holder)
        return state

    @contextmanager
    def write_lock(self, os_path: str) -> Iterator[str]:
        """Hold our lock around a write; FileLockedError if someone else holds it.

        Yields the acquire state. LOCK_NO_FILE means the write creates the
        file, which cannot be locked before it exists: it is locked right after
        a successful write instead.
        """
        state = self.ensure_write_lock(os_path)
        yield state
        if state == LOCK_NO_FILE:
            self.lock_after_create(os_path)

    def lock_after_create(self, os_path: str) -> None:
        """Best-effort lock of a file we just created.

        A file cannot be locked before it exists; if another holder wins the
        race in between, the next write fails cleanly with 423.
        """
        try:
            self.set_lock(os_path)
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
    def unlock(self, path: str) -> None:
        """Release our lock on a file."""
        try:
            self.client.file.unlock(
                self.auth.get_token(),
                resource_from_path(path),
                self.lock_holder,
                self.lock_value
            )
        except Exception as e:
            translate_cs3_error(e)

    @retry_on_auth_failure
    def get_lock(self, path: str) -> Union[cs3spr.Lock, dict, None]:
        """Get the lock on a file, None if there is none."""
        try:
            return self.client.file.get_lock(
                self.auth.get_token(),
                resource_from_path(path)
            )
        # GetLock returns NotFound if there's no lock (or no file).
        except NotFoundException:
            return None
        except FileNotFoundError:
            return None
        except Exception as e:
            translate_cs3_error(e)

    @retry_on_auth_failure
    def refresh_lock(self, path: str) -> None:
        """Extend our lock on a file; fails unless we hold it."""
        try:
            self.client.file.refresh_lock(
                self.auth.get_token(),
                resource_from_path(path),
                self.lock_holder,
                self.lock_value,
                self.lock_value
            )
        except Exception as e:
            translate_cs3_error(e)

    @retry_on_auth_failure
    def set_lock(self, path: str) -> None:
        """Lock a file; FileLockedError if it is locked already."""
        try:
            self.client.file.set_lock(
                self.auth.get_token(),
                resource_from_path(path),
                self.lock_holder,
                self.lock_value,
            )
        except Exception as e:
            translate_cs3_error(e)
