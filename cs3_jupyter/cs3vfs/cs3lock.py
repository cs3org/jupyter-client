import asyncio
from typing import Union
import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr
from cs3client.exceptions import NotFoundException
from .utils import retry_on_auth_failure, resource_from_path

class CS3Lock:

    # Serializes reva-lock state with session-tracker state. The POST
    # (acquire+user_seen) and DELETE (user_left+unlock) flows in the lock handler
    # each touch two pieces of state that must move together; without this mutex a
    # DELETE can decide to unlock between a concurrent POST's successful refresh
    # and its user_seen increment, leaving the joining session believing it holds
    # a lock that has already been released.
    _lock_mutex = asyncio.Lock()

    def acquire_or_refresh_lock(self, path: str) -> bool:
        """Try to set or refresh our lock on the given path.

        Returns True if we now hold the lock, False if it is held by another client.
        """
        # If the lock is our the refresh will succeed, otherwise it will fail (even if there is no lock).
        # In that case, we try to set the lock, which will only succeed if there is no active lock.
        try:
           self.refresh_lock(path, self.lock_app_name, self.lock_value, self.lock_value)
        except Exception:
           try:
               self.set_lock(path, self.lock_app_name, self.lock_value)
           except OSError:
               return False
        return True

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
        # GetLock returns NotFound if there's no lock.
        except NotFoundException:
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
