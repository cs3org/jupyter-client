"""CS3 locks held for documents that are open in the frontend."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Protocol

from anyio.to_thread import run_sync

from .cs3vfs.cs3lock import LOCK_FOREIGN, LOCK_NO_FILE
from .cs3vfs.statuscodehandler import FileLockedError
from .sessiontracker import SessionTracker


class LockService(Protocol):
    """The lock operations DocumentLocks needs, all with our own holder and lock id."""

    def acquire_or_refresh_lock(self, path: str) -> str: ...
    def foreign_lock_holder(self, path: str) -> Optional[str]: ...
    def refresh_lock(self, path: str) -> None: ...
    def unlock(self, path: str) -> None: ...


class DocumentLocks:
    """Hold a document's CS3 lock while any frontend session has it open.

    The frontend opens a document with open() and repeats it as a heartbeat,
    and calls close() when done. The lock is taken on the first open and
    released when the last session closes. A background refresher keeps the
    locks of open documents alive and releases those whose sessions stopped
    sending heartbeats (a crashed tab never calls close()).

    Paths are storage paths: they key both the session counts and the locks.
    """

    def __init__(self, locks: LockService, expiration: int, log: Optional[logging.Logger] = None):
        self._locks = locks
        self.expiration = expiration
        self.log = log or logging.getLogger(__name__)
        self.sessions = SessionTracker(heartbeat_timeout_seconds=expiration)
        self._refresher: Optional[asyncio.Future] = None
        # Serializes the lock state in the storage with the session counts.
        # open (acquire + count) and close (count + unlock) each touch both, and
        # they must move together: otherwise a close can decide to unlock
        # between a concurrent open's refresh and its count, leaving the
        # joining session believing it holds a lock that was just released.
        self._mutex = asyncio.Lock()

    async def open(self, path: str, session_id: str) -> dict:
        """A session opened the document, or is still there (heartbeat).

        Returns the JSON the frontend reads. Raises FileNotFoundError if there
        is no such file.
        """
        async with self._mutex:
            state = await run_sync(self._locks.acquire_or_refresh_lock, path)
            if state == LOCK_NO_FILE:
                raise FileNotFoundError(path)
            if state == LOCK_FOREIGN:
                holder = await run_sync(self._locks.foreign_lock_holder, path)
                return {"locked": False, "read_only": True, "holder": holder}
            count = self.sessions.user_seen(path, session_id)
        self._ensure_refresher()
        return {"locked": True, "read_only": False, "count": count}

    async def close(self, path: str, session_id: str) -> Optional[dict]:
        """A session closed the document; the last one out releases the lock.

        Returns None if the session was not tracked, so a stray or duplicate
        close cannot unlock a file other sessions still have open. Raises
        FileLockedError if the lock is no longer ours (expired and taken over).
        """
        async with self._mutex:
            count = self.sessions.user_left(path, session_id)
            if count is None:
                return None
            if count > 0:
                return {"unlocked": False, "count": count}
            try:
                await run_sync(self._locks.unlock, path)
            except FileNotFoundError:
                pass
            return {"unlocked": True, "count": 0}

    def _ensure_refresher(self) -> None:
        """Start the background refresher if it is not already running."""
        if self._refresher is None or self._refresher.done():
            self._refresher = asyncio.ensure_future(self._refresh_loop())

    async def _refresh_loop(self) -> None:
        """Keep locks of open documents alive and release locks whose sessions all went stale.

        Runs while there are sessions; exits when idle (restarted lazily by the
        next open).
        """
        interval = max(self.expiration // 3, 10)
        while True:
            await asyncio.sleep(interval)
            async with self._mutex:
                active, expired = self.sessions.sweep()
                # A storage error must never kill the refresher: it is the only
                # thing keeping open documents locked and stale ones released.
                for path in expired:
                    try:
                        await run_sync(self._locks.unlock, path)
                    except FileLockedError:
                        # Not ours anymore - nothing to release.
                        pass
                    except OSError as e:
                        self.log.warning(f"Could not release lock on {path}: {e}")
                for path in active:
                    try:
                        await run_sync(self._locks.refresh_lock, path)
                    except OSError as e:
                        self.log.warning(f"Lock refresh lost on {path}: {e}")
            if not active and not expired:
                return
