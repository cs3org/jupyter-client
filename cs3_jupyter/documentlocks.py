"""CS3 locks held for documents that are open in the frontend."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Optional, Protocol

from anyio.to_thread import run_sync

from .cs3api.errors import FileLockedError
from .cs3api.locks import LOCK_FOREIGN, LOCK_NO_FILE


class SessionTracker:
    def __init__(self, heartbeat_timeout_seconds: float = 15.0):
        # Maps file_path -> { session_id: last_seen_timestamp }
        self._sessions = {}
        self._lock = threading.Lock()
        self._timeout = heartbeat_timeout_seconds

    def _evict_stale_sessions(self, file_path: str):
        """Internal helper to clean up expired sessions. Must be called under lock."""
        if file_path not in self._sessions:
            return

        cutoff = time.time() - self._timeout
        self._sessions[file_path] = {
            session_id: last_seen
            for session_id, last_seen in self._sessions[file_path].items()
            if last_seen > cutoff
        }

        # If no active users remain, clean up the file entirely to prevent memory leaks
        if not self._sessions[file_path]:
            del self._sessions[file_path]

    def user_seen(self, file_path: str, session_id: str) -> int:
        """Mark a session as currently active on a file.

        Used both on initial lock acquisition and on heartbeat refresh —
        either way we record "this session was alive at this time" and
        return the total number of active users.
        """
        with self._lock:
            self._evict_stale_sessions(file_path)

            if file_path not in self._sessions:
                self._sessions[file_path] = {}

            self._sessions[file_path][session_id] = time.time()
            return len(self._sessions[file_path])

    def user_left(self, file_path: str, session_id: str) -> Optional[int]:
        """Call this on explicit Unlock. Returns the remaining active users.

        Returns None if the session was not tracked for this file, so a stray
        or duplicate leave request cannot trigger an unlock of a file that
        other sessions still hold open.
        """
        with self._lock:
            self._evict_stale_sessions(file_path)

            if file_path not in self._sessions or session_id not in self._sessions[file_path]:
                return None

            del self._sessions[file_path][session_id]

            # Clean up if empty and return 0
            if not self._sessions[file_path]:
                del self._sessions[file_path]
                return 0

            return len(self._sessions[file_path])

    def sweep(self) -> tuple[dict[str, int], list[str]]:
        """Evict stale sessions everywhere.

        Returns (active, expired): paths that still have live sessions with
        their counts, and paths whose last session just went stale (whose
        locks should be released by the caller).
        """
        with self._lock:
            active = {}
            expired = []
            for file_path in list(self._sessions):
                self._evict_stale_sessions(file_path)
                if file_path in self._sessions:
                    active[file_path] = len(self._sessions[file_path])
                else:
                    expired.append(file_path)
            return active, expired


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
