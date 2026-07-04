import time
import threading
from typing import Optional

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
