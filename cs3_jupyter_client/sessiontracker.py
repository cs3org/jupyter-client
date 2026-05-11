import time
import threading

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

    def user_left(self, file_path: str, session_id: str) -> int:
        """Call this on explicit Unlock. Returns the remaining active users."""
        with self._lock:
            self._evict_stale_sessions(file_path)

            if file_path in self._sessions:
                if session_id in self._sessions[file_path]:
                    del self._sessions[file_path][session_id]

                # Clean up if empty and return 0
                if not self._sessions[file_path]:
                    del self._sessions[file_path]
                    return 0

                return len(self._sessions[file_path])
            return 0
