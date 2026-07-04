import time

from cs3_jupyter_client.sessiontracker import SessionTracker


def test_user_seen_counts_distinct_sessions():
    t = SessionTracker(heartbeat_timeout_seconds=60)
    assert t.user_seen("/f", "a") == 1
    assert t.user_seen("/f", "b") == 2
    # heartbeat of an existing session doesn't inflate the count
    assert t.user_seen("/f", "a") == 2


def test_user_left_counts_down_to_zero():
    t = SessionTracker(heartbeat_timeout_seconds=60)
    t.user_seen("/f", "a")
    t.user_seen("/f", "b")
    assert t.user_left("/f", "a") == 1
    assert t.user_left("/f", "b") == 0


def test_user_left_unknown_session_returns_none():
    t = SessionTracker(heartbeat_timeout_seconds=60)
    t.user_seen("/f", "a")
    # unknown session id, unknown path, and double-leave must not report 0
    assert t.user_left("/f", "bogus") is None
    assert t.user_left("/other", "a") is None
    assert t.user_left("/f", "a") == 0
    assert t.user_left("/f", "a") is None


def test_stale_sessions_evicted_on_access():
    t = SessionTracker(heartbeat_timeout_seconds=0.01)
    t.user_seen("/f", "a")
    time.sleep(0.02)
    assert t.user_seen("/f", "b") == 1  # "a" evicted


def test_sweep_reports_active_and_expired():
    t = SessionTracker(heartbeat_timeout_seconds=0.05)
    t.user_seen("/stale", "a")
    time.sleep(0.06)
    t.user_seen("/live", "b")
    active, expired = t.sweep()
    assert active == {"/live": 1}
    assert expired == ["/stale"]
    # a second sweep no longer reports the already-forgotten path
    active, expired = t.sweep()
    assert active == {"/live": 1}
    assert expired == []
