from __future__ import annotations
from typing import Any, Optional
from google.protobuf.json_format import MessageToDict

# Process-wide user info cache (per server process).
# These Jupyter server environments are short-lived, and the set of users is bounded,
# so it's acceptable to keep an unbounded cache for the lifetime of the process.
_user_info_cache: dict[tuple[str, str], dict[str, Any]] = {}


def cache_get_user_info(cm, user_id: str, log=None) -> Optional[dict[str, Any]]:
    """Get user info from an in-memory process-wide cache."""
    key = (cm.user_idp, user_id)

    cached = _user_info_cache.get(key)
    if cached is not None:
        return cached

    try:
        user_info = cm.get_user(idp=cm.user_idp, user_id=user_id)
        data = MessageToDict(user_info, preserving_proto_field_name=True)
        _user_info_cache[key] = data
        return data
    except Exception as e:
        if log is not None:
            log.error(f"Error retrieving user info for user {user_id}: {e}")
        return None
