"""Sharing/users/storage endpoints, exercised through a real jupyter server."""

import json
import threading
from types import SimpleNamespace

import pytest
from google.protobuf.struct_pb2 import Struct
from tornado.httpclient import HTTPClientError

pytest_plugins = ["pytest_jupyter.jupyter_server"]


def message(**fields):
    """Any protobuf message will do: the handlers only MessageToDict them."""
    s = Struct()
    s.update(fields)
    return s


def user_share(opaque_id):
    return message(share={"grantee": {"type": "GRANTEE_TYPE_USER", "user_id": {"opaque_id": opaque_id}}})


class FakeSharingClient:
    """The extension's client: a session, and an object per CS3 service."""

    def __init__(self):
        self.threads = set()
        self.error = None
        self.session = SimpleNamespace(user_idp="idp", user_opaque_id="me")
        self.users = SimpleNamespace(
            find_users=self._rpc(lambda search, user_type=None: [message(username=search)]),
            get_user=self._rpc(lambda idp, user_id: message(display_name=f"User {user_id}")),
        )
        self.sharing = SimpleNamespace(
            list_existing_shares_by_creator=self._rpc(lambda idp, opaque_id: ([user_share("alice")], None)),
            list_existing_public_shares_by_creator=self._rpc(
                lambda idp, opaque_id: ([message(public_share={"id": {"opaque_id": "p1"}})], None)
            ),
            remove_share=self._rpc(lambda share_id: None),
        )

    def _rpc(self, result):
        """A fake RPC: records the calling thread, fails with self.error if set."""
        def call(*args, **kwargs):
            self.threads.add(threading.current_thread())
            if self.error:
                raise self.error
            return result(*args, **kwargs)
        return call


@pytest.fixture(autouse=True)
def offline_cs3(patch_cs3):
    """The extension builds its own CS3 client at load: keep it off the network."""


@pytest.fixture
def jp_server_config():
    return {"ServerApp": {"jpserver_extensions": {"cs3_jupyter.server_extension": True}}}


@pytest.fixture
def cs3(jp_serverapp):
    client = FakeSharingClient()
    jp_serverapp.web_app.settings["cs3_sharing_client"] = client
    return client


async def test_calls_run_off_the_event_loop(jp_fetch, cs3):
    r = await jp_fetch("find", "users", params={"search": "bob"})
    assert json.loads(r.body) == {"search": "bob", "items": [{"username": "bob"}]}
    assert threading.main_thread() not in cs3.threads


@pytest.mark.parametrize(
    "error, status",
    [(FileNotFoundError("gone"), 404), (PermissionError("no"), 403), (RuntimeError("boom"), 500)],
)
async def test_failures_keep_their_status_and_error_body(jp_fetch, cs3, error, status):
    cs3.error = error
    with pytest.raises(HTTPClientError) as exc:
        await jp_fetch("find", "users", params={"search": "bob"})
    assert exc.value.code == status
    assert json.loads(exc.value.response.body) == {"error": str(error)}


async def test_shared_by_me_attaches_grantee_info(jp_fetch, cs3):
    r = await jp_fetch("share", "getSharedByMe")
    data = json.loads(r.body)
    assert data["shares"][0]["grantee_user_info"] == {"display_name": "User alice"}
    assert data["public_shares"] == [{"public_share": {"id": {"opaque_id": "p1"}}}]


async def test_delete_share_is_204(jp_fetch, cs3):
    r = await jp_fetch("share", "share", method="DELETE", params={"share_id": "s1"})
    assert r.code == 204
