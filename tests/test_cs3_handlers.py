"""Sharing/users/storage endpoints, exercised through a real jupyter server."""

import json
import threading

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
    user_idp = "idp"
    user_opaque_id = "me"

    def __init__(self):
        self.threads = set()
        self.error = None

    def _called(self):
        self.threads.add(threading.current_thread())
        if self.error:
            raise self.error

    def find_users(self, search, user_type=None):
        self._called()
        return [message(username=search)]

    def get_user(self, idp, user_id):
        self._called()
        return message(display_name=f"User {user_id}")

    def list_existing_shares_by_creator(self, idp, opaque_id):
        self._called()
        return [user_share("alice")], None

    def list_existing_public_shares_by_creator(self, idp, opaque_id):
        self._called()
        return [message(public_share={"id": {"opaque_id": "p1"}})], None

    def remove_share(self, share_id):
        self._called()


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
