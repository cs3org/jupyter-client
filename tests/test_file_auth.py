import logging
from types import SimpleNamespace

import jwt
import pytest
from cs3.rpc.v1beta1.code_pb2 import CODE_OK
from cs3client.exceptions import SecretNotSetException

from cs3_jupyter.cs3mixin import FileAuth


def dummy_jwt(sub: str):
    return jwt.encode({"sub": sub, "exp": 2**31}, "a" * 32, algorithm="HS256")


def get_sub_from_jwt(token: str):
    return jwt.decode(token, algorithms=["HS256"], options={"verify_signature": False})[
        "sub"
    ]


class DummyGateway:
    def Authenticate(self, req):
        return SimpleNamespace(
            status=SimpleNamespace(code=CODE_OK), token=dummy_jwt(sub=req.client_secret)
        )


@pytest.fixture
def auth(tmp_path):
    gateway = DummyGateway()
    client = SimpleNamespace(
        _gateway=gateway,
        _log=logging.getLogger("test"),
        _config=SimpleNamespace(
            auth_client_secret=None, auth_client_id="user", auth_login_type="bearer"
        ),
    )
    return FileAuth(client, str(tmp_path / "oauth.token"))


def test_missing_file_at_construction_is_picked_up_later(auth):
    with pytest.raises(SecretNotSetException):
        auth.get_token()

    with open(auth._token_path, "w") as f:
        f.write("secret-1\n")

    token = auth.get_token()[1]
    assert get_sub_from_jwt(token) == "secret-1"


def test_rewritten_file_is_used_for_the_next_authentication(auth):
    with open(auth._token_path, "w") as f:
        f.write("secret-1")

    token = auth.get_token()[1]
    assert get_sub_from_jwt(token) == "secret-1"

    with open(auth._token_path, "w") as f:
        f.write("secret-2")

    # The file has changed, but the previous token is still valid,
    # only the client_secret should change:
    token = auth.get_token()[1]
    assert get_sub_from_jwt(token) == "secret-1"
    assert auth._client_secret == "secret-2"

    # Force token refresh
    auth.refresh()
    token = auth.get_token()[1]
    assert get_sub_from_jwt(token) == "secret-2"


def test_empty_file_is_not_cached(auth):
    # When the token file is updated while bein read concurrently it might appear
    # to be empty. In that case we should read the token file again.
    open(auth._token_path, "w").close()  # writer truncated, not yet written

    with pytest.raises(SecretNotSetException):
        auth.get_token()

    with open(auth._token_path, "w") as f:
        f.write("secret-1")

    token = auth.get_token()[1]
    assert get_sub_from_jwt(token) == "secret-1"
