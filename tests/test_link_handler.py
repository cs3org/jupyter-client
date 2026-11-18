"""Public link endpoints give the frontend the URL of each link."""

import json

import pytest
from cs3.gateway.v1beta1.resources_pb2 import PublicShareResourceInfo
from cs3.sharing.link.v1beta1.resources_pb2 import PublicShare
from cs3.storage.provider.v1beta1.resources_pb2 import ResourceInfo

pytest_plugins = ["pytest_jupyter.jupyter_server"]

URL = "https://cernbox.example.org/s/abc"


@pytest.fixture
def jp_server_config(patch_cs3):
    return {
        "ServerApp": {"jpserver_extensions": {"cs3_jupyter.server_extension": True}},
        "CS3Mixin": {"root_path": "/eos"},
    }


@pytest.fixture
def sharing(jp_serverapp):
    return jp_serverapp.web_app.settings["cs3_sharing_client"]


async def create_link(jp_fetch):
    r = await jp_fetch(
        "cs3", "shares", "link", method="POST", body=json.dumps({"role": "VIEWER"}), params={"path": "f.txt"}
    )
    return json.loads(r.body)


async def test_created_link_has_url(jp_fetch, sharing):
    sharing.create_public_share = lambda path, role, **kwargs: PublicShare(token="abc", url=URL)

    assert (await create_link(jp_fetch))["url"] == URL


async def test_link_without_url(jp_fetch, sharing):
    # Reva leaves the url empty when its public_url is not set.
    sharing.create_public_share = lambda path, role, **kwargs: PublicShare(token="abc")

    assert (await create_link(jp_fetch))["url"] == ""


async def test_listed_links_have_url(jp_fetch, sharing):
    link = PublicShareResourceInfo(
        public_share=PublicShare(token="abc", url=URL), resource_info=ResourceInfo(path="/eos/user/f.txt")
    )
    sharing.list_existing_shares_by_resource = lambda path: ([], None)
    sharing.list_existing_public_shares_by_resource = lambda path: ([link], None)

    r = await jp_fetch("cs3", "shares", "getSharedByResource", params={"path": "user/f.txt"})
    assert [s["url"] for s in json.loads(r.body)["public_shares"]] == [URL]
