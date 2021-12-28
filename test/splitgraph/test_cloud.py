"""Test suite for the public cloud API clients"""
import base64
import json
import time
from unittest.mock import MagicMock, patch

import httpretty
import pytest
from requests import HTTPError

from splitgraph.cloud import RESTAPIClient
from splitgraph.config import create_config_dict
from splitgraph.exceptions import AuthAPIError

# Methods currently are small enough and all exercised in test_commandline_registration_*,
# so this is mostly testing various failure states.

_REMOTE = "remote_engine"
_ENDPOINT = "http://some-auth-service.example.com"


@httpretty.activate(allow_net_connect=False)
def test_auth_api_user_error():
    client = RESTAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        assert json.loads(request.body) == {"username": "someuser", "password": "somepassword"}
        return [400, response_headers, json.dumps({"error": "Invalid username or password"})]

    httpretty.register_uri(httpretty.HTTPretty.POST, _ENDPOINT + "/refresh_token", body=callback)

    with pytest.raises(AuthAPIError) as e:
        client.get_refresh_token("someuser", "somepassword")

    assert isinstance(e.value.__cause__, HTTPError)
    assert "400" in str(e.value.__cause__)


@httpretty.activate(allow_net_connect=False)
def test_auth_api_server_error_missing_entries():
    client = RESTAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        return [200, response_headers, json.dumps({})]

    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/create_machine_credentials", body=callback
    )

    with pytest.raises(AuthAPIError) as e:
        client.create_machine_credentials("AAABBBB", "somepassword")

    assert "Missing entries" in str(e.value)


@httpretty.activate(allow_net_connect=False)
def test_auth_api_server_error_no_json():
    client = RESTAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        return [500, response_headers, "Guru Meditation deadcafebeef-feed12345678"]

    httpretty.register_uri(httpretty.HTTPretty.POST, _ENDPOINT + "/refresh_token", body=callback)

    with pytest.raises(AuthAPIError) as e:
        client.get_refresh_token("someuser", "somepassword")

    assert "500 Server Error: Internal Server Error" in str(e.value.__cause__)


def test_auth_api_access_token_property_no_refresh():
    client = RESTAPIClient(_REMOTE)

    # Test that we raise an error if there's no refresh token in the config
    # Delete the API keys from the config (engine password) so that we don't try and
    # get an access token with those.
    config = create_config_dict()
    del config["remotes"][_REMOTE]["SG_ENGINE_USER"]
    del config["remotes"][_REMOTE]["SG_ENGINE_PWD"]

    with patch("splitgraph.cloud.create_config_dict", return_value=config), pytest.raises(
        AuthAPIError
    ) as e:
        client.access_token
    assert "No refresh token or API keys found in the config" in str(e.value)


def _make_dummy_access_token(exp):
    # Since the auth API doesn't verify the token on the client side
    # and only looks at the claims, only the claims part matters.
    claims = json.dumps({"exp": exp}).encode("utf-8")
    claims_enc = base64.urlsafe_b64encode(claims).decode("ascii").rstrip("=")
    return "DUMMYHEADER.%s.DUMMYSIG" % claims_enc


def test_auth_api_access_token_property_not_expired():
    client = RESTAPIClient(_REMOTE)

    now = time.time()

    token = _make_dummy_access_token(now + 1800)
    client.get_access_token = MagicMock(name="get_access_token")
    with patch(
        "splitgraph.cloud.create_config_dict",
        return_value={"remotes": {_REMOTE: {"SG_CLOUD_ACCESS_TOKEN": token}}},
    ):
        token = client.access_token
        assert client.get_access_token.call_count == 0


@httpretty.activate(allow_net_connect=False)
def test_auth_api_access_token_property_expired():
    client = RESTAPIClient(_REMOTE)

    # strictly speaking, we should use freezegun or patch time here,
    # but by default AuthClient is supposed to refresh the token 30s
    # before it actually expires.
    now = time.time()
    old_token = _make_dummy_access_token(now)
    new_token = _make_dummy_access_token(now + 1800)
    refresh_token = "EEEEFFFFGGGGHHHH"

    def callback(request, uri, response_headers):
        assert json.loads(request.body) == {"refresh_token": refresh_token}
        return [200, response_headers, json.dumps({"access_token": new_token})]

    httpretty.register_uri(httpretty.HTTPretty.POST, _ENDPOINT + "/access_token", body=callback)

    with patch(
        "splitgraph.cloud.create_config_dict",
        return_value={
            "remotes": {
                _REMOTE: {
                    "SG_CLOUD_ACCESS_TOKEN": old_token,
                    "SG_CLOUD_REFRESH_TOKEN": refresh_token,
                }
            },
            "SG_CONFIG_FILE": ".sgconfig",
        },
    ):
        with patch("splitgraph.cloud.overwrite_config") as oc:
            client.access_token

    oc.assert_called_once_with(
        {
            "remotes": {
                _REMOTE: {
                    "SG_CLOUD_ACCESS_TOKEN": new_token,
                    "SG_CLOUD_REFRESH_TOKEN": refresh_token,
                }
            },
            "SG_CONFIG_FILE": ".sgconfig",
        },
        ".sgconfig",
    )
