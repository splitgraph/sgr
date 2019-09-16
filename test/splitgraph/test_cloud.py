"""Test suite for the public cloud API clients"""
import base64
import json
import time
from unittest.mock import patch, MagicMock

import httpretty
import pytest
from requests import HTTPError

from splitgraph.cloud import AuthAPIClient

from splitgraph.exceptions import AuthAPIError


# Methods currently are small enough and all exercised in test_commandline_registration_*,
# so this is mostly testing various failure states.

_REMOTE = "remote_engine"
_ENDPOINT = "http://some-auth-service"


@httpretty.activate
def test_auth_api_user_error():
    client = AuthAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        assert json.loads(request.body) == {"username": "someuser", "password": "somepassword"}
        return [400, response_headers, json.dumps({"error": "Invalid username or password"})]

    httpretty.register_uri(httpretty.HTTPretty.POST, _ENDPOINT + "/refresh_token", body=callback)

    with pytest.raises(AuthAPIError) as e:
        client.get_refresh_token("someuser", "somepassword")

        assert isinstance(e.__cause__, HTTPError)
        assert "403" in str(e.__cause__)


@httpretty.activate
def test_auth_api_server_error_missing_entries():
    client = AuthAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        return [200, response_headers, json.dumps({})]

    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/create_machine_credentials", body=callback
    )

    with pytest.raises(AuthAPIError) as e:
        client.create_machine_credentials("AAABBBB", "somepassword")

        assert "Missing entries" in str(e)


@httpretty.activate
def test_auth_api_server_error_no_json():
    client = AuthAPIClient(_REMOTE)

    def callback(request, uri, response_headers):
        return [500, response_headers, "Guru Meditation deadcafebeef-feed12345678"]

    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/get_refresh_token", body=callback
    )

    with pytest.raises(AuthAPIError) as e:
        client.get_refresh_token("someuser", "somepassword")

        assert "deadcafebeef-feed12345678" in str(e)


def test_auth_api_access_token_property_no_refresh():
    client = AuthAPIClient(_REMOTE)

    # Test that we raise an error if there's no refresh token in the config
    # (there won't be if we just use the default config).

    with pytest.raises(AuthAPIError) as e:
        token = client.access_token
        assert "No refresh token found in the config" in str(e)


def _make_dummy_access_token(exp):
    # Since the auth API doesn't verify the token on the client side
    # and only looks at the claims, only the claims part matters.
    claims = json.dumps({"exp": exp}).encode("utf-8")
    claims_enc = base64.urlsafe_b64encode(claims).decode("ascii").rstrip("=")
    return "DUMMYHEADER.%s.DUMMYSIG" % claims_enc


def test_auth_api_access_token_property_not_expired():
    client = AuthAPIClient(_REMOTE)

    now = time.time()

    token = _make_dummy_access_token(now + 1800)
    client.get_access_token = MagicMock(name="get_access_token")
    with patch(
        "splitgraph.cloud.create_config_dict",
        return_value={"remotes": {_REMOTE: {"SG_CLOUD_ACCESS_TOKEN": token}}},
    ):
        token = client.access_token
        assert client.get_access_token.call_count == 0


@httpretty.activate
def test_auth_api_access_token_property_expired():
    client = AuthAPIClient(_REMOTE)

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
            token = client.access_token

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
