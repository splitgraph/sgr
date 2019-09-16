"""Test suite for the public cloud API clients"""
import json

import httpretty
import pytest
from requests import HTTPError

from splitgraph.cloud import AuthAPIClient

from splitgraph.exceptions import AuthAPIError


# Methods currently are small enough and all exercised in test_commandline_registration_*,
# so this is mostly testing various failure states.

_REMOTE = "data.splitgraph.com"
_ENDPOINT = "data.splitgraph.com/auth"


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
