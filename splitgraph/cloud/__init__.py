"""Public API for interacting with the Splitgraph registry"""
import base64
import json
import time
from functools import wraps

import requests
from requests import HTTPError

from splitgraph import CONFIG
from splitgraph.config import create_config_dict
from splitgraph.config.export import overwrite_config
from splitgraph.exceptions import AuthAPIError


def expect_result(result):
    """
    A decorator that can be wrapped around a function returning a requests.Response with a JSON body.
    If the request has failed, it will extract the "error" from the JSON response and raise an AuthAPIError.

    :param result: Items to extract. Will raise an AuthAPIError if not all items were fetched.
    :return: Tuple of items enumerated in the `result` list. If there's only one item, it will
        return just that item.
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                response = func(*args, **kwargs)
            except Exception as e:
                raise AuthAPIError from e

            try:
                response.raise_for_status()
            except HTTPError as e:
                error = response.json().get("error", "") or response.body
                if error:
                    raise AuthAPIError(error) from e
                else:
                    raise

            json = response.json()
            missing = [f for f in result if f not in json]
            if missing:
                raise AuthAPIError("Missing entries %s in the response!" % (tuple(missing),))
            if len(result) == 1:
                return json[result[0]]
            else:
                return tuple(json[f] for f in result)

        return wrapped

    return decorator


class AuthAPIClient:
    """
    Client for the Splitgraph registry auth API that generates tokens to access
    other Splitgraph services.

    Currently incomplete with just enough methods to allow to register and access
    the Splitgraph registry via the command line.
    """

    def __init__(self, remote):
        """
        :param remote: Name of the remote engine that this auth client communicates with,
            as specified in the config.
        """
        self.remote = remote
        self.endpoint = CONFIG["remotes"][remote]["SG_AUTH_API"]

    @expect_result(["user_uuid"])
    def register(self, username, password, email):
        """
        Register a new Splitgraph user.

        :param username: Username
        :param password: Password
        :param email: Email
        """
        body = dict(username=username, password=password, email=email)
        return requests.post(self.endpoint + "/register_user", json=body)

    @expect_result(["access_token", "refresh_token"])
    def get_refresh_token(self, username, password):
        """
        Get a long-lived refresh token and a short-lived access token from the API.

        :param username: Username
        :param password: Password
        :return: Tuple of (access_token, refresh_token).
        """
        body = dict(username=username, password=password)
        return requests.post(self.endpoint + "/refresh_token", json=body)

    @expect_result(["key", "secret"])
    def create_machine_credentials(self, access_token, password):
        """
        Generate a key and secret that can be used to log into the Splitgraph registry
        via a normal Postgres connection. The secret must be stored in the user's local
        configuration file (it's not stored on Splitgraph servers).

        :param access_token: Access token
        :param password: Password
        :return: Tuple of (key, secret).
        """
        body = dict(password=password)
        return requests.post(
            self.endpoint + "/create_machine_credentials",
            json=body,
            headers={"Authorization": "Bearer " + access_token},
        )

    @expect_result(["access_token"])
    def get_access_token(self, refresh_token):
        """
        Get a new access token from a refresh token.

        :param refresh_token: Refresh token
        :return: New access token.
        """

        body = dict(refresh_token=refresh_token)
        return requests.post(self.endpoint + "/access_token", json=body)

    @property
    def access_token(self, tolerance=30):
        """
        Will return an up-to-date access token by either getting it from
        the configuration file or contacting the auth service for a new one.
        Will write the new access token into the configuration file.

        :param tolerance: How soon before the token expiry to refresh the token, in seconds.
        :return: Access token.
        """

        config = create_config_dict()
        current_access_token = config["remotes"][self.remote].get("SG_CLOUD_ACCESS_TOKEN")

        if current_access_token:
            # Extract the expiry timestamp from the JWT token. We don't really
            # need to validate it here, so we can just directly decode the base64
            # claims part without pulling in any JWT libraries.
            claims = current_access_token.split(".")[1]

            # Pad the JWT claims because urlsafe_b64decode doesn't like us
            claims += "=" * (-len(claims) % 4)
            exp = json.loads(base64.urlsafe_b64decode(claims).decode("utf-8"))["exp"]
            now = time.time()
            if now < exp - tolerance:
                return current_access_token

        # Token expired or non-existent, get a new one.
        try:
            refresh_token = config["remotes"][self.remote]["SG_CLOUD_REFRESH_TOKEN"]
        except KeyError as e:
            raise AuthAPIError(
                "No refresh token found in the config for remote %s! " % self.remote
                + "Generate one and store it in the config for "
                "the remote under SG_CLOUD_REFRESH_TOKEN."
            ) from e

        new_access_token = self.get_access_token(refresh_token)
        config["remotes"][self.remote]["SG_CLOUD_ACCESS_TOKEN"] = new_access_token
        overwrite_config(config, config["SG_CONFIG_FILE"])
