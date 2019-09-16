"""Public API for interacting with the Splitgraph registry"""
from functools import wraps

import requests
from requests import HTTPError

from splitgraph import CONFIG
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
