"""Public API for interacting with the Splitgraph registry"""
import base64
import json
import time
from functools import wraps
from json import JSONDecodeError
from typing import Callable, List, Union, Tuple, cast, Optional

import requests
from requests import HTTPError
from requests.models import Response

from splitgraph.config import create_config_dict, get_singleton, CONFIG
from splitgraph.config.config import get_from_subsection, set_in_subsection
from splitgraph.config.export import overwrite_config
from splitgraph.exceptions import AuthAPIError


def expect_result(
    result: List[str], ignore_status_codes: Optional[List[int]] = None
) -> Callable[[Callable[..., Response]], Callable[..., Union[str, Tuple[str]]]]:
    """
    A decorator that can be wrapped around a function returning a requests.Response with a JSON body.
    If the request has failed, it will extract the "error" from the JSON response and raise an AuthAPIError.

    :param result: Items to extract. Will raise an AuthAPIError if not all items were fetched.
    :param ignore_status_codes: If one of these status codes is returned (e.g. 404),
        it gets ignored and a None is returned instead.
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

            if ignore_status_codes and response.status_code in ignore_status_codes:
                return None

            try:
                response.raise_for_status()
            except HTTPError as e:
                error = response.text
                raise AuthAPIError(error) from e

            if not result:
                return None
            try:
                json = response.json()
            except JSONDecodeError:
                raise AuthAPIError("Invalid response from service: %s" % response.text)
            missing = [f for f in result if f not in json]
            if missing:
                raise AuthAPIError("Missing entries %s in the response!" % (tuple(missing),))
            if len(result) == 1:
                return json[result[0]]
            return tuple(json[f] for f in result)

        return wrapped

    return decorator


def get_token_claim(jwt, claim):
    """Extract a claim from a JWT token without validating it."""
    # Directly decode the base64 claims part without pulling in any JWT libraries
    # (since we're not validating any signatures).

    claims = jwt.split(".")[1]
    # Pad the JWT claims because urlsafe_b64decode doesn't like us
    claims += "=" * (-len(claims) % 4)
    exp = json.loads(base64.urlsafe_b64decode(claims).decode("utf-8"))[claim]
    return exp


class AuthAPIClient:
    """
    Client for the Splitgraph registry auth API that generates tokens to access
    other Splitgraph services.

    Currently incomplete with just enough methods to allow to register and access
    the Splitgraph registry via the command line.
    """

    def __init__(self, remote: str) -> None:
        """
        :param remote: Name of the remote engine that this auth client communicates with,
            as specified in the config.
        """
        self.remote = remote
        self.endpoint = get_from_subsection(CONFIG, "remotes", remote, "SG_AUTH_API")

        # Allow overriding the CA bundle for test purposes (requests doesn't use the system
        # cert store)
        self.verify: Union[bool, str] = True
        try:
            self.verify = get_from_subsection(CONFIG, "remotes", remote, "SG_AUTH_API_CA_PATH")
        except KeyError:
            pass

        # How soon before the token expiry to refresh the token, in seconds.
        self.access_token_expiry_tolerance = 30

    @expect_result(["tos"])
    def tos(self) -> Response:
        """
        Get a Terms of Service message from the registry (if accepting ToS is required)
        :return: Link to the Terms of Service or None
        """
        return requests.get(self.endpoint + "/tos", verify=self.verify)

    @expect_result(["user_id"])
    def register(self, username: str, password: str, email: str, accept_tos: bool) -> Response:
        """
        Register a new Splitgraph user.

        :param username: Username
        :param password: Password
        :param email: Email
        :param accept_tos: Accept the Terms of Service if they exist
        """
        body = dict(username=username, password=password, email=email, accept_tos=accept_tos)
        return requests.post(self.endpoint + "/register_user", json=body, verify=self.verify)

    @expect_result(["access_token", "refresh_token"])
    def get_refresh_token(self, username: str, password: str) -> Response:
        """
        Get a long-lived refresh token and a short-lived access token from the API.

        :param username: Username
        :param password: Password
        :return: Tuple of (access_token, refresh_token).
        """
        body = dict(username=username, password=password)
        return requests.post(self.endpoint + "/refresh_token", json=body, verify=self.verify)

    @expect_result(["key", "secret"])
    def create_machine_credentials(self, access_token: str, password: str) -> Response:
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
            verify=self.verify,
        )

    @expect_result(["access_token"])
    def get_access_token(self, refresh_token: str) -> Response:
        """
        Get a new access token from a refresh token.

        :param refresh_token: Refresh token
        :return: New access token.
        """

        body = dict(refresh_token=refresh_token)
        return requests.post(self.endpoint + "/access_token", json=body, verify=self.verify)

    @expect_result(["access_token"])
    def get_access_token_from_api(self, api_key: str, api_secret: str) -> Response:
        """
        Get a new access token from API keys

        :param api_key: API key
        :param api_secret: API secret
        :return: New access token.
        """

        body = dict(api_key=api_key, api_secret=api_secret)
        return requests.post(self.endpoint + "/access_token", json=body, verify=self.verify)

    @property
    def access_token(self) -> str:
        """
        Will return an up-to-date access token by either getting it from
        the configuration file or contacting the auth service for a new one.
        Will write the new access token into the configuration file.

        :return: Access token.
        """

        config = create_config_dict()

        try:
            current_access_token = get_from_subsection(
                config, "remotes", self.remote, "SG_CLOUD_ACCESS_TOKEN"
            )
            exp = get_token_claim(current_access_token, "exp")
            now = time.time()
            if now < exp - self.access_token_expiry_tolerance:
                return current_access_token
        except KeyError:
            pass

        # Token expired or non-existent, get a new one.
        try:
            api_key = get_from_subsection(config, "remotes", self.remote, "SG_ENGINE_USER")
            api_secret = get_from_subsection(config, "remotes", self.remote, "SG_ENGINE_PWD")
            new_access_token = cast(str, self.get_access_token_from_api(api_key, api_secret))
        except KeyError as e:
            try:
                refresh_token = get_from_subsection(
                    config, "remotes", self.remote, "SG_CLOUD_REFRESH_TOKEN"
                )
                new_access_token = cast(str, self.get_access_token(refresh_token))
            except KeyError:
                raise AuthAPIError(
                    (
                        "No refresh token or API keys found in the config for remote %s! "
                        % self.remote
                    )
                    + "Log into the registry using sgr cloud login."
                ) from e

        set_in_subsection(config, "remotes", self.remote, "SG_CLOUD_ACCESS_TOKEN", new_access_token)
        overwrite_config(config, get_singleton(config, "SG_CONFIG_FILE"))
        return new_access_token
