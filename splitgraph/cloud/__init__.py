"""Public API for interacting with the Splitgraph registry"""
import base64
import json
import logging
import os
import time
import warnings
from functools import wraps
from json import JSONDecodeError
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import requests
from pydantic import BaseModel
from requests import HTTPError
from requests.models import Response
from splitgraph.__version__ import __version__
from splitgraph.cloud.models import (
    AddExternalCredentialRequest,
    AddExternalRepositoriesRequest,
    AddExternalRepositoryRequest,
    Credential,
    External,
    ExternalResponse,
    ListExternalCredentialsResponse,
    Metadata,
    MetadataResponse,
    Repository,
    UpdateExternalCredentialRequest,
    UpdateExternalCredentialResponse,
    make_repositories,
)
from splitgraph.config import CONFIG, create_config_dict, get_singleton
from splitgraph.config.config import (
    get_all_in_subsection,
    get_from_subsection,
    set_in_subsection,
)
from splitgraph.config.export import overwrite_config
from splitgraph.config.management import patch_and_save_config
from splitgraph.exceptions import (
    AuthAPIError,
    GQLAPIError,
    GQLRepoDoesntExistError,
    GQLUnauthenticatedError,
    GQLUnauthorizedError,
)


def get_headers():
    return {"User-Agent": "sgr %s" % __version__}


DEFAULT_REMOTES = {
    "data.splitgraph.com": {
        "SG_IS_REGISTRY": "true",
        "SG_ENGINE_HOST": "data.splitgraph.com",
        "SG_ENGINE_PORT": "5432",
        "SG_ENGINE_DB_NAME": "sgregistry",
        "SG_AUTH_API": "https://api.splitgraph.com/auth",
        "SG_QUERY_API": "https://data.splitgraph.com",
        "SG_GQL_API": "https://api.splitgraph.com/gql/cloud/graphql",
    }
}

_BULK_UPSERT_REPO_PROFILES_QUERY = """mutation BulkUpsertRepoProfilesMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $descriptions: [String]
  $readmes: [String]
  $licenses: [String]
  $metadata: [JSON]
) {
  __typename
  bulkUpsertRepoProfiles(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      descriptions: $descriptions
      readmes: $readmes
      licenses: $licenses
      metadata: $metadata
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

_BULK_UPDATE_REPO_SOURCES_QUERY = """mutation BulkUpdateRepoSourcesMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $sources: [DatasetSourceInput]
) {
  __typename
  bulkUpdateRepoSources(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      sources: $sources
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

_BULK_UPSERT_REPO_TOPICS_QUERY = """mutation BulkUpsertRepoTopicsMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $topics: [String]
) {
  __typename
  bulkUpsertRepoTopics(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      topics: $topics
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

_PROFILE_UPSERT_QUERY = """mutation UpsertRepoProfile(
  $namespace: String!
  $repository: String!
  $description: String
  $readme: String
  $topics: [String]
  $sources: [DatasetSourceInput]
  $license: String
  $metadata: JSON
) {
  __typename
  upsertRepoProfileByNamespaceAndRepository(
    input: {
      repoProfile: {
        namespace: $namespace
        repository: $repository
        description: $description
        readme: $readme
        sources: $sources
        license: $license
        metadata: $metadata
      }
      patch: {
        namespace: $namespace
        repository: $repository
        description: $description
        readme: $readme
        sources: $sources
        license: $license
        metadata: $metadata
      }
    }
  ) {
    clientMutationId
    __typename
  }
  __typename
  createRepoTopicsAgg(
  input: {
    repoTopicsAgg: {
      namespace: $namespace
      repository: $repository
      topics: $topics
    }
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

_FIND_REPO_QUERY = """query FindRepositories($query: String!, $limit: Int!) {
  findRepository(query: $query, first: $limit) {
    edges {
      node {
        namespace
        repository
        highlight
      }
    }
    totalCount
  }
}"""

_REPO_PARAMS = "($namespace: String!, $repository: String!)"
_REPO_CONDITIONS = ", condition: {namespace: $namespace, repository: $repository}"

_GET_REPO_METADATA_QUERY = """query GetRepositoryMetadata%s {
  repositories(first: 1000%s) {
    nodes {
      namespace
      repository
      repoTopicsByNamespaceAndRepository {
        nodes {
          topic
        }
      }
      repoProfileByNamespaceAndRepository {
        description
        license
        metadata
        readme
        sources {
          anchor
          href
          isCreator
          isSameAs
        }
      }
    }
  }
}"""

_GET_REPO_SOURCE_QUERY = """query GetRepositoryDataSource%s {
  repositoryDataSources(first: 1000%s) {
    nodes {
      namespace
      repository
      credentialId
      dataSource
      params
      tableParams
      externalImageByNamespaceAndRepository {
        imageByNamespaceAndRepositoryAndImageHash {
          tablesByNamespaceAndRepositoryAndImageHash {
            nodes {
              tableName
              tableSchema
            }
          }
        }
      }
    }
  }
}"""


def get_remote_param(remote: str, key: str) -> str:
    return str(
        get_all_in_subsection(CONFIG, "remotes", remote).get(key) or DEFAULT_REMOTES[remote][key]
    )


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


def _handle_gql_errors(response):
    logging.debug("GQL API status: %d, response: %s", response.status_code, response.text)
    # GQL itself doesn't seem to return non-200 codes, so this catches HTTP-level errors.
    response.raise_for_status()
    response_j = response.json()
    if "errors" in response_j:
        message = response_j["errors"][0]["message"]
        if "new row violates row-level security policy for table" in message:
            raise GQLUnauthorizedError("You do not have write access to this repository!")
        elif "Invalid token" in message:
            raise GQLUnauthenticatedError("Your access token doesn't exist or has expired!")
        elif "violates foreign key constraint" in message:
            raise GQLRepoDoesntExistError("Unknown repository!")
        else:
            raise GQLAPIError(message)


def handle_gql_errors(func: Callable[..., Response]) -> Callable[..., Response]:
    """
    A decorator that handles responses from the GQL API, transforming errors into exceptions.
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        response = func(*args, **kwargs)

        _handle_gql_errors(response)

        return response

    return wrapped


def get_token_claim(jwt, claim):
    """Extract a claim from a JWT token without validating it."""
    # Directly decode the base64 claims part without pulling in any JWT libraries
    # (since we're not validating any signatures).

    claims = jwt.split(".")[1]
    # Pad the JWT claims because urlsafe_b64decode doesn't like us
    claims += "=" * (-len(claims) % 4)
    exp = json.loads(base64.urlsafe_b64decode(claims).decode("utf-8"))[claim]
    return exp


T = TypeVar("T", bound=BaseModel)


class RESTAPIClient:
    """
    Client for various Splitgraph Registry REST APIs: auth token generation, external repo setup...
    """

    def __init__(self, remote: str) -> None:
        """
        :param remote: Name of the remote engine that this auth client communicates with,
            as specified in the config.
        """
        self.remote = remote
        self.endpoint = get_remote_param(remote, "SG_AUTH_API")
        self.externals_endpoint = get_remote_param(remote, "SG_QUERY_API") + "/api/external"

        # Allow overriding the CA bundle for test purposes (requests doesn't use the system
        # cert store)
        self.verify: Union[bool, str] = True
        try:
            self.verify = get_remote_param(remote, "SG_AUTH_API_CA_PATH")
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
        return requests.get(self.endpoint + "/tos", verify=self.verify, headers=get_headers())

    @expect_result(["user_id", "access_token", "refresh_token"])
    def register(self, username: str, password: str, email: str, accept_tos: bool) -> Response:
        """
        Register a new Splitgraph user.

        :param username: Username
        :param password: Password
        :param email: Email
        :param accept_tos: Accept the Terms of Service if they exist
        """
        body = dict(username=username, password=password, email=email, accept_tos=accept_tos)

        headers = get_headers()
        try:
            headers["Authorization"] = "Bearer " + self.access_token
        except AuthAPIError:
            # We can optionally pass an access token for logged-in admin users to make new users
            # on the registry if new signups are disabled, but it will be missing in most cases
            # (since the user is registering anew)
            pass
        return requests.post(
            self.endpoint + "/register_user", json=body, verify=self.verify, headers=headers
        )

    @expect_result(["access_token", "refresh_token"])
    def get_refresh_token(self, username: str, password: str) -> Response:
        """
        Get a long-lived refresh token and a short-lived access token from the API.

        :param username: Username
        :param password: Password
        :return: Tuple of (access_token, refresh_token).
        """
        body = dict(username=username, password=password)
        return requests.post(
            self.endpoint + "/refresh_token", json=body, verify=self.verify, headers=get_headers()
        )

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
            headers={**get_headers(), **{"Authorization": "Bearer " + access_token}},
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
        return requests.post(
            self.endpoint + "/access_token", json=body, verify=self.verify, headers=get_headers()
        )

    @expect_result(["access_token"])
    def get_access_token_from_api(self, api_key: str, api_secret: str) -> Response:
        """
        Get a new access token from API keys

        :param api_key: API key
        :param api_secret: API secret
        :return: New access token.
        """

        body = dict(api_key=api_key, api_secret=api_secret)
        return requests.post(
            self.endpoint + "/access_token", json=body, verify=self.verify, headers=get_headers()
        )

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

    def get_latest_version(self) -> Optional[str]:
        # Do a version check to see if updates are available. If the user is logged
        # into the registry, also send the user ID for metrics.
        # The user can opt out by setting "SG_UPDATE_FREQUENCY" to 0 or opt out of
        # sending user ID by setting SG_UPDATE_ANONYMOUS to true.

        config = create_config_dict()
        frequency = int(get_singleton(config, "SG_UPDATE_FREQUENCY"))

        if frequency == 0:
            return None

        last_check = int(get_singleton(config, "SG_UPDATE_LAST"))
        now = int(time.time())

        if last_check + frequency > now:
            return None

        headers = get_headers()
        if get_singleton(config, "SG_UPDATE_ANONYMOUS").lower() == "false":
            try:
                headers.update({"Authorization": "Bearer " + self.access_token})
            except AuthAPIError:
                pass

        try:
            logging.debug("Running update check")
            response = requests.post(
                self.endpoint + "/update_check",
                verify=self.verify,
                headers=headers,
            )
            response.raise_for_status()
            latest_version = str(response.json()["latest_version"])
        except requests.RequestException as e:
            logging.debug("Error running the update check", exc_info=e)
            return None
        except KeyError:
            logging.debug("Malformed response from the update service")
            return None

        try:
            patch_and_save_config(config, {"SG_UPDATE_LAST": str(now)})
        except Exception as e:
            logging.debug("Error patching the config", exc_info=e)
            return latest_version

        return latest_version

    def _perform_request(
        self,
        route,
        access_token,
        request: Optional[BaseModel] = None,
        response_class: Optional[Type[T]] = None,
        endpoint=None,
        method: str = "post",
    ) -> Optional[T]:
        endpoint = endpoint or self.endpoint
        response = requests.request(
            method,
            endpoint + route,
            headers={**get_headers(), **{"Authorization": "Bearer " + access_token}},
            verify=self.verify,
            data=request.json(by_alias=True, exclude_unset=True) if request else None,
        )
        logging.debug(response.text)
        response.raise_for_status()

        if response_class:
            return response_class.parse_obj(response.json())
        return None

    def list_external_credentials(self) -> ListExternalCredentialsResponse:
        response = self._perform_request(
            "/list_external_credentials",
            self.access_token,
            None,
            ListExternalCredentialsResponse,
            method="get",
        )
        assert response
        return response

    def ensure_external_credential(
        self, credential_data: Dict[str, Any], credential_name: str, plugin_name: str
    ) -> str:
        """Store a credential for accessing an external data source
        in Splitgraph Cloud and return its ID"""
        access_token = self.access_token

        credentials = self.list_external_credentials()

        for credential in credentials.credentials:
            if (
                credential.plugin_name == plugin_name
                and credential.credential_name == credential_name
            ):
                self._perform_request(
                    "/update_external_credential",
                    access_token,
                    UpdateExternalCredentialRequest(
                        credential_id=credential.credential_id,
                        credential_name=credential_name,
                        credential_data=credential_data,
                        plugin_name=plugin_name,
                    ),
                )
                return credential.credential_id

        # Credential doesn't exist: create it.
        credential = self._perform_request(
            "/add_external_credential",
            access_token,
            AddExternalCredentialRequest(
                credential_name=credential_name,
                credential_data=credential_data,
                plugin_name=plugin_name,
            ),
            UpdateExternalCredentialResponse,
        )
        assert credential
        return credential.credential_id

    def bulk_upsert_external(self, repositories: List[AddExternalRepositoryRequest]):
        request = AddExternalRepositoriesRequest(repositories=repositories)
        self._perform_request(
            "/bulk-add", self.access_token, request, endpoint=self.externals_endpoint
        )


def AuthAPIClient(*args, **kwargs):
    warnings.warn("AuthAPIClient is deprecated; use RESTAPIClient", DeprecationWarning, 2)
    return RESTAPIClient(*args, **kwargs)


class GQLAPIClient:
    """Wrapper class for select Splitgraph Registry GQL operations that can be
    called from the CLI"""

    def __init__(
        self,
        remote: Optional[str],
        endpoint: Optional[str] = None,
        access_token: Optional[str] = None,
    ):
        if not remote and not endpoint:
            raise ValueError(
                "GQLAPIClient must be initialized with either a remote or an endpoint!"
            )

        if remote:
            self.endpoint = get_remote_param(remote, "SG_GQL_API")
            self._auth_client: Optional[RESTAPIClient] = RESTAPIClient(remote)
            self._access_token: Optional[str] = access_token
        elif endpoint:
            self.endpoint = endpoint
            self._auth_client = None
            self._access_token = access_token

        self.registry_endpoint = self.endpoint.replace("/cloud/", "/registry/")  # :eyes:

    @property
    def access_token(self) -> Optional[str]:
        if self._auth_client:
            return self._auth_client.access_token
        else:
            return self._access_token

    def _gql(self, query: Dict, endpoint=None, handle_errors=False) -> requests.Response:
        endpoint = endpoint or self.endpoint
        access_token = self.access_token
        headers = get_headers()
        if access_token:
            headers.update({"Authorization": "Bearer " + access_token})

        result = requests.post(
            endpoint, headers=headers, json=query, verify=os.environ.get("SSL_CERT_FILE", True)
        )
        if handle_errors:
            _handle_gql_errors(result)
        return result

    @staticmethod
    def _validate_metadata(namespace: str, repository: str, metadata: Metadata):
        # Pre-flight validation
        if metadata.description and len(metadata.description) > 160:
            raise ValueError("The description should be 160 characters or shorter!")

        variables: Dict[str, Any] = {"namespace": namespace, "repository": repository}

        variables.update(metadata.dict(by_alias=True, exclude_unset=True))

        variables = {k: v for k, v in variables.items() if v is not None}
        if "extra_metadata" in variables:
            extra_metadata = variables.pop("extra_metadata")

            # This is a bit of a hack. The actual metadata field in the repository is a JSON with
            # three toplevel fields:
            #   * created_at
            #   * updated_at
            #   * upstream_metadata
            #
            # The former two are used to populate schema.org specifications and override the
            # Updated At field on the repository page. The latter is used to render the
            # semi-structured box at the bottom with double nesting and arbitrary metadata.
            # We don't necessarily want to keep it that way (it was designed to support cataloguing
            # upstream open datasets), but want to let users set these anyway. Here, we
            # pluck out the two special "created_at"/"updated_at" fields into the toplevel
            # and put the rest of the metadata dict into "upstream_metadata" to get it rendering.
            metadata_doc: Dict[str, Any] = {}
            for toplevel_key in ["created_at", "updated_at"]:
                if toplevel_key in extra_metadata:
                    metadata_doc[toplevel_key] = str(extra_metadata.pop(toplevel_key))
            metadata_doc["upstream_metadata"] = extra_metadata
            variables["metadata"] = metadata_doc

        if "readme" in variables and isinstance(variables["readme"], dict):
            variables["readme"] = variables["readme"]["text"]

        return variables

    @staticmethod
    def _prepare_upsert_metadata_gql(namespace: str, repository: str, metadata: Metadata, v1=False):
        variables = GQLAPIClient._validate_metadata(namespace, repository, metadata)

        gql_query = _PROFILE_UPSERT_QUERY
        if v1:
            gql_query = gql_query.replace("createRepoTopicsAgg", "createRepoTopic").replace(
                "repoTopicsAgg", "repoTopic"
            )

        query = {
            "operationName": "UpsertRepoProfile",
            "variables": variables,
            "query": gql_query,
        }

        logging.debug("Prepared GraphQL query: %s", json.dumps(query))

        return query

    @handle_gql_errors
    def upsert_metadata(self, namespace: str, repository: str, metadata: Metadata):
        """
        Update metadata for a single repository.
        """
        response = self._gql(self._prepare_upsert_metadata_gql(namespace, repository, metadata))
        return response

    def bulk_upsert_metadata(
        self, namespace_list: List[str], repository_list: List[str], metadata_list: List[Metadata]
    ):
        repo_profiles: Dict[str, List[Any]] = dict(
            namespaces=namespace_list,
            repositories=repository_list,
            descriptions=[],
            readmes=[],
            licenses=[],
            metadata=[],
        )
        repo_sources: Dict[str, List[Any]] = dict(namespaces=[], repositories=[], sources=[])
        repo_topics: Dict[str, List[str]] = dict(namespaces=[], repositories=[], topics=[])

        # populate mutation payloads
        for ind, metadata in enumerate(metadata_list):
            validated_metadata = GQLAPIClient._validate_metadata(
                namespace_list[ind], repository_list[ind], metadata
            )

            repo_profiles["descriptions"].append(validated_metadata.get("description"))
            repo_profiles["readmes"].append(validated_metadata.get("readme"))
            repo_profiles["licenses"].append(validated_metadata.get("license"))
            repo_profiles["metadata"].append(validated_metadata.get("metadata"))

            # flatten sources, which will be aggregated on the server side
            if len(validated_metadata.get("sources", [])) > 0:
                for source in validated_metadata["sources"]:
                    repo_sources["namespaces"].append(namespace_list[ind])
                    repo_sources["repositories"].append(repository_list[ind])
                    repo_sources["sources"].append(source)

            # flatten topics, which will be aggregated on the server side
            if len(validated_metadata.get("topics", [])) > 0:
                for topic in validated_metadata["topics"]:
                    repo_topics["namespaces"].append(namespace_list[ind])
                    repo_topics["repositories"].append(repository_list[ind])
                    repo_topics["topics"].append(topic)

        self._bulk_upsert_repo_profiles(repo_profiles)
        self._bulk_upsert_repo_sources(repo_sources)
        self._bulk_upsert_repo_topics(repo_topics)

    @handle_gql_errors
    def _bulk_upsert_repo_profiles(self, repo_profiles: Dict[str, List[Any]]):
        repo_profiles_query = {
            "operationName": "BulkUpsertRepoProfilesMutation",
            "variables": repo_profiles,
            "query": _BULK_UPSERT_REPO_PROFILES_QUERY,
        }
        response = self._gql(repo_profiles_query)
        return response

    @handle_gql_errors
    def _bulk_upsert_repo_sources(self, repo_sources: Dict[str, List[Any]]):
        repo_sources_query = {
            "operationName": "BulkUpdateRepoSourcesMutation",
            "variables": repo_sources,
            "query": _BULK_UPDATE_REPO_SOURCES_QUERY,
        }
        response = self._gql(repo_sources_query)
        return response

    @handle_gql_errors
    def _bulk_upsert_repo_topics(self, repo_topics: Dict[str, List[str]]):
        repo_topics_query = {
            "operationName": "BulkUpsertRepoTopicsMutation",
            "variables": repo_topics,
            "query": _BULK_UPSERT_REPO_TOPICS_QUERY,
        }
        response = self._gql(repo_topics_query)
        return response

    def upsert_readme(self, namespace: str, repository: str, readme: str):
        return self.upsert_metadata(namespace, repository, Metadata(readme=readme))

    @handle_gql_errors
    def upsert_description(self, namespace: str, repository: str, description: str):
        return self.upsert_metadata(namespace, repository, Metadata(description=description))

    @handle_gql_errors
    def upsert_topics(self, namespace: str, repository: str, topics: List[str]):
        return self.upsert_metadata(namespace, repository, Metadata(topics=topics))

    def find_repository(
        self, query: str, limit: int = 10
    ) -> Tuple[int, List[Tuple[str, str, str]]]:
        response = self._gql(
            {
                "operationName": "FindRepositories",
                "variables": {"query": query, "limit": limit},
                "query": _FIND_REPO_QUERY,
            }
        )

        _handle_gql_errors(response)
        result = response.json()

        # Extract data from the response
        find_repository = result["data"]["findRepository"]
        total_count = find_repository["totalCount"]
        repos_previews = [
            (r["node"]["namespace"], r["node"]["repository"], r["node"]["highlight"])
            for r in find_repository["edges"]
        ]

        return total_count, repos_previews

    def get_metadata(self, namespace: str, repository: str) -> Optional[MetadataResponse]:
        response = self._gql(
            {
                "query": _GET_REPO_METADATA_QUERY % (_REPO_PARAMS, _REPO_CONDITIONS),
                "operationName": "GetRepositoryMetadata",
                "variables": {"namespace": namespace, "repository": repository},
            },
            handle_errors=True,
        )

        parsed_responses = MetadataResponse.from_response(response.json())
        if parsed_responses:
            assert len(parsed_responses) == 1
            return parsed_responses[0]
        return None

    def get_external_metadata(self, namespace: str, repository: str) -> Optional[ExternalResponse]:
        response = self._gql(
            {
                "query": _GET_REPO_SOURCE_QUERY % (_REPO_PARAMS, _REPO_CONDITIONS),
                "operationName": "GetRepositoryDataSource",
                "variables": {"namespace": namespace, "repository": repository},
            },
            endpoint=self.registry_endpoint,
            handle_errors=True,
        )

        parsed_responses = ExternalResponse.from_response(response.json())
        if parsed_responses:
            assert len(parsed_responses) == 1
            return parsed_responses[0]
        return None

    def load_all_repositories(self, limit_to: List[str] = None) -> List[Repository]:
        from splitgraph.core.repository import Repository as SGRepository

        if limit_to:
            parsed_metadata = []
            parsed_external = []

            for repository in limit_to:
                # We currently can't filter on multiple fields in GQL, so we loop instead.
                repo_obj = SGRepository.from_schema(repository)
                metadata = self.get_metadata(repo_obj.namespace, repo_obj.repository)
                external = self.get_external_metadata(repo_obj.namespace, repo_obj.repository)

                if metadata:
                    parsed_metadata.append(metadata)
                if external:
                    parsed_external.append(external)
        else:
            metadata_r = self._gql(
                {
                    "query": _GET_REPO_METADATA_QUERY % ("", ""),
                    "operationName": "GetRepositoryMetadata",
                },
                handle_errors=True,
            )

            external_r = self._gql(
                {
                    "query": _GET_REPO_SOURCE_QUERY % ("", ""),
                    "operationName": "GetRepositoryDataSource",
                },
                endpoint=self.registry_endpoint,
                handle_errors=True,
            )
            parsed_metadata = MetadataResponse.from_response(metadata_r.json())
            parsed_external = ExternalResponse.from_response(external_r.json())

        return make_repositories(parsed_metadata, parsed_external)
