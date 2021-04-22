import json

from splitgraph.cloud import _PROFILE_UPSERT_QUERY

REMOTE = "remote_engine"
AUTH_ENDPOINT = "http://some-auth-service.example.com"
GQL_ENDPOINT = "http://some-gql-service.example.com"
QUERY_ENDPOINT = "http://some-query-service.example.com"

ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1ODA1OTQyMzQsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImV4cCI6MTU4MDU5NzgzNCwidXNlcl9pZCI6IjEyM2U0NTY3LWU4OWItMTJkMy1hNDU2LTQyNjY1NTQ0MDAwMCIsImdyYW50IjoiYWNjZXNzIiwidXNlcm5hbWUiOiJzb21ldXNlciIsImlhdCI6MTU4MDU5NDIzNH0.YEuNhqKfFoxHloohfxInSEV9rnivXcF9SvFP72Vv1mDDsaqlRqCjKYM4S7tdSMap5__e3_UTwE_CpH8eI7DdePjMu8AOFXwFHPl34AAxZgavP4Mly0a0vrMsxNJ4KbtmL5-7ih3uneTEuZLt9zQLUh-Bi_UYlEYwGl8xgz5dDZ1YlwTEMsqSrDnXdjl69CTk3vVHIQdxtki4Ng7dZhbOnEdJIRsZi9_VdMlsg2TIU-0FsU2bYYBWktms5hyAAH0RkHYfvjGwIRirSEjxTpO9vci-eAsF8C4ohTUg6tajOcyWz8d7JSaJv_NjLFMZI9mC09hchbQZkw-37CdbS_8Yvw"
REFRESH_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NzYzMTk5MTYsImlhdCI6MTU3NjMxOTkxNiwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicmVmcmVzaF90b2tlbl9zZWNyZXQiOiJzb21lc2VjcmV0IiwiZXhwIjoxNTc4OTExOTE2LCJ1c2VyX2lkIjoiMTIzZTQ1NjctZTg5Yi0xMmQzLWE0NTYtNDI2NjU1NDQwMDAwIiwidXNlcm5hbWUiOiJzb21ldXNlciIsInJlZnJlc2hfdG9rZW5fa2V5Ijoic29tZWtleSIsImdyYW50IjoicmVmcmVzaCJ9.lO3nN3Tmu3twwUjrWsVpBq7nHHEvLnOGXeMkXXv4PRBADUAHyhmmaIPzgccq9XlwpLIexBAxTKJ4GaxSQufKUVLbzAKIMHqxiGTzELY6JMyUvMDHKeKNsq6FdhHxXoKa96fHaDDa65eGcSRSKS3Yr-9sBiANMBJGRbwypYw41gf61pewMA8TXqBmA-mvsBzMUaQNz1DfjkkpHs4SCERPK0GhYSJwDAwK8U3wG47S9k-CQqpq2B99yRRrdSVRzA_lcKe7GlF-Pw6hbRR7xBPBtX61pPME5hFUCPcwYWYXa_KhqEx9IF9edt9UahZuBudaVLmTdKKWgE9M53jQofxNzg"
API_KEY = "abcdef123456"
API_SECRET = "654321fedcba"
REMOTE_CONFIG = {
    "SG_ENGINE_USER": API_KEY,
    "SG_ENGINE_PWD": API_SECRET,
    "SG_NAMESPACE": "someuser",
    "SG_CLOUD_REFRESH_TOKEN": REFRESH_TOKEN,
    "SG_CLOUD_ACCESS_TOKEN": ACCESS_TOKEN,
    "SG_IS_REGISTRY": "true",
}

INVALID_METADATA = """
what_is_this_key: no_value
"""
VALID_METADATA = """
readme: {}
description: Description for a sample repo
extra_keys: are ok for now
topics:
  - topic_1
  - topic_2
sources:
  - anchor: Creator of the dataset
    href: https://www.splitgraph.com
    isCreator: true
    isSameAs: false
  - anchor: Source 2
    href: https://www.splitgraph.com
    isCreator: false
    isSameAs: true
license: Public Domain
extra_metadata:
  created_at: 2020-01-01 12:00:00
  Some Metadata Key 1:
    key_1_1: value_1_1
    key_1_2: value_1_2
  key_2:
    key_2_1: value_2_1
    key_2_2: value_2_2
"""


def gql_metadata_operation(expect_variables):
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)
        assert body["variables"] == expect_variables
        if body["operationName"] == "UpsertRepoProfile":
            assert body["query"] == _PROFILE_UPSERT_QUERY
        elif body["operationName"] == "FindRepositories":
            response = {
                "data": {
                    "findRepository": {
                        "edges": [
                            {
                                "node": {
                                    "namespace": "namespace1",
                                    "repository": "repo1",
                                    "highlight": "<<some_query>> is here",
                                }
                            },
                            {
                                "node": {
                                    "namespace": "namespace2",
                                    "repository": "repo2",
                                    "highlight": "this is another result for <<ome_query>>",
                                }
                            },
                        ],
                        "totalCount": 42,
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        else:
            raise AssertionError()

        namespace = body["variables"]["namespace"]
        repository = body["variables"]["repository"]

        error_response = {
            "errors": [
                {
                    "message": "An error has occurred",
                    "locations": [{"line": 3, "column": 3}],
                    "path": ["upsertRepoProfileByNamespaceAndRepository"],
                }
            ],
            "data": {"__typename": "Mutation", "upsertRepoProfileByNamespaceAndRepository": None},
        }

        success_response = {
            "data": {
                "__typename": "Mutation",
                "upsertRepoProfileByNamespaceAndRepository": {
                    "clientMutationId": None,
                    "__typename": "UpsertRepoProfilePayload",
                },
            }
        }

        if request.headers.get("Authorization") != "Bearer " + ACCESS_TOKEN:
            response = error_response
            response["errors"][0]["message"] = "Invalid token"
        elif namespace != "someuser":
            response = error_response
            response["errors"][0][
                "message"
            ] = 'new row violates row-level security policy for table "repo_profiles"'
        elif repository != "somerepo":
            response = error_response
            response["errors"][0][
                "message"
            ] = 'insert or update on table "repo_profiles" violates foreign key constraint "repo_fk"'
        else:
            response = success_response

        return [
            200,
            response_headers,
            json.dumps(response),
        ]

    return _gql_callback


def gql_metadata_get():
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)
        if body["operationName"] == "GetRepositoryMetadata":
            response = {
                "data": {
                    "repositories": {
                        "nodes": [
                            {
                                "namespace": "someuser",
                                "repository": "somerepo_1",
                                "repoTopicsByNamespaceAndRepository": {"nodes": []},
                                "repoProfileByNamespaceAndRepository": {
                                    "description": "Repository Description 1",
                                    "license": "Public Domain",
                                    "metadata": {
                                        "created_at": "2020-01-01 12:00:00",
                                        "upstream_metadata": {"key_1": {"key_2": "value_1"}},
                                    },
                                    "readme": "Test Repo 1 Readme",
                                    "sources": [
                                        {
                                            "anchor": "test data source",
                                            "href": "https://example.com",
                                            "isCreator": True,
                                            "isSameAs": False,
                                        }
                                    ],
                                },
                            },
                            {
                                "namespace": "otheruser",
                                "repository": "somerepo_2",
                                "repoTopicsByNamespaceAndRepository": {
                                    "nodes": [{"topics": ["topic_1", "topic_2"]}]
                                },
                                "repoProfileByNamespaceAndRepository": {
                                    "description": "Repository Description 2",
                                    "license": None,
                                    "metadata": None,
                                    "readme": "Test Repo 2 Readme",
                                    "sources": [
                                        {
                                            "anchor": "test data source",
                                            "href": "https://example.com",
                                        }
                                    ],
                                },
                            },
                        ]
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        elif body["operationName"] == "GetRepositoryDataSource":
            response = {
                "data": {
                    "repositoryDataSources": {
                        "nodes": [
                            {
                                "namespace": "otheruser",
                                "repository": "somerepo_2",
                                "credentialId": "abcdef-123456",
                                "dataSource": "plugin",
                                "params": {"plugin": "specific", "params": "here"},
                                "tableParams": {
                                    "table_1": {"param_1": "val_1"},
                                    "table_2": {"param_1": "val_2"},
                                },
                                "externalImageByNamespaceAndRepository": {
                                    "imageByNamespaceAndRepositoryAndImageHash": {
                                        "tablesByNamespaceAndRepositoryAndImageHash": {
                                            "nodes": [
                                                {
                                                    "tableName": "table_1",
                                                    "tableSchema": [
                                                        [
                                                            0,
                                                            "id",
                                                            "text",
                                                            False,
                                                            "Column ID",
                                                        ],
                                                        [
                                                            1,
                                                            "val",
                                                            "text",
                                                            False,
                                                            "Some value",
                                                        ],
                                                    ],
                                                },
                                                {
                                                    "tableName": "table_3",
                                                    "tableSchema": [
                                                        [
                                                            0,
                                                            "id",
                                                            "text",
                                                            False,
                                                            "Column ID",
                                                        ],
                                                        [
                                                            1,
                                                            "val",
                                                            "text",
                                                            False,
                                                            "Some value",
                                                        ],
                                                    ],
                                                },
                                            ]
                                        }
                                    }
                                },
                            },
                        ]
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        else:
            raise AssertionError()

    return _gql_callback


def list_external_credentials(request, uri, response_headers):
    return [
        200,
        response_headers,
        json.dumps(
            {
                "credentials": [
                    {
                        "credential_id": "123e4567-e89b-12d3-a456-426655440000",
                        "credential_name": "my_other_credential",
                        "plugin_name": "plugin_2",
                    },
                    {
                        "credential_id": "98765432-aaaa-bbbb-a456-000000000000",
                        "credential_name": "my_credential",
                        "plugin_name": "plugin",
                    },
                ]
            }
        ),
    ]


def update_external_credential(request, uri, response_headers):
    data = json.loads(request.body)

    assert data == {
        "credential_id": "98765432-aaaa-bbbb-a456-000000000000",
        "credential_name": "my_credential",
        "credential_data": {"username": "my_username", "password": "secret"},
        "plugin_name": "plugin",
    }

    return [
        200,
        response_headers,
        json.dumps({"credential_id": "98765432-aaaa-bbbb-a456-000000000000"}),
    ]


def add_external_credential(request, uri, response_headers):
    data = json.loads(request.body)
    assert data == {
        "credential_data": {"password": "secret", "username": "my_username"},
        "credential_name": "my_unused_credential",
        "plugin_name": "plugin_3",
    }
    return [
        200,
        response_headers,
        json.dumps({"credential_id": "cccccccc-aaaa-bbbb-dddd-000000000000"}),
    ]


def add_external_repo(request, uri, response_headers):
    data = json.loads(request.body)

    if data["namespace"] == "someuser" and data["repository"] == "somerepo_1":
        assert data == {
            "namespace": "someuser",
            "repository": "somerepo_1",
            "plugin_name": "plugin_2",
            "params": {},
            "is_live": True,
            "tables": {},
            "credential_id": "123e4567-e89b-12d3-a456-426655440000",
        }
    elif data["namespace"] == "someuser" and data["repository"] == "somerepo_2":
        assert data == {
            "namespace": "someuser",
            "repository": "somerepo_2",
            "plugin_name": "plugin_3",
            "params": {},
            "is_live": True,
            "tables": {},
            "credential_id": "00000000-0000-0000-0000-000000000000",
        }
    elif data["namespace"] == "otheruser" and data["repository"] == "somerepo_2":
        assert data == {
            "credential_id": "98765432-aaaa-bbbb-a456-000000000000",
            "is_live": True,
            "namespace": "otheruser",
            "params": {"params": "here", "plugin": "specific"},
            "plugin_name": "plugin",
            "repository": "somerepo_2",
            "tables": {
                "table_1": {
                    "options": {"param_1": "val_1"},
                    "schema": {"id": "text", "val": "text"},
                },
                "table_2": {"options": {"param_1": "val_2"}, "schema": {}},
                "table_3": {"options": {}, "schema": {"id": "text", "val": "text"}},
            },
        }
    else:
        raise AssertionError("Unknown repository %s/%s!" % (data["namespace"], data["repository"]))

    return [
        200,
        response_headers,
        json.dumps({"live_image_hash": "abcdef12" * 8}),
    ]


def upsert_repository_metadata(request, uri, response_headers):
    data = json.loads(request.body)
    assert data["operationName"] == "UpsertRepoProfile"
    assert data["query"] == _PROFILE_UPSERT_QUERY

    variables = data["variables"]
    if variables["namespace"] == "someuser" and variables["repository"] == "somerepo_1":
        assert variables == {
            "namespace": "someuser",
            "repository": "somerepo_1",
            "readme": "# Readme 1",
            "description": "Repository Description 1",
            "topics": [],
            "sources": [
                {
                    "anchor": "test data source",
                    "href": "https://example.com",
                    "isCreator": True,
                    "isSameAs": False,
                }
            ],
            "license": "Public Domain",
        }
    elif variables["namespace"] == "someuser" and variables["repository"] == "somerepo_2":
        assert variables == {
            "description": "Another Repository",
            "namespace": "someuser",
            "repository": "somerepo_2",
        }
    elif variables["namespace"] == "otheruser" and variables["repository"] == "somerepo_2":
        assert variables == {
            "description": "Repository Description 2",
            "namespace": "otheruser",
            "readme": "# Readme 2",
            "repository": "somerepo_2",
            "sources": [{"anchor": "test data source", "href": "https://example.com"}],
            "topics": ["topic_1", "topic_2"],
        }
    else:
        raise AssertionError(
            "Unknown repository %s/%s!" % (variables["namespace"], variables["repository"])
        )

    success_response = {
        "data": {
            "__typename": "Mutation",
            "upsertRepoProfileByNamespaceAndRepository": {
                "clientMutationId": None,
                "__typename": "UpsertRepoProfilePayload",
            },
        }
    }

    return [
        200,
        response_headers,
        json.dumps(success_response),
    ]


def register_user(request, uri, response_headers):
    assert json.loads(request.body) == {
        "username": "someuser",
        "password": "somepassword",
        "email": "someuser@example.com",
        "accept_tos": True,
    }
    return [
        200,
        response_headers,
        json.dumps(
            {
                "user_id": "123e4567-e89b-12d3-a456-426655440000",
                "access_token": ACCESS_TOKEN,
                "refresh_token": REFRESH_TOKEN,
            }
        ),
    ]


def refresh_token(request, uri, response_headers):
    assert json.loads(request.body) == {"username": "someuser", "password": "somepassword"}
    return [
        200,
        response_headers,
        json.dumps({"access_token": ACCESS_TOKEN, "refresh_token": REFRESH_TOKEN}),
    ]


def access_token(request, uri, response_headers):
    assert json.loads(request.body) == {
        "api_key": API_KEY,
        "api_secret": API_SECRET,
    }
    return [
        200,
        response_headers,
        json.dumps({"access_token": ACCESS_TOKEN}),
    ]


def create_credentials(request, uri, response_headers):
    assert json.loads(request.body) == {"password": "somepassword"}
    assert request.headers["Authorization"] == "Bearer %s" % ACCESS_TOKEN
    return [
        200,
        response_headers,
        json.dumps({"key": API_KEY, "secret": API_SECRET}),
    ]


def tos(request, uri, response_headers):
    return [
        200,
        response_headers,
        json.dumps({"tos": "Sample ToS message"}),
    ]
