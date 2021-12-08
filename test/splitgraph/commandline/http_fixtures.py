import json
import random
from datetime import datetime
from urllib.parse import urlparse

from splitgraph.cloud import (
    BULK_UPDATE_REPO_SOURCES,
    BULK_UPSERT_REPO_PROFILES,
    BULK_UPSERT_REPO_TOPICS,
    CSV_URL,
    INGESTION_JOB_STATUS,
    JOB_LOGS,
    PROFILE_UPSERT,
    START_LOAD,
)

REMOTE = "remote_engine"
AUTH_ENDPOINT = "http://some-auth-service.example.com"
GQL_ENDPOINT = "http://some-gql-service.example.com"
QUERY_ENDPOINT = "http://some-query-service.example.com"
STORAGE_ENDPOINT = "http://some-storage-service.example.com"

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
            assert body["query"] == PROFILE_UPSERT
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
        _somerepo_1 = {
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
        }
        _somerepo_2 = {
            "namespace": "otheruser",
            "repository": "somerepo_2",
            "repoTopicsByNamespaceAndRepository": {
                "nodes": [{"topic": "topic_1"}, {"topic": "topic_2"}]
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
        }

        body = json.loads(request.body)
        if body["operationName"] == "GetRepositoryMetadata":
            response = {
                "data": {
                    "repositories": {
                        "nodes": [_somerepo_1]
                        if body.get("variables", {}).get("repository") == "somerepo_1"
                        else [_somerepo_1, _somerepo_2]
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
                                "ingestionScheduleByNamespaceAndRepository": None,
                            },
                            {
                                "namespace": "otheruser",
                                "repository": "somerepo_3",
                                "credentialId": "abcdef-123456",
                                "dataSource": "plugin",
                                "params": {"plugin": "specific", "params": "here"},
                                "tableParams": {
                                    "table_1": {"param_1": "val_1"},
                                    "table_2": {"param_1": "val_2"},
                                },
                                "externalImageByNamespaceAndRepository": None,
                                "ingestionScheduleByNamespaceAndRepository": {
                                    "schedule": "0 * * * *",
                                    "enabled": True,
                                    "schema": {
                                        "table_1": [
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
                                        "table_3": [
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
                                },
                            },
                        ]
                        if body.get("variables", {}).get("repository") != "somerepo_1"
                        else []
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

    assert data["repositories"] is not None
    assert data["repositories"] == [
        {
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
            "schedule": None,
        },
        {
            "namespace": "someuser",
            "repository": "somerepo_1",
            "plugin_name": "plugin_2",
            "params": {},
            "is_live": True,
            "tables": {},
            "credential_id": "123e4567-e89b-12d3-a456-426655440000",
            "schedule": None,
        },
        {
            "namespace": "someuser",
            "repository": "somerepo_2",
            "plugin_name": "plugin_3",
            "params": {},
            "is_live": True,
            "tables": {},
            "credential_id": "00000000-0000-0000-0000-000000000000",
            "schedule": None,
        },
    ]

    return [
        200,
        response_headers,
        json.dumps({"live_image_hashes": ["abcdef12" * 8, "ghijkl34" * 8, "mnoprs56" * 8]}),
    ]


def assert_repository_profiles(request):
    data = json.loads(request.body)
    assert data["operationName"] == "BulkUpsertRepoProfilesMutation"
    assert data["query"] == BULK_UPSERT_REPO_PROFILES

    variables = data["variables"]
    assert variables["namespaces"] == ["otheruser", "someuser", "someuser"]
    assert variables["repositories"] == ["somerepo_2", "somerepo_1", "somerepo_2"]
    assert variables["readmes"] == ["# Readme 2", "# Readme 1", None]
    assert variables["descriptions"] == [
        "Repository Description 2",
        "Repository Description 1",
        "Another Repository",
    ]
    assert variables["licenses"] == [None, "Public Domain", None]
    assert variables["metadata"] == [None, None, None]


def assert_repository_sources(request):
    data = json.loads(request.body)
    assert data["operationName"] == "BulkUpdateRepoSourcesMutation"
    assert data["query"] == BULK_UPDATE_REPO_SOURCES

    variables = data["variables"]
    assert variables["namespaces"] == ["otheruser", "someuser"]
    assert variables["repositories"] == ["somerepo_2", "somerepo_1"]
    assert variables["sources"] == [
        {"anchor": "test data source", "href": "https://example.com"},
        {
            "anchor": "test data source",
            "href": "https://example.com",
            "isCreator": True,
            "isSameAs": False,
        },
    ]


def assert_repository_topics(request):
    data = json.loads(request.body)
    assert data["operationName"] == "BulkUpsertRepoTopicsMutation"
    assert data["query"] == BULK_UPSERT_REPO_TOPICS

    variables = data["variables"]
    assert variables["namespaces"] == ["otheruser", "otheruser"]
    assert variables["repositories"] == ["somerepo_2", "somerepo_2"]
    assert variables["topics"] == ["topic_1", "topic_2"]


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


def gql_job_status():
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)

        if body["query"] == INGESTION_JOB_STATUS:
            namespace, repository = body["variables"]["namespace"], body["variables"]["repository"]

            if namespace == "someuser" and repository == "somerepo_1":
                nodes = [
                    {
                        "taskId": "somerepo1_task",
                        "started": str(datetime(2020, 1, 1, 0, 0, 0)),
                        "finished": None,
                        "isManual": False,
                        "status": "STARTED",
                    }
                ]
            elif namespace == "someuser" and repository == "somerepo_2":
                nodes = [
                    {
                        "taskId": "somerepo2_task",
                        "started": str(datetime(2021, 1, 1, 0, 0, 0)),
                        "finished": str(datetime(2021, 1, 1, 1, 0, 0)),
                        "isManual": False,
                        "status": "SUCCESS",
                    }
                ]
            else:
                nodes = []

            response = {"data": {"repositoryIngestionJobStatus": {"nodes": nodes}}}
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        else:
            raise AssertionError()

    return _gql_callback


def gql_job_logs():
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)

        if body["query"] == JOB_LOGS:
            return mock_gql_job_logs(body, response_headers)
        else:
            raise AssertionError()

    return _gql_callback


def mock_gql_job_logs(body, response_headers):
    namespace, repository, task_id = (
        body["variables"]["namespace"],
        body["variables"]["repository"],
        body["variables"]["taskId"],
    )
    url = f"{STORAGE_ENDPOINT}/{namespace}/{repository}/{task_id}"
    response = {"data": {"jobLogs": {"url": url}}}
    return [
        200,
        response_headers,
        json.dumps(response),
    ]


def job_log_callback(request, uri, response_headers):
    parsed = urlparse(uri)
    if parsed.path.split("/")[-1] == "notfound":
        return [404, response_headers, f"Logs for {parsed.path} not found!"]
    return [200, response_headers, f"Logs for {parsed.path}"]


def gql_upload(namespace, repository, final_status="SUCCESS"):
    status_call_count = 0
    upload_links = []
    download_links = []

    def _gql_callback(request, uri, response_headers):
        nonlocal download_links
        nonlocal upload_links
        body = json.loads(request.body)

        if body["query"] == JOB_LOGS:
            return mock_gql_job_logs(body, response_headers)
        elif body["query"] == CSV_URL:
            upload_id = "{0:016x}".format(random.getrandbits(64))
            download_links.append(f"{STORAGE_ENDPOINT}/download/{upload_id}")
            upload_links.append(f"{STORAGE_ENDPOINT}/upload/{upload_id}")

            return [
                200,
                response_headers,
                json.dumps(
                    {
                        "data": {
                            "csvUploadDownloadUrls": {
                                "upload": upload_links[-1],
                                "download": download_links[-1],
                            }
                        }
                    }
                ),
            ]
        elif body["query"] == START_LOAD:
            assert body["variables"] == {
                "namespace": "someuser",
                "repository": "somerepo_1",
                "pluginName": "csv",
                "params": '{"connection": {"connection_type": "http", "url": ""}}',
                "tableParams": [
                    {
                        "name": "base_df",
                        "options": f'{{"url": "{download_links[0]}"}}',
                        "schema": [],
                    },
                    {
                        "name": "patch_df",
                        "options": f'{{"url": "{download_links[1]}"}}',
                        "schema": [],
                    },
                ],
            }
            return [
                200,
                response_headers,
                json.dumps({"data": {"startExternalRepositoryLoad": {"taskId": "ingest_task"}}}),
            ]
        elif body["query"] == INGESTION_JOB_STATUS:
            nonlocal status_call_count
            if status_call_count == 0:
                status = {
                    "taskId": "ingest_task",
                    "started": str(datetime(2021, 1, 1, 0, 0, 0)),
                    "finished": None,
                    "isManual": False,
                    "status": "STARTED",
                }
                status_call_count = 1
            else:
                status = {
                    "taskId": "ingest_task",
                    "started": str(datetime(2021, 1, 1, 0, 0, 0)),
                    "finished": str(datetime(2021, 1, 1, 1, 0, 0)),
                    "isManual": False,
                    "status": final_status,
                }
            return [
                200,
                response_headers,
                json.dumps({"data": {"repositoryIngestionJobStatus": {"nodes": [status]}}}),
            ]
        raise AssertionError()

    def _file_upload_callback(request, uri, response_headers):
        assert uri in upload_links
        assert b"fruit_id,timestamp,name" in request.body
        return [200, response_headers, ""]

    return _gql_callback, _file_upload_callback


def gql_sync(namespace, repository, is_existing=True, is_sync=True):
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)

        if body["query"] == START_LOAD:
            if is_existing:
                assert body["variables"] == {
                    "namespace": namespace,
                    "repository": repository,
                    "sync": is_sync,
                }
            else:
                assert body["variables"] == {
                    "namespace": "otheruser",
                    "repository": "somerepo_2",
                    "pluginName": "plugin",
                    "params": '{"plugin": "specific", "params": "here"}',
                    "tableParams": [
                        {
                            "name": "table_1",
                            "options": '{"param_1": "val_1"}',
                            "schema": [
                                {"name": "id", "pgType": "text"},
                                {"name": "val", "pgType": "text"},
                            ],
                        },
                        {"name": "table_2", "options": '{"param_1": "val_2"}', "schema": []},
                        {
                            "name": "table_3",
                            "options": "{}",
                            "schema": [
                                {"name": "id", "pgType": "text"},
                                {"name": "val", "pgType": "text"},
                            ],
                        },
                    ],
                    "sync": True,
                    "credentialData": '{"username": "my_username", "password": "secret"}',
                }
            return [
                200,
                response_headers,
                json.dumps({"data": {"startExternalRepositoryLoad": {"taskId": "ingest_task"}}}),
            ]
        raise AssertionError()

    return _gql_callback
