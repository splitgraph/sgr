"""
This script querys all repositories that exist in Splitgraph using the GraphQL API. This
information is summarised and written out as a file repositories.yml. READMEs are also
downloaded and stored in the readmes/ directory, named according to their Splitgraph
repository.

This script requires you to have already generated access credentials for the GraphQL
API:

 1) Install the ``sgr`` CLI
 3) Run ``sgr cloud login`` or ``sgr cloud login --remote my_remote``

This script also requires a few dependencies: ``pip install pydantic requests yaml``
"""
import argparse
import hashlib
import os
import posixpath
import string
import sys
from configparser import ConfigParser
from glob import glob
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import requests
import yaml
from pydantic import BaseModel

REMOTE = "data.splitgraph.com"
READMES_DIRECTORY = "readmes"
DOMAIN = "https://api.splitgraph.com/"  # TODO read from .sgconfig
VALID_FILENAME_CHARACTERS = string.ascii_letters + string.digits + "-_."


class MetadataResponse(BaseModel, extra="ignore"):
    """
    GraphQL query and Pydantic schema for fetching metadata (topics, description,
    readme) from all repositories via Splitgraph's /gql/cloud/graphql endpoint
    """

    _ENDPOINT = "gql/cloud/graphql"  # TODO get from config
    _QUERY = """
        query MyQuery {
          repositories {
            nodes {
              namespace
              repository
              repoTopicsByNamespaceAndRepository {
                nodes {
                  topics
                }
              }
              repoProfileByNamespaceAndRepository {
                metadata
                readme
                description
              }
            }
          }
        }
    """

    class RepoTopicsByNamespaceAndRepository(BaseModel, extra="ignore"):
        class RepositoryTopics(BaseModel, extra="ignore"):
            topics: List[str]

        nodes: List[RepositoryTopics]

    class RepoProfileByNamespaceAndRepository(BaseModel, extra="ignore"):
        metadata: Any
        readme: Optional[str]
        description: Optional[str]

    namespace: str
    repository: str
    repoTopicsByNamespaceAndRepository: RepoTopicsByNamespaceAndRepository
    repoProfileByNamespaceAndRepository: RepoProfileByNamespaceAndRepository

    @classmethod
    def from_response(cls, response) -> List["MetadataResponse"]:
        nodes = response["data"]["repositories"]["nodes"]
        return [cls.parse_obj(obj) for obj in nodes]


class SourcesResponse(BaseModel, extra="ignore"):
    """
    GraphQL query and Pydantic schema for fetching source information (schema, data
    source) from all repositories via Splitgraph's /gql/registry/graphql endpoint
    """

    _ENDPOINT = "gql/registry/graphql"
    _QUERY = """
        query MyQuery {
          repositoryDataSources {
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
        }
    """

    class Params(BaseModel, extra="ignore"):
        s3_endpoint: str
        s3_region: str
        s3_bucket: str
        s3_secure: bool

    class TableParam(BaseModel, extra="ignore"):
        s3_object: str

    class ExternalImageByNamespaceAndRepository(BaseModel, extra="ignore"):
        class ImageByNamespaceAndRepositoryAndImageHash(BaseModel, extra="ignore"):
            class TablesByNamespaceAndRepositoryAndImageHash(BaseModel, extra="ignore"):
                class Node(BaseModel, extra="ignore"):
                    class ColumnSchema(NamedTuple):
                        index: str
                        name: str
                        field_type: str
                        unknown1: bool
                        unknown2: Any

                    tableName: str
                    tableSchema: List[ColumnSchema]

                nodes: List[Node]

            tablesByNamespaceAndRepositoryAndImageHash: TablesByNamespaceAndRepositoryAndImageHash

        imageByNamespaceAndRepositoryAndImageHash: ImageByNamespaceAndRepositoryAndImageHash

    namespace: str
    repository: str
    credentialId: str
    dataSource: str
    dataSource: str
    params: Params
    tableParams: Dict[str, TableParam]
    externalImageByNamespaceAndRepository: ExternalImageByNamespaceAndRepository

    @classmethod
    def from_response(cls, response) -> List["SourcesResponse"]:
        nodes = response["data"]["repositoryDataSources"]["nodes"]
        return [cls.parse_obj(obj) for obj in nodes]


class TableOptions(BaseModel):
    s3_object: str


class Table(BaseModel):
    options: TableOptions


class Params(BaseModel):
    s3_endpoint: str
    s3_region: str
    s3_bucket: str
    s3_secure: bool
    tables: Dict[str, Table]
    schemas: Dict[str, Dict[str, str]]


class External(BaseModel):
    credential: str
    plugin: str
    params: Params


class Metadata(BaseModel):
    topics: List[str]
    description: Optional[str]
    readme: Optional[str]


class Repository(BaseModel):
    namespace: str
    repository: str
    metadata: Metadata
    external: External


class Repositories(BaseModel):
    repositories: List[Repository]


def construct_repositories(repositories_sources, repositories_metadata) -> Repositories:
    def combine_dictionaries(a: dict, b: dict) -> Dict[Any, Tuple]:
        """Note: assumes a and b have identical keys"""
        keys = set(a.keys()).union(set(b.keys()))
        return {key: (a[key], b[key]) for key in keys}

    # put both responses into dictionaries keyed by (namespace, repository) so that we
    # can combine them
    d1 = {(r.namespace, r.repository): r for r in repositories_sources}
    d2 = {(r.namespace, r.repository): r for r in repositories_metadata}
    combined = combine_dictionaries(d1, d2)

    return Repositories(
        repositories=[
            Repository(
                namespace=namespace,
                repository=repository,
                metadata=Metadata(
                    description=metadata.repoProfileByNamespaceAndRepository.description,
                    readme=metadata.repoProfileByNamespaceAndRepository.readme,
                    topics=[
                        topic
                        for node in metadata.repoTopicsByNamespaceAndRepository.nodes
                        for topic in node.topics
                    ],
                ),
                external=External(
                    credential=source.credentialId,
                    plugin=source.dataSource,
                    params=Params(
                        s3_endpoint=source.params.s3_endpoint,
                        s3_region=source.params.s3_region,
                        s3_bucket=source.params.s3_bucket,
                        s3_secure=source.params.s3_secure,
                        tables={
                            name: Table(options=TableOptions(s3_object=param.s3_object))
                            for name, param in source.tableParams.items()
                        },
                        schemas={
                            node.tableName: {
                                column.name: column.field_type for column in node.tableSchema
                            }
                            for node in source.externalImageByNamespaceAndRepository.imageByNamespaceAndRepositoryAndImageHash.tablesByNamespaceAndRepositoryAndImageHash.nodes
                        },
                    ),
                ),
            )
            for ((namespace, repository), (source, metadata)) in combined.items()
        ]
    )


def _normalise_filename(filename):
    """
    Strips out odd characters from a string so that it can be used as a valid filename.
    In order for the result to be unique, we also makea short hash of the original
    string and stick it on the end
    """
    base = "".join(c for c in filename if c in VALID_FILENAME_CHARACTERS)
    short_hash = hashlib.sha1(filename.encode()).hexdigest()[:4]
    return f"{base}.{short_hash}"


def _write_readmes_to_disk(data: Repositories, directory):
    """
    READMEs aren't rendering very nicely in YAML so we instead write them out to
    individual files and replace the README contents with their file paths.

    :param data: will be modified by this function
    """

    for repository in data.repositories:
        readme = repository.metadata.readme
        if readme is not None:
            filename = f"{repository.namespace}-{repository.repository}"
            filename = _normalise_filename(filename)
            filename = f"{filename}.md"
            path = os.path.join(directory, filename)
            with open(path, "w") as file:
                file.write(readme)
            print(f"written {path}")
            repository.metadata.readme = path


def output_repositories_yaml(data: Repositories, file):
    data = Repositories(
        repositories=sorted(data.repositories, key=lambda r: (r.namespace, r.repository))
    )
    yaml.dump(data.dict(), file)


def _delete_all_existing_readmes(directory):
    for path in glob(os.path.join(directory, "*.md")):
        os.unlink(path)
        print(f"deleted {path}")


def query_graphql(url, token, query):
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        data={"query": query},
    )
    response.raise_for_status()
    data = response.json()
    if "errors" in data:
        raise RuntimeError(data["errors"])
    return data


def read_graphql_token_from_splitgraph_config(remote) -> str:
    config_path = os.path.expanduser("~/.splitgraph/.sgconfig")
    config = ConfigParser()
    config.read(config_path)
    return config[f"remote: {remote}"]["SG_CLOUD_ACCESS_TOKEN"]


def fetch(cls, token):
    url = posixpath.join(DOMAIN, cls._ENDPOINT)
    response = query_graphql(url, token, cls._QUERY)
    return cls.from_response(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", default=REMOTE)
    parser.add_argument("--readme-directory", default=READMES_DIRECTORY)
    parser.add_argument("output", nargs="?", type=argparse.FileType("w"), default=sys.stdout)
    parser.add_argument("--domain", default=DOMAIN)  # TODO read from .sgconfig
    args = parser.parse_args()

    os.makedirs(args.readme_directory, exist_ok=True)  # check directory can be created

    token = read_graphql_token_from_splitgraph_config(args.remote)
    repositories_sources: List[SourcesResponse] = fetch(SourcesResponse, token)
    repositories_metadata: List[MetadataResponse] = fetch(MetadataResponse, token)

    data = construct_repositories(repositories_sources, repositories_metadata)

    _delete_all_existing_readmes(args.readme_directory)
    _write_readmes_to_disk(data, args.readme_directory)
    output_repositories_yaml(data, args.output)


if __name__ == "__main__":
    main()
