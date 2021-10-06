"""
Definitions for the repositories.yml format that's used to batch-populate a Splitgraph catalog
with repositories and their metadata.
"""
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field
from splitgraph.core.types import Params, TableSchema

# Models for the externals API data (tables, plugin params etc)


class Table(BaseModel):
    class Column(BaseModel):
        name: str
        pg_type: str = Field(alias="type")
        comment: Optional[str]

    options: Dict[str, Any]
    schema_: List[Column] = Field(alias="schema")


class Credential(BaseModel):
    plugin: str
    data: Dict[str, Any]


class IngestionSchedule(BaseModel):
    schedule: str
    enabled = True


class External(BaseModel):
    credential_id: Optional[str]
    credential: Optional[str]
    plugin: str
    params: Dict[str, Any]
    tables: Dict[str, Table]
    is_live: bool = True
    schedule: Optional[IngestionSchedule]


# Models for the catalog metadata (description, README, topics etc)


class Source(BaseModel):
    anchor: str
    href: str
    isCreator: Optional[bool]
    isSameAs: Optional[bool]


class Metadata(BaseModel):
    class Readme(BaseModel):
        file: Optional[str]
        text: Optional[str]

    readme: Optional[Union[str, Readme]]
    description: Optional[str]
    topics: Optional[List[str]]
    sources: Optional[List[Source]]
    license: Optional[str]
    extra_metadata: Optional[Dict[str, Any]]


class Repository(BaseModel):
    namespace: str
    repository: str
    metadata: Optional[Metadata]
    external: Optional[External]


class RepositoriesYAML(BaseModel):
    repositories: List[Repository]
    credentials: Optional[Dict[str, Credential]]


# GQL response for the catalog metadata


class MetadataResponse(BaseModel):
    class RepoTopicsResponse(BaseModel):
        class RepositoryTopics(BaseModel):
            topic: str

        nodes: List[RepositoryTopics]

    class RepoProfileResponse(BaseModel):
        class MetadataResponse(BaseModel):
            created_at: Optional[str]
            updated_at: Optional[str]
            upstream_metadata: Dict[str, Any]

        readme: Optional[str]
        description: Optional[str]
        topics: Optional[List[str]]
        sources: Optional[List[Source]]
        license: Optional[str]
        metadata: Optional[MetadataResponse]

    namespace: str
    repository: str
    repoTopicsByNamespaceAndRepository: RepoTopicsResponse
    repoProfileByNamespaceAndRepository: RepoProfileResponse

    @classmethod
    def from_response(cls, response) -> List["MetadataResponse"]:
        nodes = response["data"]["repositories"]["nodes"]
        return [cls.parse_obj(obj) for obj in nodes]

    def to_metadata(self) -> Metadata:
        profile = self.repoProfileByNamespaceAndRepository

        return Metadata(
            readme=Metadata.Readme(text=profile.readme),
            description=profile.description,
            topics=[node.topic for node in self.repoTopicsByNamespaceAndRepository.nodes],
            sources=profile.sources,
            license=profile.license,
            extra_metadata=profile.metadata.upstream_metadata if profile.metadata else None,
        )


# GQL response for the external repo's metadata


class ExternalResponse(BaseModel):
    class ExternalImageResponse(BaseModel):
        class ImageResponse(BaseModel):
            class TablesResponse(BaseModel):
                class Node(BaseModel):
                    tableName: str
                    tableSchema: TableSchema

                nodes: List[Node]

            tablesByNamespaceAndRepositoryAndImageHash: TablesResponse

        imageByNamespaceAndRepositoryAndImageHash: ImageResponse

    namespace: str
    repository: str
    credentialId: Optional[str]
    dataSource: str
    params: Dict[str, Any]
    tableParams: Dict[str, Any]
    externalImageByNamespaceAndRepository: ExternalImageResponse

    @classmethod
    def from_response(cls, response) -> List["ExternalResponse"]:
        nodes = response["data"]["repositoryDataSources"]["nodes"]
        return [cls.parse_obj(obj) for obj in nodes]

    def to_external(self) -> External:
        schemas = {
            t.tableName: t.tableSchema
            for t in self.externalImageByNamespaceAndRepository.imageByNamespaceAndRepositoryAndImageHash.tablesByNamespaceAndRepositoryAndImageHash.nodes
        }

        all_tables = list(self.tableParams.keys())
        all_tables.extend(schemas.keys())

        tables = {
            t: Table(
                options=self.tableParams.get(t, {}),
                schema=[Table.Column(name=c.name, type=c.pg_type) for c in schemas.get(t, [])],
            )
            for t in all_tables
        }

        return External(
            credential_id=self.credentialId,
            plugin=self.dataSource,
            params=self.params,
            tables=tables,
        )


def make_repositories(
    metadata_responses: List[MetadataResponse], external_responses: List[ExternalResponse]
) -> List[Repository]:
    metadata = {(m.namespace, m.repository): m.to_metadata() for m in metadata_responses}
    external = {(r.namespace, r.repository): r.to_external() for r in external_responses}

    return [
        Repository(
            namespace=n, repository=r, metadata=metadata.get((n, r)), external=external.get((n, r))
        )
        for (n, r) in sorted(set(metadata.keys()) | set(external.keys()))
    ]


class ListExternalCredentialsResponse(BaseModel):
    class ExternalCredential(BaseModel):
        plugin_name: str
        credential_name: Optional[str]
        credential_id: str

    credentials: List[ExternalCredential]


class UpdateExternalCredentialRequest(BaseModel):
    credential_id: str
    credential_name: str
    credential_data: Dict[str, Any]
    plugin_name: str


class AddExternalCredentialRequest(BaseModel):
    credential_name: str
    credential_data: Dict[str, Any]
    plugin_name: str


class UpdateExternalCredentialResponse(BaseModel):
    credential_id: str


class ExternalTableRequest(BaseModel):
    options: Dict[str, Any] = {}
    schema_: Optional[Dict[str, str]] = Field(alias="schema")


class AddExternalRepositoryRequest(BaseModel):
    namespace: str
    repository: str
    plugin_name: str
    params: Params
    is_live: bool
    tables: Optional[Dict[str, ExternalTableRequest]]
    credential_id: Optional[str]
    credential_name: Optional[str]
    credential_data: Optional[Dict[str, Any]]
    schedule: Optional[IngestionSchedule]

    @classmethod
    def from_external(
        cls,
        namespace: str,
        repository: str,
        external: External,
        credential_map: Optional[Dict[str, str]] = None,
    ):
        credential_map = credential_map or {}

        credential_id = external.credential_id
        if external.credential and not credential_id:
            if external.credential not in credential_map:
                raise ValueError(
                    "Credential %s not defined in the file and not set up on the remote!"
                    % external.credential
                )
            credential_id = credential_map[external.credential]

        return cls(
            namespace=namespace,
            repository=repository,
            plugin_name=external.plugin,
            params=external.params,
            tables={
                table_name: ExternalTableRequest(
                    options=table.options, schema={c.name: c.pg_type for c in table.schema_}
                )
                for table_name, table in external.tables.items()
            },
            credential_id=credential_id,
            is_live=external.is_live,
            schedule=external.schedule,
        )


class AddExternalRepositoriesRequest(BaseModel):
    repositories: List[AddExternalRepositoryRequest]
