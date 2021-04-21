"""
Definitions for the repositories.yml format that's used to batch-populate
"""
from typing import Dict, List, Optional, Any

from pydantic import BaseModel, Field

# Models for the externals API data (tables, plugin params etc)
from splitgraph.core.types import TableSchema


class Table(BaseModel):
    options: Dict[str, Any]
    schema_: Dict[str, str] = Field(alias="schema")


class Credential(BaseModel):
    plugin: str
    data: Dict[str, Any]


class External(BaseModel):
    credential_id: Optional[str]
    credential: Optional[str]
    plugin: str
    params: Dict[str, Any]
    tables: Dict[str, Table]


# Models for the catalog metadata (description, README, topics etc)


class Source(BaseModel):
    anchor: str
    href: str
    isCreator: Optional[bool]
    isSameAs: Optional[bool]


class Metadata(BaseModel):
    readme: Optional[str]
    description: Optional[str]
    topics: Optional[List[str]]
    sources: Optional[List[Source]]
    license: Optional[str]
    extra_metadata: Optional[Dict[str, Dict[str, Any]]]


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
            topics: List[str]

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
            readme=profile.readme,
            description=profile.description,
            topics=[
                t for node in self.repoTopicsByNamespaceAndRepository.nodes for t in node.topics
            ],
            sources=profile.sources,
            license=profile.license,
            metadata=profile.metadata.upstream_metadata if profile.metadata else None,
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
                schema={c.name: c.pg_type for c in schemas.get(t, {})},
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
