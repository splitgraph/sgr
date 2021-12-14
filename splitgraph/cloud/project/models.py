"""Definitions for the splitgraph.yml format that's used to batch-populate a Splitgraph catalog
with repositories and their metadata.
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


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


class SplitgraphYAML(BaseModel):
    repositories: List[Repository] = []
    credentials: Optional[Dict[str, Credential]]
