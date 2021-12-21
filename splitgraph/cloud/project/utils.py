from functools import reduce, wraps
from pathlib import Path
from typing import Callable, Dict, List, Optional, TypeVar

from splitgraph.cloud.project.models import (
    Credential,
    External,
    Metadata,
    Repository,
    SplitgraphYAML,
)
from splitgraph.utils.yaml import safe_dump, safe_load

T = TypeVar("T")


def resolve_optional(
    func: Callable[[T, T], T]
) -> Callable[[Optional[T], Optional[T]], Optional[T]]:
    @wraps(func)
    def wrapped(left: Optional[T], right: Optional[T]) -> Optional[T]:
        if not left:
            return right
        if not right:
            return left
        return func(left, right)

    return wrapped


@resolve_optional
def merge_credentials(
    left: Dict[str, Credential], right: Dict[str, Credential]
) -> Dict[str, Credential]:
    # Credentials override each other fully
    return {**left, **right}


@resolve_optional
def merge_metadata(left: Metadata, right: Metadata) -> Metadata:
    return Metadata.parse_obj(
        {
            **left.dict(by_alias=True, exclude_unset=True),
            **right.dict(by_alias=True, exclude_unset=True),
        }
    )


@resolve_optional
def merge_external(left: External, right: External) -> External:
    left_d = left.dict(by_alias=True, exclude_unset=True)
    right_d = right.dict(by_alias=True, exclude_unset=True)

    for field_name, field_value in right_d.items():
        if field_name in ["credential_id", "credential"]:
            # Credentials (ID or name) override credential references.
            if "credential" in left_d:
                del left_d["credential"]
            if "credential_id" in left_d:
                del left_d["credential_id"]
            left_d[field_name] = field_value
        elif field_name == "tables":
            # Override the schema and the table params separately
            left_d[field_name] = {**left_d.get(field_name, {}), **field_value}
        else:
            left_d[field_name] = field_value
    return External.parse_obj(left_d)


@resolve_optional
def merge_repository(left: Repository, right: Repository) -> Repository:
    return Repository(
        namespace=left.namespace,
        repository=left.repository,
        metadata=merge_metadata(left.metadata, right.metadata),
        external=merge_external(left.external, right.external),
    )


def merge_repository_lists(left: List[Repository], right: List[Repository]) -> List[Repository]:
    left_by_repo = {(r.namespace, r.repository): r for r in left}
    right_by_repo = {(r.namespace, r.repository): r for r in right}

    all_repos = [(r.namespace, r.repository) for r in left]
    all_repos.extend(
        [
            (r.namespace, r.repository)
            for r in right
            if (r.namespace, r.repository) not in left_by_repo
        ]
    )

    result = [
        merge_repository(left_by_repo.get(repo), right_by_repo.get(repo)) for repo in all_repos
    ]
    return [r for r in result if r]


def merge_project_files(left: SplitgraphYAML, right: SplitgraphYAML) -> SplitgraphYAML:
    return SplitgraphYAML(
        credentials=merge_credentials(left.credentials, right.credentials),
        repositories=merge_repository_lists(left.repositories, right.repositories),
    )


def load_project(paths: List[Path]) -> SplitgraphYAML:
    if not paths:
        raise ValueError("No project files specified!")

    def _load(path: Path) -> SplitgraphYAML:
        with open(path, "r") as f:
            return SplitgraphYAML.parse_obj(safe_load(f))

    return reduce(merge_project_files, map(_load, paths))


def dump_project(project: SplitgraphYAML, stream) -> None:
    safe_dump(project.dict(by_alias=True, exclude_unset=True), stream)


def get_source_name(repository: str) -> str:
    return repository.replace("/", "_").replace("-", "_")
