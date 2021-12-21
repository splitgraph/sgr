from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from splitgraph.core.image_mounting import ImageMounter
from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    Params,
    TableInfo,
    TableParams,
    get_table_list,
)
from splitgraph.hooks.data_source.base import LoadableDataSource, TransformingDataSource
from splitgraph.ingestion.dbt.utils import (
    compile_dbt_manifest,
    run_dbt_transformation_from_git,
)

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


class DBTDataSource(TransformingDataSource, LoadableDataSource):

    table_params_schema = {"type": "object", "properties": {}}

    params_schema = {
        "type": "object",
        "properties": {
            "git_branch": {
                "type": "string",
                "title": "Git branch",
                "description": "Branch or commit hash to use for the dbt project.",
                "default": "master",
            },
            "sources": {
                "type": "array",
                "title": "dbt source map",
                "description": "Describe which Splitgraph image to use for each dbt source "
                "(defined in sources.yml).",
                "items": {
                    "type": "object",
                    "properties": {
                        "dbt_source_name": {
                            "type": "string",
                            "title": "Source name",
                            "description": "Source name in the dbt model",
                        },
                        "namespace": {
                            "type": "string",
                            "title": "Namespace",
                        },
                        "repository": {
                            "type": "string",
                            "title": "Repository",
                        },
                        "hash_or_tag": {
                            "type": "string",
                            "title": "Hash or tag",
                            "default": "latest",
                        },
                    },
                    "required": ["dbt_source_name", "namespace", "repository"],
                },
            },
        },
    }

    credentials_schema = {
        "type": "object",
        "properties": {
            # This is a secret since the git URL might have a password in it.
            "git_url": {
                "type": "string",
                "title": "dbt model Git URL",
                "description": "URL to the Git repo "
                "with the dbt project, for example,"
                "`https://uname:pass_or_token@github.com/organisation/repository.git`.",
            },
        },
        "required": ["git_url"],
    }

    _icon_file = "dbt.svg"

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
        image_mounter: Optional[ImageMounter] = None,
    ):
        super().__init__(engine, credentials, params, tables, image_mounter)

        self.git_url: str = self.credentials["git_url"]
        self.git_branch: str = self.params.get("git_branch", "master")
        self.source_map: Dict[str, Tuple[str, str, str]] = {
            source["dbt_source_name"]: (
                source["namespace"],
                source["repository"],
                source.get("hash_or_tag", "latest"),
            )
            for source in self.params.get("sources", [])
        }

    @classmethod
    def get_name(cls) -> str:
        return "dbt"

    @classmethod
    def get_description(cls) -> str:
        return "Create a Splitgraph repository from a dbt model"

    def get_required_images(self) -> List[Tuple[str, str, str]]:
        return sorted(set(self.source_map.values()))

    def introspect(self) -> IntrospectionResult:
        # Get the dbt manifest file
        manifest = compile_dbt_manifest(
            engine=self.engine, repository_url=self.git_url, repository_ref=self.git_branch
        )

        # Extract all models from the repository that materialize as "table" and aren't tests
        model_names = [
            n["name"]
            for n in manifest["nodes"].values()
            if n["config"]["materialized"] == "table" and n["resource_type"] == "model"
        ]
        return IntrospectionResult({m: ([], TableParams({})) for m in model_names})

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        with self.mount_required_images() as schema_map:
            # Build a map of dbt data sources to schemas
            dbt_source_map = {
                source_name: schema_map[image] for source_name, image in self.source_map.items()
            }

            # Get a list of models to run, based on the tables we were passed
            models = get_table_list(tables) if tables else None

            # Run dbt against our source schemas and write data out into the target schema
            run_dbt_transformation_from_git(
                self.engine,
                schema,
                self.git_url,
                self.git_branch,
                source_schema_map=dbt_source_map,
                models=models,
            )
