import itertools
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, cast

from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    MountError,
    Params,
    PreviewResult,
    TableColumn,
    TableInfo,
    TableParams,
    TableSchema,
)
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import get_exception_name
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.seafowl.client import (
    SeafowlClient,
    emit_value,
    get_pg_type,
    quote_ident,
)


class SeafowlDataSource(ForeignDataWrapperDataSource):
    # Hack to make us previewable, even though we don't mount things
    def get_remote_schema_name(self) -> str:
        return self.schema

    def get_fdw_name(self):
        return "seafowl"

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        pass

    def mount(
        self, schema: str, tables: Optional[TableInfo] = None, overwrite: bool = True
    ) -> Optional[List[MountError]]:
        pass

    @classmethod
    def get_name(cls) -> str:
        return "Seafowl"

    @classmethod
    def get_description(cls) -> str:
        return "Query a Seafowl database"

    @classmethod
    def _get_table_schema(cls, table_columns: Iterator[Dict[str, Any]]) -> TableSchema:
        return [
            TableColumn(i + 1, tc["column_name"], get_pg_type(tc["data_type"]), False, None)
            for i, tc in enumerate(table_columns)
        ]

    def fake_rewrite(self, query: str, shim_schema: str) -> str:
        query = query.replace(shim_schema, self.schema)
        results = self.client.sql(query)

        if not results:
            return "SELECT 'no results'"

        column_names = list(results[0].keys())

        return (
            "SELECT * FROM (VALUES "
            + ",".join(
                "(" + ",".join(emit_value(r.get(c)) for c in column_names) + ")" for r in results
            )
            + ") v("
            + ",".join(quote_ident(i) for i in column_names)
            + ")"
        )

    def introspect(self) -> IntrospectionResult:
        result = self.client.sql(
            """SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1
        ORDER BY table_name ASC, ordinal_position ASC""",
            (self.schema,),
        )

        tables = {
            table_name: cast(
                Union[Tuple[TableSchema, TableParams], MountError],
                (
                    self._get_table_schema(table_columns),
                    TableParams({"table_name": table_name}),
                ),
            )
            for table_name, table_columns in itertools.groupby(
                result, lambda r: str(r["table_name"])
            )
        }

        return IntrospectionResult(tables)

    def preview(self, tables: Optional[TableInfo]) -> PreviewResult:
        # Preview data in tables mounted by this FDW / data source

        result = PreviewResult({})

        if not tables:
            to_preview = sorted(self.introspect().keys())
        elif isinstance(tables, list):
            to_preview = tables
        else:
            to_preview = list(tables.keys())

        for table_name in to_preview:
            try:
                result[table_name] = self.client.sql(
                    f"SELECT * FROM {quote_ident(self.schema)}.{quote_ident(table_name)} LIMIT 10"
                )
            except Exception as e:
                result[table_name] = MountError(
                    table_name=table_name, error=get_exception_name(e), error_text=str(e).strip()
                )

        return result

    credentials_schema: Dict[str, Any] = {
        "type": "object",
    }

    params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "title": "URL",
                "description": "URL of your Seafowl instance, e.g. https://demo.seafowl.io",
            },
            "schema": {
                "type": "string",
                "title": "Schema",
                "description": "Name of schema to import",
                "default": "public",
            },
        },
        "required": ["url"],
    }

    table_params_schema = {
        "type": "object",
        "properties": {"table_name": {"type": "string", "title": "Table name"}},
        "required": ["table_name"],
    }

    commandline_help: str = ""
    commandline_kwargs_help: str = ""

    supports_mount = True
    supports_load = True
    supports_sync = False

    _icon_file = "seafowl.svg"

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        super().__init__(engine, credentials, params, tables)

        self.client = SeafowlClient(url=params["url"])
        self.schema = str(params.get("schema", "public"))
