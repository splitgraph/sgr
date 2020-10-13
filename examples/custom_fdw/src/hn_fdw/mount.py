from typing import Dict

from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.hooks.data_source import ForeignDataWrapperDataSource

# Define the schema of the foreign table we wish to create
# We're only going to be fetching stories, so limit the columns to the ones that
# show up for stories. See https://github.com/HackerNews/API for reference.
_story_schema_spec = [
    TableColumn(1, "id", "integer", True),
    TableColumn(2, "by", "text", False),
    TableColumn(3, "time", "integer", False),
    TableColumn(4, "title", "text", False),
    TableColumn(5, "url", "text", False),
    TableColumn(6, "text", "text", False),
    TableColumn(7, "score", "integer", False),
    TableColumn(8, "kids", "integer[]", False),
    TableColumn(9, "descendants", "integer", False),
]

_all_endpoints = [
    "topstories",
    "newstories",
    "beststories",
    "askstories",
    "showstories",
    "jobstories",
]


class HackerNewsDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {"type": "object"}

    params_schema = {
        "type": "object",
        "properties": {
            "endpoints": {"type": "array", "items": {"type": "string", "enum": _all_endpoints}}
        },
    }

    @classmethod
    def get_name(cls) -> str:
        return "Hacker News"

    @classmethod
    def get_description(cls) -> str:
        return "Hacker News stories through the Firebase API"

    def get_fdw_name(self):
        # Define the FDW that this plugin uses on the backend
        return "multicorn"

    def get_server_options(self):
        # Define server options that are common for all tables managed by this wrapper
        return {
            # Module path to our foreign data wrapper class on the engine side
            "wrapper": "hn_fdw.fdw.HNForeignDataWrapper",
        }

    def introspect(self) -> Dict[str, TableSchema]:
        # Return a list of this FDW's tables and their schema.
        endpoints = self.params["endpoints"] or _all_endpoints
        return {e: _story_schema_spec for e in endpoints}
