from splitgraph.core.types import IntrospectionResult
from splitgraph.hooks.data_source import DataSource


class TestDataSource(DataSource):
    def introspect(self) -> IntrospectionResult:
        return {"some_table": ([], {})}

    @classmethod
    def get_name(cls) -> str:
        return "Test Data Source"

    @classmethod
    def get_description(cls) -> str:
        return "Data source for testing"

    credentials_schema = {
        "type": "object",
        "properties": {"access_token": {"type": "string"}},
        "required": ["access_token"],
    }

    params_schema = {
        "type": "object",
        "properties": {"some_field": {"type": "string"}},
        "required": ["some_field"],
    }


__plugin__ = TestDataSource
