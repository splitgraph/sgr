from splitgraph.core.types import TableColumn
from splitgraph.hooks.s3_server import MINIO
from splitgraph.ingestion.csv import CSVDataSource
from splitgraph.ingestion.csv.fdw import CSVForeignDataWrapper


def test_csv_introspection_s3():
    fdw_options = {
        "s3_endpoint": "objectstorage:9000",
        "s3_secure": "false",
        "s3_access_key": "minioclient",
        "s3_secret_key": "supersecure",
        "s3_bucket": "test_csv",
        "s3_object_prefix": "some_prefix/",
    }

    schema = CSVForeignDataWrapper.import_schema(
        schema=None, srv_options=fdw_options, options={}, restriction_type=None, restricts=[]
    )

    assert len(schema) == 2
    schema = sorted(schema, key=lambda s: s["table_name"])

    assert schema[0] == {
        "table_name": "some_prefix/fruits.csv",
        "schema": None,
        "columns": [
            {"column_name": "fruit_id", "type_name": "integer"},
            {"column_name": "timestamp", "type_name": "timestamp"},
            {"column_name": "name", "type_name": "character varying"},
        ],
        "options": {"s3_object": "some_prefix/fruits.csv"},
    }
    assert schema[1]["table_name"] == "some_prefix/rdu-weather-history.csv"
    assert schema[1]["columns"][0] == {"column_name": "date", "type_name": "date"}

    # TODO we need a way to pass suggested table options in the inference / preview response,
    #   since we need to somehow decouple the table name from the S3 object name and/or customize
    #   delimiter/quotechar


def test_csv_introspection_http():
    # Pre-sign the S3 URL for an easy HTTP URL to test this
    schema = CSVForeignDataWrapper.import_schema(
        schema=None,
        srv_options={"url": MINIO.presigned_get_object("test_csv", "some_prefix/fruits.csv")},
        options={},
        restriction_type=None,
        restricts=[],
    )
    assert len(schema) == 1

    assert schema[0] == {
        "table_name": "data",
        "schema": None,
        "columns": [
            {"column_name": "fruit_id", "type_name": "integer"},
            {"column_name": "timestamp", "type_name": "timestamp"},
            {"column_name": "name", "type_name": "character varying"},
        ],
        "options": None,
    }


def test_csv_data_source_s3(local_engine_empty):
    source = CSVDataSource(
        local_engine_empty,
        credentials={"s3_access_key": "minioclient", "s3_secret_key": "supersecure",},
        params={
            "s3_endpoint": "objectstorage:9000",
            "s3_secure": False,
            "s3_bucket": "test_csv",
            "s3_object_prefix": "some_prefix/",
        },
    )

    schema = source.introspect()

    assert len(schema.keys()) == 2
    assert schema["some_prefix/fruits.csv"] == [
        TableColumn(ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None),
        TableColumn(
            ordinal=2,
            name="timestamp",
            pg_type="timestamp without time zone",
            is_pk=False,
            comment=None,
        ),
        TableColumn(ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None),
    ]
    assert len(schema["some_prefix/rdu-weather-history.csv"]) == 28

    preview = source.preview(schema)
    assert len(preview.keys()) == 2
    assert len(preview["some_prefix/fruits.csv"]) == 4
    assert len(preview["some_prefix/rdu-weather-history.csv"]) == 10


def test_csv_data_source_http(local_engine_empty):
    source = CSVDataSource(
        local_engine_empty,
        credentials={},
        params={
            "url": MINIO.presigned_get_object("test_csv", "some_prefix/rdu-weather-history.csv"),
        },
    )

    schema = source.introspect()
    assert len(schema.keys()) == 1
    assert len(schema["data"]) == 28

    preview = source.preview(schema)
    assert len(preview.keys()) == 1
    assert len(preview["data"]) == 10


## TODO this breaks
"""sgr mount csv target_schema -o@- <<EOF
  {
    "s3_endpoint": "objectstorage:9000",
    "s3_access_key": "minioclient",
    "s3_secret_key": "supersecure",
    "s3_bucket": "test_csv",
    "s3_object_prefix": "some_prefix/",
    "s3_secure": false,
    "autodetect_header": true,
    "autodetect_dialect": true
  }
EOF

>> SELECT * FROM target_schema."some_prefix/rdu-weather-history.csv"
Error in python: IndexError
DETAIL:  list index out of range                                                            
"""
