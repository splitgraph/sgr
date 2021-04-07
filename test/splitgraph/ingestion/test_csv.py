import os

from splitgraph.core.types import TableColumn
from splitgraph.engine import ResultShape
from splitgraph.hooks.s3_server import MINIO
from splitgraph.ingestion.common import generate_column_names
from splitgraph.ingestion.csv import CSVDataSource
from splitgraph.ingestion.csv.common import CSVOptions, make_csv_reader
from splitgraph.ingestion.csv.fdw import CSVForeignDataWrapper
from splitgraph.ingestion.inference import infer_sg_schema
from test.splitgraph.conftest import INGESTION_RESOURCES_CSV


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

    assert len(schema) == 3
    schema = sorted(schema, key=lambda s: s["table_name"])

    assert schema[0] == {
        "columns": [
            {"column_name": "col_1", "type_name": "integer"},
            {"column_name": "DATE", "type_name": "character varying"},
            {"column_name": "TEXT", "type_name": "character varying"},
        ],
        "options": {"s3_object": "some_prefix/encoding-win-1252.csv"},
        "schema": None,
        "table_name": "encoding-win-1252.csv",
    }
    assert schema[1] == {
        "table_name": "fruits.csv",
        "schema": None,
        "columns": [
            {"column_name": "fruit_id", "type_name": "integer"},
            {"column_name": "timestamp", "type_name": "timestamp"},
            {"column_name": "name", "type_name": "character varying"},
            {"column_name": "number", "type_name": "integer"},
            {"column_name": "bignumber", "type_name": "bigint"},
            {"column_name": "vbignumber", "type_name": "numeric"},
        ],
        "options": {"s3_object": "some_prefix/fruits.csv"},
    }
    assert schema[2]["table_name"] == "rdu-weather-history.csv"
    assert schema[2]["columns"][0] == {"column_name": "date", "type_name": "date"}

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
            {"column_name": "number", "type_name": "integer"},
            {"column_name": "bignumber", "type_name": "bigint"},
            {"column_name": "vbignumber", "type_name": "numeric"},
        ],
        "options": None,
    }


def test_csv_data_source_s3(local_engine_empty):
    source = CSVDataSource(
        local_engine_empty,
        credentials={
            "s3_access_key": "minioclient",
            "s3_secret_key": "supersecure",
        },
        params={
            "s3_endpoint": "objectstorage:9000",
            "s3_secure": False,
            "s3_bucket": "test_csv",
            "s3_object_prefix": "some_prefix/",
        },
    )

    schema = source.introspect()

    assert len(schema.keys()) == 3
    assert schema["fruits.csv"] == [
        TableColumn(ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None),
        TableColumn(
            ordinal=2,
            name="timestamp",
            pg_type="timestamp without time zone",
            is_pk=False,
            comment=None,
        ),
        TableColumn(ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None),
        TableColumn(ordinal=4, name="number", pg_type="integer", is_pk=False, comment=None),
        TableColumn(ordinal=5, name="bignumber", pg_type="bigint", is_pk=False, comment=None),
        TableColumn(ordinal=6, name="vbignumber", pg_type="numeric", is_pk=False, comment=None),
    ]
    assert schema["encoding-win-1252.csv"] == [
        TableColumn(ordinal=1, name="col_1", pg_type="integer", is_pk=False, comment=None),
        TableColumn(ordinal=2, name="DATE", pg_type="character varying", is_pk=False, comment=None),
        TableColumn(ordinal=3, name="TEXT", pg_type="character varying", is_pk=False, comment=None),
    ]
    assert len(schema["rdu-weather-history.csv"]) == 28

    preview = source.preview(schema)
    assert len(preview.keys()) == 3
    assert len(preview["fruits.csv"]) == 4
    assert len(preview["encoding-win-1252.csv"]) == 3
    assert len(preview["rdu-weather-history.csv"]) == 10

    try:
        source.mount("temp_data")

        assert local_engine_empty.run_sql('SELECT COUNT(1) FROM temp_data."fruits.csv"') == [(4,)]

        # Test NULL "inference" for numbers
        assert (
            local_engine_empty.run_sql(
                'SELECT number FROM temp_data."fruits.csv"',
                return_shape=ResultShape.MANY_ONE,
            )
            == [1, 2, None, 4]
        )

        assert local_engine_empty.run_sql(
            'SELECT COUNT(1) FROM temp_data."rdu-weather-history.csv"'
        ) == [(4633,)]

        assert local_engine_empty.run_sql(
            'SELECT "TEXT" FROM temp_data."encoding-win-1252.csv"'
        ) == [("Pañamao",), ("–",), ("División",)]
    finally:
        local_engine_empty.delete_schema("temp_data")


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


def test_csv_dialect_encoding_inference():
    # Test CSV dialect inference with:
    #  - win-1252 encoding (will autodetect with chardet)
    #  - Windows line endings
    #  - different separator
    #  - first column name missing

    with open(os.path.join(INGESTION_RESOURCES_CSV, "encoding-win-1252.csv"), "rb") as f:
        options = CSVOptions()
        options, reader = make_csv_reader(f, options)

        assert options.encoding == "Windows-1252"
        assert options.header is True

        # TODO: we keep these in the dialect struct rather than extract back out into the
        #  CSVOptions. Might need to do the latter if we want to return the proposed FDW table
        #  params to the user.
        assert options.dialect.lineterminator == "\r\n"
        assert options.dialect.delimiter == ";"

        data = list(reader)

        assert data == [
            ["", "DATE", "TEXT"],
            ["1", "01/07/2021", "Pañamao"],
            ["2", "06/11/2018", "–"],
            ["3", "28/05/2018", "División"],
        ]

        schema = generate_column_names(infer_sg_schema(data))
        assert schema == [
            TableColumn(ordinal=1, name="col_1", pg_type="integer", is_pk=False, comment=None),
            TableColumn(
                ordinal=2, name="DATE", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=3, name="TEXT", pg_type="character varying", is_pk=False, comment=None
            ),
        ]
