import json
import os
from io import BytesIO
from test.splitgraph.conftest import INGESTION_RESOURCES_CSV
from unittest import mock

import pytest

from splitgraph.core.types import MountError, Params, TableColumn, unwrap
from splitgraph.engine import ResultShape
from splitgraph.hooks.s3_server import MINIO
from splitgraph.ingestion.common import generate_column_names
from splitgraph.ingestion.csv import CSVDataSource
from splitgraph.ingestion.csv.common import (
    CSVOptions,
    dump_options,
    load_options,
    make_csv_reader,
)
from splitgraph.ingestion.csv.fdw import CSVForeignDataWrapper
from splitgraph.ingestion.inference import infer_sg_schema

_s3_win_1252_opts = {
    "s3_object": "some_prefix/encoding-win-1252.csv",
    "autodetect_dialect": False,
    "autodetect_encoding": False,
    "autodetect_header": False,
    "delimiter": ";",
    "encoding": "Windows-1252",
    "header": True,
    "quotechar": '"',
}

_s3_fruits_opts = {
    "s3_object": "some_prefix/fruits.csv",
    "autodetect_dialect": False,
    "autodetect_encoding": False,
    "autodetect_header": False,
    "delimiter": ",",
    "encoding": "utf-8",
    "header": True,
    "quotechar": '"',
}


def test_csv_param_migration():
    assert CSVDataSource.migrate_params(
        Params(
            {
                "s3_endpoint": "objectstorage:9000",
                "s3_secure": False,
                "s3_bucket": "test_csv",
                "delimiter": ",",
            }
        )
    ) == Params(
        {
            "delimiter": ",",
            "connection": {
                "connection_type": "s3",
                "s3_endpoint": "objectstorage:9000",
                "s3_secure": False,
                "s3_bucket": "test_csv",
            },
        }
    )

    assert CSVDataSource.migrate_params(Params({"url": "some-url"})) == Params(
        {"connection": {"connection_type": "http", "url": "some-url"}}
    )


def test_csv_introspection_s3():
    fdw_options = {
        "s3_endpoint": "objectstorage:9000",
        "s3_secure": False,
        "s3_access_key": "minioclient",
        "s3_secret_key": "supersecure",
        "s3_bucket": "test_csv",
        "s3_object_prefix": "some_prefix/",
    }

    schema = CSVForeignDataWrapper.import_schema(
        schema=None,
        srv_options=dump_options(fdw_options),
        options={},
        restriction_type=None,
        restricts=[],
    )

    assert len(schema) == 4
    schema = sorted(schema, key=lambda s: s["table_name"])

    assert schema[0] == {
        "columns": [
            {"column_name": "col_1", "type_name": "integer"},
            {"column_name": "DATE", "type_name": "character varying"},
            {"column_name": "TEXT", "type_name": "character varying"},
        ],
        "options": mock.ANY,
        "schema": None,
        "table_name": "encoding-win-1252.csv",
    }
    assert load_options(schema[0]["options"]) == _s3_win_1252_opts
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
        "options": mock.ANY,
    }
    assert load_options(schema[1]["options"]) == _s3_fruits_opts
    assert schema[2]["table_name"] == "percentage_sign.csv"
    assert schema[2]["columns"][0] == {"column_name": "Id", "type_name": "integer"}
    assert schema[3]["table_name"] == "rdu-weather-history.csv"
    assert schema[3]["columns"][0] == {"column_name": "date", "type_name": "date"}


def test_csv_introspection_http():
    # Pre-sign the S3 URL for an easy HTTP URL to test this
    schema = CSVForeignDataWrapper.import_schema(
        schema=None,
        srv_options=dump_options(
            {"url": MINIO.presigned_get_object("test_csv", "some_prefix/fruits.csv")}
        ),
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
        "options": mock.ANY,
    }

    assert load_options(schema[0]["options"]) == {
        "autodetect_dialect": False,
        "autodetect_encoding": False,
        "autodetect_header": False,
        "delimiter": ",",
        "encoding": "utf-8",
        "header": True,
        "quotechar": '"',
    }


def test_csv_introspection_multiple():
    # Test running the introspection passing the table options as CREATE FOREIGN SCHEMA params.
    # In effect, we specify the table names, S3 key/URL and expect the FDW to figure out
    # the rest.

    fdw_options = {
        "s3_endpoint": "objectstorage:9000",
        "s3_secure": False,
        "s3_access_key": "minioclient",
        "s3_secret_key": "supersecure",
        "s3_bucket": "test_csv",
        "s3_object_prefix": "some_prefix/",
    }

    url = MINIO.presigned_get_object("test_csv", "some_prefix/rdu-weather-history.csv")
    schema = CSVForeignDataWrapper.import_schema(
        schema=None,
        srv_options=dump_options(fdw_options),
        options={
            "table_options": json.dumps(
                {
                    "from_url": {"url": url},
                    "from_s3_rdu": {"s3_object": "some_prefix/rdu-weather-history.csv"},
                    "from_s3_encoding": {"s3_object": "some_prefix/encoding-win-1252.csv"},
                }
            )
        },
        restriction_type=None,
        restricts=[],
    )

    assert len(schema) == 3
    schema = sorted(schema, key=lambda s: s["table_name"])

    assert schema[0] == {
        "table_name": "from_s3_encoding",
        "schema": None,
        "columns": mock.ANY,
        "options": mock.ANY,
    }
    assert load_options(schema[0]["options"]) == _s3_win_1252_opts
    assert schema[1] == {
        "table_name": "from_s3_rdu",
        "schema": None,
        "columns": mock.ANY,
        "options": mock.ANY,
    }
    assert load_options(schema[1]["options"]) == {
        "autodetect_dialect": False,
        "autodetect_encoding": False,
        "autodetect_header": False,
        "delimiter": ";",
        "encoding": "utf-8",
        "header": True,
        "quotechar": '"',
        "s3_object": "some_prefix/rdu-weather-history.csv",
    }

    assert schema[2] == {
        "table_name": "from_url",
        "schema": None,
        "columns": mock.ANY,
        "options": mock.ANY,
    }
    assert load_options(schema[2]["options"]) == {
        "autodetect_dialect": False,
        "autodetect_encoding": False,
        "autodetect_header": False,
        "delimiter": ";",
        "encoding": "utf-8",
        "header": True,
        "quotechar": '"',
        "url": url,
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

    assert len(schema.keys()) == 5
    assert schema["fruits.csv"] == (
        [
            TableColumn(ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None),
            TableColumn(
                ordinal=2,
                name="timestamp",
                pg_type="timestamp without time zone",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(ordinal=4, name="number", pg_type="integer", is_pk=False, comment=None),
            TableColumn(ordinal=5, name="bignumber", pg_type="bigint", is_pk=False, comment=None),
            TableColumn(ordinal=6, name="vbignumber", pg_type="numeric", is_pk=False, comment=None),
        ],
        {
            "s3_object": "some_prefix/fruits.csv",
            "autodetect_dialect": False,
            "autodetect_encoding": False,
            "autodetect_header": False,
            "delimiter": ",",
            "encoding": "utf-8",
            "header": True,
            "quotechar": '"',
        },
    )
    assert schema["encoding-win-1252.csv"] == (
        [
            TableColumn(ordinal=1, name="col_1", pg_type="integer", is_pk=False, comment=None),
            TableColumn(
                ordinal=2, name="DATE", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=3, name="TEXT", pg_type="character varying", is_pk=False, comment=None
            ),
        ],
        {
            "s3_object": "some_prefix/encoding-win-1252.csv",
            "autodetect_dialect": False,
            "autodetect_encoding": False,
            "autodetect_header": False,
            "delimiter": ";",
            "encoding": "Windows-1252",
            "header": True,
            "quotechar": '"',
        },
    )
    assert len(schema["rdu-weather-history.csv"][0]) == 28
    assert schema["percentage_sign.csv"] == (
        [
            TableColumn(ordinal=1, name="Id", pg_type="integer", is_pk=False, comment=None),
            TableColumn(
                ordinal=2,
                name="Submit time",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=3, name="Profile", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=4, name="Status", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=5,
                name="Source currency",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=6, name="Amount paid by", pg_type="numeric", is_pk=False, comment=None
            ),
            TableColumn(ordinal=7, name="Fee", pg_type="numeric", is_pk=False, comment=None),
            TableColumn(
                ordinal=8, name="Amount converted", pg_type="numeric", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=9, name="Excess refund", pg_type="numeric", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=10,
                name="Target currency",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=11,
                name="Converted and sent to",
                pg_type="numeric",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=12, name="Exchange rate", pg_type="numeric", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=13,
                name="Exchange Rate Date",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=14,
                name="Payout time",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=15, name="Name", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=16,
                name="Account details",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
            TableColumn(
                ordinal=17, name="Reference", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=18, name="VAT (10%)", pg_type="character varying", is_pk=False, comment=None
            ),
        ],
        {
            "autodetect_dialect": False,
            "autodetect_encoding": False,
            "autodetect_header": False,
            "delimiter": ",",
            "encoding": "utf-8",
            "header": True,
            "quotechar": '"',
            "s3_object": "some_prefix/percentage_sign.csv",
        },
    )

    assert schema["not_a_csv.txt"] == MountError(
        table_name="not_a_csv.txt",
        error="ValueError",
        error_text="Malformed CSV: header has 7 columns, rows have 0 columns",
    )

    schema = unwrap(schema)[0]

    # Add a nonexistent file to the schema with malformed params to check preview error reporting
    schema["doesnt_exist"] = (
        [],
        {"s3_object": "doesnt_exist"},
    )
    schema["exists_but_broken"] = (
        # Force a schema that doesn't work for this CSV
        [TableColumn(1, "col_1", "date", False)],
        {"s3_object": "some_prefix/fruits.csv"},
    )

    preview = source.preview(schema)
    assert len(preview.keys()) == 6
    assert len(preview["fruits.csv"]) == 4
    assert len(preview["encoding-win-1252.csv"]) == 3
    assert len(preview["rdu-weather-history.csv"]) == 10
    assert len(preview["percentage_sign.csv"]) == 1
    assert preview["doesnt_exist"] == MountError.construct(
        table_name="doesnt_exist", error="minio.error.S3Error", error_text=mock.ANY
    )
    assert preview["exists_but_broken"] == MountError.construct(
        table_name="exists_but_broken",
        error="psycopg2.errors.InvalidDatetimeFormat",
        error_text='invalid input syntax for type date: "1"',
    )

    try:
        source.mount("temp_data")

        assert local_engine_empty.run_sql('SELECT COUNT(1) FROM temp_data."fruits.csv"') == [(4,)]

        # Test NULL "inference" for numbers
        assert local_engine_empty.run_sql(
            'SELECT number FROM temp_data."fruits.csv"',
            return_shape=ResultShape.MANY_ONE,
        ) == [1, 2, None, 4]

        assert local_engine_empty.run_sql(
            'SELECT COUNT(1) FROM temp_data."rdu-weather-history.csv"'
        ) == [(4633,)]

        assert local_engine_empty.run_sql(
            'SELECT "TEXT" FROM temp_data."encoding-win-1252.csv"'
        ) == [("Pañamao",), ("–",), ("División",)]
    finally:
        local_engine_empty.delete_schema("temp_data")


def test_csv_data_source_multiple(local_engine_empty):
    # End-to-end version for test_csv_introspection_multiple to check things like table params
    # getting serialized and deserialized properly.

    url = MINIO.presigned_get_object("test_csv", "some_prefix/rdu-weather-history.csv")

    credentials = {
        "s3_access_key": "minioclient",
        "s3_secret_key": "supersecure",
    }

    params = {
        "s3_endpoint": "objectstorage:9000",
        "s3_secure": False,
        "s3_bucket": "test_csv",
        # Put this delimiter in as a canary to make sure table params override server params.
        "delimiter": ",",
    }

    tables = {
        # Pass an empty table schema to denote we want to introspect it
        "from_url": ([], {"url": url}),
        "from_s3_rdu": ([], {"s3_object": "some_prefix/rdu-weather-history.csv"}),
        "from_s3_encoding": ([], {"s3_object": "some_prefix/encoding-win-1252.csv"}),
        "from_url_broken": ([], {"url": "invalid_url"}),
        "from_s3_broken": ([], {"s3_object": "invalid_object"}),
    }

    source = CSVDataSource(
        local_engine_empty,
        credentials,
        params,
        tables,
    )

    schema = source.introspect()

    assert schema == {
        "from_url": (
            mock.ANY,
            {
                "autodetect_dialect": False,
                "url": url,
                "quotechar": '"',
                "header": True,
                "encoding": "utf-8",
                "delimiter": ";",
                "autodetect_header": False,
                "autodetect_encoding": False,
            },
        ),
        "from_s3_rdu": (
            mock.ANY,
            {
                "encoding": "utf-8",
                "autodetect_dialect": False,
                "autodetect_encoding": False,
                "autodetect_header": False,
                "delimiter": ";",
                "header": True,
                "quotechar": '"',
                "s3_object": "some_prefix/rdu-weather-history.csv",
            },
        ),
        "from_s3_encoding": (
            mock.ANY,
            {
                "s3_object": "some_prefix/encoding-win-1252.csv",
                "quotechar": '"',
                "header": True,
                "encoding": "Windows-1252",
                "autodetect_dialect": False,
                "delimiter": ";",
                "autodetect_header": False,
                "autodetect_encoding": False,
            },
        ),
        "from_url_broken": MountError.construct(
            table_name="from_url_broken",
            error="requests.exceptions.MissingSchema",
            error_text="Invalid URL 'invalid_url': No scheme supplied. Perhaps you meant https://invalid_url?",
        ),
        "from_s3_broken": MountError.construct(
            table_name="from_s3_broken",
            error="minio.error.S3Error",
            error_text=mock.ANY,
        ),
    }

    # Mount the datasets with this introspected schema.
    schema = unwrap(schema)[0]
    try:
        source.mount("temp_data", tables=schema)
        rows = local_engine_empty.run_sql("SELECT * FROM temp_data.from_s3_encoding")
        assert len(rows) == 3
        assert len(rows[0]) == 3
    finally:
        local_engine_empty.delete_schema("temp_data")

    # Override the delimiter and blank out the schema for a single table
    schema["from_s3_encoding"] = (
        [],
        {
            "s3_object": "some_prefix/encoding-win-1252.csv",
            "quotechar": '"',
            "header": True,
            "encoding": "Windows-1252",
            "autodetect_dialect": False,
            # We force a delimiter "," here which will make the CSV a single-column one
            # (to test we can actually override these)
            "delimiter": ",",
            "autodetect_header": False,
            "autodetect_encoding": False,
            # Override this to make sure that we can roundtrip numerical parameters through
            # this introspection flow.
            "schema_inference_rows": 10,
        },
    )

    # Reintrospect the source with the new table parameters
    source = CSVDataSource(local_engine_empty, credentials, params, schema)
    new_schema = source.introspect()
    assert len(new_schema) == 3
    # Check other tables are unchanged
    assert new_schema["from_url"] == schema["from_url"]
    assert new_schema["from_s3_rdu"] == schema["from_s3_rdu"]

    # Table with a changed separator only has one column (since we have , for delimiter
    # instead of ;)
    assert new_schema["from_s3_encoding"][0] == [
        TableColumn(
            ordinal=1, name=";DATE;TEXT", pg_type="character varying", is_pk=False, comment=None
        )
    ]

    assert new_schema["from_s3_encoding"][1]["schema_inference_rows"] == 10

    try:
        source.mount("temp_data", tables=new_schema)
        rows = local_engine_empty.run_sql("SELECT * FROM temp_data.from_s3_encoding")
        assert len(rows) == 3
        # Check we get one column now
        assert rows[0] == ("1;01/07/2021;Pañamao",)
    finally:
        local_engine_empty.delete_schema("temp_data")


def test_csv_data_source_raw_url(local_engine_empty):
    # Use the data from the previous test to test out the raw URL functionality
    url = MINIO.presigned_get_object("test_csv", "some_prefix/rdu-weather-history.csv")

    credentials = {
        "s3_access_key": "minioclient",
        "s3_secret_key": "supersecure",
    }

    params = {
        "s3_endpoint": "objectstorage:9000",
        "s3_secure": False,
        "s3_bucket": "test_csv",
        # Put this delimiter in as a canary to make sure table params override server params.
        "delimiter": ",",
    }

    tables = {
        "from_url": ([], {"url": url}),
        "from_s3_rdu": ([], {"s3_object": "some_prefix/rdu-weather-history.csv"}),
        "from_s3_encoding": ([], {"s3_object": "some_prefix/encoding-win-1252.csv"}),
        "from_url_broken": ([], {"url": "invalid_url"}),
        "from_s3_broken": ([], {"s3_object": "invalid_object"}),
    }

    source = CSVDataSource(
        local_engine_empty,
        credentials,
        params,
        tables,
    )

    schema = source.introspect()
    schema = unwrap(schema)[0]

    raw_urls = source.get_raw_url(tables=schema)
    assert raw_urls == {
        "from_s3_encoding": [
            (
                "text/csv",
                mock.ANY,
            )
        ],
        "from_s3_rdu": [
            (
                "text/csv",
                mock.ANY,
            )
        ],
        "from_url": [
            (
                "text/csv",
                url,
            )
        ],
    }

    assert "objectstorage:9000" in raw_urls["from_s3_encoding"][0][1]
    assert "objectstorage:9000" in raw_urls["from_s3_rdu"][0][1]


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
    assert len(schema["data"][0]) == 28

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
        # NB we don't extract everything from the sniffed dialect, just the delimiter and the
        # quotechar. The sniffer also returns doublequote and skipinitialspace.
        assert options.delimiter == ";"

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


def test_csv_mac_newlines():
    # Test a CSV file with old Mac-style newlines (\r)

    with open(os.path.join(INGESTION_RESOURCES_CSV, "mac_newlines.csv"), "rb") as f:
        options = CSVOptions()
        options, reader = make_csv_reader(f, options)

        assert options.encoding == "utf-8"
        assert options.header is True

        data = list(reader)
        assert len(data) == 5
        assert data[0] == ["fruit_id", "timestamp", "name"]

        schema = generate_column_names(infer_sg_schema(data))
        assert schema == [
            TableColumn(ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None),
            TableColumn(
                ordinal=2, name="timestamp", pg_type="timestamp", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None
            ),
        ]


def test_csv_ignore_decoding_errors():
    # Test doomed CSVs with malformed Unicode characters. Can't repro this with a small example,
    # but in some situations chardet can return None, so we fall back to UTF-8. For the purposes
    # of this test, we force UTF-8 instead.

    malformed = b"name;number\nTA\xef\xbf\xbd\xef\xbf\xbd\xef\xc3\x87\xc3\x83O\xc2\xba;17"

    options = CSVOptions(ignore_decode_errors=False, encoding="utf-8", autodetect_encoding=False)

    with pytest.raises(UnicodeDecodeError):
        make_csv_reader(BytesIO(malformed), options)

    options = CSVOptions(ignore_decode_errors=True, encoding="utf-8", autodetect_encoding=False)
    options, reader = make_csv_reader(BytesIO(malformed), options)
    assert options.encoding == "utf-8"
    assert options.header is True

    data = list(reader)
    assert len(data) == 2
    assert data[0] == ["name", "number"]
    assert data[1] == ["TA��ÇÃOº", "17"]


def test_csv_grades():
    # Test a grades.csv file from https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html with
    # multiple features:
    #  * Line 10 is missing a comma, which means the next field merges: test we pad it and
    #    still try to do best-effort inference
    #  * Fields/separators have commas

    with open(os.path.join(INGESTION_RESOURCES_CSV, "grades.csv"), "rb") as f:
        options = CSVOptions()
        options, reader = make_csv_reader(f, options)

        assert options.encoding == "utf-8"
        assert options.header is True
        assert options.delimiter == ","
        assert options.quotechar == '"'

        data = list(reader)
        assert len(data) == 17
        # Error on line 10 leads to two fields merging
        assert len(data[9]) == 8
        assert len(data[0]) == 9
        assert data[0] == [
            "Last name",
            "First name",
            "SSN",
            "Test1",
            "Test2",
            "Test3",
            "Test4",
            "Final",
            "Grade",
        ]

        schema = generate_column_names(infer_sg_schema(data))
        assert schema == [
            TableColumn(
                ordinal=1, name="Last name", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=2, name="First name", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=3, name="SSN", pg_type="character varying", is_pk=False, comment=None
            ),
            # This field has to be a varchar because of line 10 (becomes "49.0      1.0")
            TableColumn(
                ordinal=4, name="Test1", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(ordinal=5, name="Test2", pg_type="numeric", is_pk=False, comment=None),
            TableColumn(ordinal=6, name="Test3", pg_type="numeric", is_pk=False, comment=None),
            TableColumn(ordinal=7, name="Test4", pg_type="numeric", is_pk=False, comment=None),
            TableColumn(
                ordinal=8, name="Final", pg_type="character varying", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=9, name="Grade", pg_type="character varying", is_pk=False, comment=None
            ),
        ]
