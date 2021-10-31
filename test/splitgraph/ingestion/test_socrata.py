import json
import os
from test.splitgraph.conftest import INGESTION_RESOURCES
from typing import NamedTuple
from unittest import mock
from unittest.mock import MagicMock, Mock, call

import pytest
from sodapy import Socrata
from splitgraph.core.types import TableColumn
from splitgraph.exceptions import RepositoryNotFoundError
from splitgraph.hooks.mount_handlers import mount
from splitgraph.ingestion.common import dedupe_sg_schema
from splitgraph.ingestion.socrata.mount import SocrataDataSource
from splitgraph.ingestion.socrata.querying import (
    ANY,
    _socrata_to_pg_type,
    cols_to_socrata,
    estimate_socrata_rows_width,
    quals_to_socrata,
    sortkeys_to_socrata,
)


class Q:  # noqa: SIM119
    def __init__(self, col, op, val, is_list=False, is_list_any=True):
        self.field_name = col
        if is_list:
            self.operator = (op, is_list_any)
        else:
            self.operator = op
        self.value = val

        self.is_list_operator = is_list
        # Code doesn't check for the actual "ALL" value so we can put whatever here
        self.list_any_or_all = ANY if is_list_any else "ALL"


class S(NamedTuple):
    attname: str
    nulls_first: bool = False
    is_reversed: bool = False


_long_name_col = "name_long_column_to_test_that_we_map_to_socrata_column_names_correctly"
_long_name_col_sg = "name_long_column_to_test_that_we_map_to_socrata_column_names_co"
_col_map = {_long_name_col_sg: _long_name_col}


@pytest.mark.parametrize(
    ("quals", "expected"),
    [
        ([Q("some_col", ">", 42)], "(`some_col` > 42)"),
        (
            [
                Q("some_col", ">", 42),
                Q("some_other_col", "~~", "%te'st%"),
                Q("some_other_col", "^", 1.23),
            ],
            "(`some_col` > 42) AND (`some_other_col` LIKE '%te''st%') AND (TRUE)",
        ),
        (
            [
                Q("some_col", "=", [1, 2], is_list=True),
                Q("some_other_col", "<>", [1, 2], is_list=True, is_list_any=False),
            ],
            "((`some_col` = 1) OR (`some_col` = 2)) "
            "AND ((`some_other_col` <> 1) AND (`some_other_col` <> 2))",
        ),
        ([Q("some_col", "=", None)], "(`some_col` IS NULL)"),
    ],
)
def test_socrata_quals(quals, expected):
    assert quals_to_socrata(quals) == expected


def test_socrata_cols():
    assert cols_to_socrata(["a", "b", "c"]) == "`a`,`b`,`c`"


def test_socrata_sortkeys():
    assert sortkeys_to_socrata([]) == ":id"
    assert sortkeys_to_socrata([S("col")]) == "`col` ASC"
    assert (
        sortkeys_to_socrata([S("col", nulls_first=True, is_reversed=True), S("col2")])
        == "`col` DESC,`col2` ASC"
    )

    with pytest.raises(ValueError):
        sortkeys_to_socrata([S("col"), S("col2", nulls_first=False, is_reversed=True)])


def test_socrata_types():
    assert _socrata_to_pg_type("multipoint") == "json"
    assert _socrata_to_pg_type("unknown type") == "text"


def test_socrata_get_rel_size():
    with open(os.path.join(INGESTION_RESOURCES, "socrata/dataset_metadata.json"), "r") as f:
        socrata_meta = json.load(f)

    assert estimate_socrata_rows_width(columns=["annual_salary"], metadata=socrata_meta) == (
        33702,
        162,
    )
    assert estimate_socrata_rows_width(
        columns=["annual_salary", _long_name_col_sg], metadata=socrata_meta, column_map=_col_map
    ) == (33702, 380)
    assert estimate_socrata_rows_width(
        columns=[_long_name_col_sg, "annual_salary"], metadata=socrata_meta, column_map=_col_map
    ) == (33702, 380)


def test_socrata_mounting(local_engine_empty):
    with open(os.path.join(INGESTION_RESOURCES, "socrata/find_datasets.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.datasets.return_value = socrata_meta
    with mock.patch("sodapy.Socrata", return_value=socrata):
        mount(
            "test/pg_mount",
            "socrata",
            {
                "domain": "example.com",
                "tables": {"some_table": "xzkq-xp2w"},
                "app_token": "some_token",
            },
        )

    assert local_engine_empty.get_full_table_schema("test/pg_mount", "some_table") == [
        TableColumn(
            ordinal=1, name=":id", pg_type="text", is_pk=False, comment="Socrata column ID"
        ),
        TableColumn(
            ordinal=2,
            name="full_or_part_time",
            pg_type="text",
            is_pk=False,
            comment="Whether the employee was employed full- (F) or part-time (P).",
        ),
        TableColumn(
            ordinal=3, name="hourly_rate", pg_type="numeric", is_pk=False, comment=mock.ANY
        ),
        TableColumn(
            ordinal=4, name="salary_or_hourly", pg_type="text", is_pk=False, comment=mock.ANY
        ),
        TableColumn(
            ordinal=5,
            name="job_titles",
            pg_type="text",
            is_pk=False,
            comment="Title of employee at the time when the data was updated.",
        ),
        TableColumn(
            ordinal=6, name="typical_hours", pg_type="numeric", is_pk=False, comment=mock.ANY
        ),
        TableColumn(
            ordinal=7, name="annual_salary", pg_type="numeric", is_pk=False, comment=mock.ANY
        ),
        TableColumn(
            ordinal=8, name=_long_name_col_sg, pg_type="text", is_pk=False, comment=mock.ANY
        ),
        TableColumn(
            ordinal=9,
            name="department",
            pg_type="text",
            is_pk=False,
            comment="Department where employee worked.",
        ),
    ]

    assert local_engine_empty.run_sql(
        "SELECT option_value FROM information_schema.foreign_table_options "
        "WHERE foreign_table_name = 'some_table' "
        "AND foreign_table_schema = 'test/pg_mount' "
        "AND option_name = 'column_map'"
    ) == [(f'{{"{_long_name_col_sg}": "{_long_name_col}"}}',)]


def test_socrata_mounting_error():
    socrata = MagicMock(spec=Socrata)
    socrata.datasets.side_effect = Exception("Unknown response format: text/html; charset=utf-8")
    with mock.patch("sodapy.Socrata", return_value=socrata):
        with pytest.raises(RepositoryNotFoundError):
            mount(
                "test/pg_mount",
                "socrata",
                {
                    "domain": "example.com",
                    "tables": {"some_table": "xzkq-xp2w"},
                    "app_token": "some_token",
                },
            )


def test_socrata_mounting_slug(local_engine_empty):
    with open(os.path.join(INGESTION_RESOURCES, "socrata/find_datasets.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.datasets.return_value = socrata_meta
    with mock.patch("sodapy.Socrata", return_value=socrata):
        mount(
            "test/pg_mount",
            "socrata",
            {
                "domain": "example.com",
                "app_token": "some_token",
            },
        )

    assert local_engine_empty.get_all_tables("test/pg_mount") == [
        "current_employee_names_salaries_and_position_xzkq_xp2w"
    ]


def test_socrata_mounting_missing_tables():
    with open(os.path.join(INGESTION_RESOURCES, "socrata/find_datasets.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.datasets.return_value = socrata_meta
    with mock.patch("sodapy.Socrata", return_value=socrata):
        with pytest.raises(ValueError) as e:
            mount(
                "test/pg_mount",
                "socrata",
                {
                    "domain": "example.com",
                    "tables": {"some_table": "wrong_id"},
                    "app_token": "some_token",
                },
            )

    assert "Some Socrata tables couldn't be found! Missing tables: xzkq-xp2w" in str(e.value)


def test_socrata_fdw():
    with open(os.path.join(INGESTION_RESOURCES, "socrata/dataset_metadata.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.get_metadata.return_value = socrata_meta
    socrata.get_all.return_value = [
        {_long_name_col: "Test", "job_titles": "Test Title", "annual_salary": 123456.0},
        {_long_name_col: "Test2", "job_titles": "Test Title 2", "annual_salary": 789101.0},
    ]

    with mock.patch("sodapy.Socrata", return_value=socrata):
        from splitgraph.ingestion.socrata.fdw import SocrataForeignDataWrapper

        fdw = SocrataForeignDataWrapper(
            fdw_options={
                "table": "xzkq-xp2w",
                "domain": "data.cityofchicago.gov",
                "app_token": "SOME_TOKEN",
                "batch_size": "4200",
                "column_map": json.dumps(_col_map),
            },
            fdw_columns=[_long_name_col_sg, "job_titles", "annual_salary"],
        )

        assert fdw.get_rel_size([], ["annual_salary", _long_name_col_sg]) == (33702, 380)
        assert fdw.can_sort(
            [S(_long_name_col_sg), S("salary", nulls_first=False, is_reversed=True)]
        ) == [S(_long_name_col_sg)]
        assert fdw.explain([], []) == [
            "Socrata query to data.cityofchicago.gov",
            "Socrata dataset ID: xzkq-xp2w",
            "Query: ",
            "Columns: ",
            "Order: :id",
        ]

        assert list(
            fdw.execute(
                quals=[Q("salary", ">", 42)],
                columns=[_long_name_col_sg, "job_titles", "annual_salary"],
                sortkeys=[S(_long_name_col_sg)],
            )
        ) == [
            {_long_name_col_sg: "Test", "job_titles": "Test Title", "annual_salary": 123456.0},
            {_long_name_col_sg: "Test2", "job_titles": "Test Title 2", "annual_salary": 789101.0},
        ]

        assert socrata.get_all.mock_calls == [
            call(
                dataset_identifier="xzkq-xp2w",
                where="(`salary` > 42)",
                select=f"`{_long_name_col}`,`job_titles`,`annual_salary`",
                limit=4200,
                order=f"`{_long_name_col}` ASC",
                exclude_system_fields="false",
            )
        ]


def test_socrata_column_deduplication():
    assert dedupe_sg_schema(
        [
            TableColumn(1, "normal_col", "some_type", True),
            TableColumn(
                2,
                "long_col_but_not_unique_until_the_59th_char_somewhere_there_yep_this_is_different",
                "some_type",
                False,
            ),
            TableColumn(3, "long_col_but_still_unique" * 3, "some_type", False),
            TableColumn(
                4,
                "long_col_but_not_unique_until_the_59th_char_somewhere_there_and_this_is_even_more_so",
                "some_type",
                False,
            ),
            TableColumn(
                5,
                "long_col_but_not_unique_until_the_59th_char_somewhere_there_and_wow_yep_were_done",
                "some_type",
                False,
            ),
        ]
    ) == [
        TableColumn(ordinal=1, name="normal_col", pg_type="some_type", is_pk=True, comment=None),
        TableColumn(
            ordinal=2,
            name="long_col_but_not_unique_until_the_59th_char_somewhere_there_000",
            pg_type="some_type",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=3,
            name="long_col_but_still_uniquelong_col_but_still_uniquelong_col_but_",
            pg_type="some_type",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=4,
            name="long_col_but_not_unique_until_the_59th_char_somewhere_there_001",
            pg_type="some_type",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=5,
            name="long_col_but_not_unique_until_the_59th_char_somewhere_there_002",
            pg_type="some_type",
            is_pk=False,
            comment=None,
        ),
    ]


@pytest.mark.parametrize(
    ("domain", "dataset_id"),
    [
        ("data.cityofchicago.org", "x2n5-8w5q"),
        # Popular for hire vehicles dataset
        ("data.cityofnewyork.us", "8wbx-tsch"),
    ],
)
def test_socrata_smoke(domain, dataset_id, local_engine_empty):
    # This relies on the Socrata API being available, but good to smoke test some popular datasets
    # to make sure the mounting works end-to-end.
    try:
        mount(
            "socrata_mount",
            "socrata",
            {"domain": domain, "tables": {"data": dataset_id}},
        )
        result = local_engine_empty.run_sql("SELECT * FROM socrata_mount.data LIMIT 10")
        assert len(result) == 10
    finally:
        local_engine_empty.delete_schema("socrata_mount")


def test_socrata_data_source_raw_url():
    engine = Mock()
    data_source = SocrataDataSource(
        engine=engine,
        params={"domain": "data.healthcare.gov"},
        tables={"dataset": ([], {"socrata_id": "7h6f-vws8"})},
        credentials={},
    )

    assert data_source.get_raw_url() == {
        "dataset": [
            (
                "text/csv",
                "https://data.healthcare.gov/api/views/7h6f-vws8/rows.csv?accessType=DOWNLOAD",
            )
        ]
    }
