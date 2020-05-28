import json
import os
from unittest import mock
from unittest.mock import MagicMock

import pytest
from sodapy import Socrata
from test.splitgraph.conftest import INGESTION_RESOURCES

from splitgraph.core.types import TableColumn
from splitgraph.ingestion.socrata.mount import mount_socrata
from splitgraph.ingestion.socrata.querying import estimate_socrata_rows_width, quals_to_socrata, ANY


class Q:
    def __init__(self, col, op, val, is_list=False, is_list_any=True):
        self.field_name = col
        self.operator = (op,)
        self.value = val

        self.is_list_operator = is_list
        # Code doesn't check for the actual "ALL" value so we can put whatever here
        self.list_any_or_all = ANY if is_list_any else "ALL"


@pytest.mark.parametrize(
    "quals,expected",
    [
        ([Q("some_col", ">", 42)], "(some_col > 42)"),
        (
            [
                Q("some_col", ">", 42),
                Q("some_other_col", "~~", "%te'st%"),
                Q("some_other_col", "^", 1.23),
            ],
            "(some_col > 42) AND (some_other_col LIKE '%te''st%') AND (TRUE)",
        ),
        (
            [
                Q("some_col", "=", [1, 2, 3, 4, 5], is_list=True),
                Q("some_other_col", "<>", [1, 2, 3, 4, 5], is_list=True, is_list_any=False),
            ],
            "(some_col IN (1,2,3,4,5) AND ((some_other_col <> 1) AND (some_other_col <> 2) "
            "AND (some_other_col <> 3) AND (some_other_col <> 4) AND (some_other_col <> 5))",
        ),
    ],
)
def test_socrata_quals(quals, expected):
    assert quals_to_socrata(quals) == expected


def test_socrata_get_rel_size():
    with open(os.path.join(INGESTION_RESOURCES, "socrata/dataset_metadata.json"), "r") as f:
        socrata_meta = json.load(f)

    assert estimate_socrata_rows_width(columns=["annual_salary"], metadata=socrata_meta) == (
        33702,
        162,
    )
    assert estimate_socrata_rows_width(
        columns=["annual_salary", "name"], metadata=socrata_meta
    ) == (33702, 380)
    assert estimate_socrata_rows_width(
        columns=["name", "annual_salary"], metadata=socrata_meta
    ) == (33702, 380)


def test_socrata_mounting(local_engine_empty):
    with open(os.path.join(INGESTION_RESOURCES, "socrata/find_datasets.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.datasets.return_value = socrata_meta
    with mock.patch("sodapy.Socrata", return_value=socrata):

        mount_socrata(
            "test/pg_mount",
            None,
            None,
            None,
            None,
            "example.com",
            {"some_table": "xzkq-xp2w"},
            "some_token",
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
        TableColumn(ordinal=8, name="name", pg_type="text", is_pk=False, comment=mock.ANY),
        TableColumn(
            ordinal=9,
            name="department",
            pg_type="text",
            is_pk=False,
            comment="Department where employee worked.",
        ),
    ]


def test_socrata_mounting_slug(local_engine_empty):
    with open(os.path.join(INGESTION_RESOURCES, "socrata/find_datasets.json"), "r") as f:
        socrata_meta = json.load(f)

    socrata = MagicMock(spec=Socrata)
    socrata.datasets.return_value = socrata_meta
    with mock.patch("sodapy.Socrata", return_value=socrata):

        mount_socrata(
            "test/pg_mount", None, None, None, None, "example.com", None, "some_token",
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
            mount_socrata(
                "test/pg_mount",
                None,
                None,
                None,
                None,
                "example.com",
                {"some_table": "wrong_id"},
                "some_token",
            )

    assert "Some Socrata tables couldn't be found! Missing tables: xzkq-xp2w" in str(e.value)
