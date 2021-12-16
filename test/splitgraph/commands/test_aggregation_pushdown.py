import pytest
from decimal import Decimal

from test.splitgraph.conftest import _mount_elasticsearch
from splitgraph.core.repository import Repository
from splitgraph.engine import get_engine

ES_MNT = Repository.from_schema("test/es_mount")


@pytest.mark.mounting
def test_elasticsearch_aggregation_functions_only(local_engine_empty):
    try:
        _mount_elasticsearch(ES_MNT)

        query = """SELECT max(account_number), avg(balance), max(balance),
            sum(balance), min(age), avg(age)
            FROM "test/es_mount".account
        """

        # Ensure query is going to be aggregated on the foreign server
        result = get_engine().run_sql("EXPLAIN " + query)
        assert len(result) == 1
        import logging

        logging.error(result)

        # Ensure results are correct
        result = get_engine().run_sql(query)
        assert len(result) == 1

        # Assert aggregation result
        assert result[0] == (999, 25714.837, 49989, 25714837, 20, Decimal("30.171"))
    finally:
        ES_MNT.delete()
