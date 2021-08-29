"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper
that communicates to Socrata datasets using sodapy."""
import json
import logging
from typing import Any, Dict, Optional

import splitgraph.config
from splitgraph.config import get_singleton
from splitgraph.ingestion.socrata.querying import (
    cols_to_socrata,
    estimate_socrata_rows_width,
    quals_to_socrata,
    sortkeys_to_socrata,
)

try:
    from multicorn import ANY, ForeignDataWrapper
except ImportError:
    # Multicorn not installed (OK if we're not on the engine -- tests).
    ForeignDataWrapper = object
    ANY = object()

_PG_LOGLEVEL = logging.INFO


def to_json(row, columns, column_map):
    result = {}
    for col in columns:
        val = row.get(column_map.get(col, col))
        if isinstance(val, (dict, list)):
            val = json.dumps(val)
        result[col] = val
    return result


class SocrataForeignDataWrapper(ForeignDataWrapper):
    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.
        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.

        """
        # Initialize the logger that will log to the engine's stderr: log timestamp and PID.
        from sodapy import Socrata

        logging.basicConfig(
            format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
            level=get_singleton(splitgraph.config.CONFIG, "SG_LOGLEVEL"),
        )

        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self.table = self.fdw_options["table"]

        # Mappings from SG to Socrata columns (for query building)
        self.column_map = json.loads(self.fdw_options.get("column_map") or "{}")

        self.app_token = self.fdw_options.get("app_token")
        self.domain = self.fdw_options["domain"]
        self.batch_size = int(self.fdw_options.get("batch_size", 1000))
        self.client = Socrata(domain=self.domain, app_token=self.app_token)

        # Cached table metadata
        self._metadata: Optional[Dict[str, Any]] = None

    def can_sort(self, sortkeys):
        """
        :param sortkeys: List of SortKey
        :return: List of SortKey the FDW can sort on
        """

        # Mostly, we can push all sort clauses down to Socrata.
        logging.debug("can_sort %r", sortkeys)

        supported = []
        for key in sortkeys:
            # Socrata sorts nulls first by default (TODO both asc and desc?)
            if key.nulls_first != key.is_reversed:
                continue
            supported.append(key)
        return supported

    def get_rel_size(self, quals, columns):
        """
        Method called from the planner to estimate the resulting relation
        size for a scan.
        It will help the planner in deciding between different types of plans,
        according to their costs.
        Args:
            quals (list): A list of Qual instances describing the filters
                applied to this scan.
            columns (list): The list of columns that must be returned.
        Returns:
            A tuple of the form (expected_number_of_rows, avg_row_width (in bytes))
        """
        try:
            return estimate_socrata_rows_width(columns, self.table_meta, self.column_map)
        except Exception:
            logging.exception("Failed planning Socrata query, returning dummy values")
            return 1000000, len(columns) * 10

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        query = quals_to_socrata(quals, self.column_map)
        select = cols_to_socrata(columns, self.column_map)
        order = sortkeys_to_socrata(sortkeys, self.column_map)

        return [
            "Socrata query to %s" % self.domain,
            "Socrata dataset ID: %s" % self.table,
            "Query: %s" % query,
            "Columns: %s" % select,
            "Order: %s" % order,
        ]

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""
        query = quals_to_socrata(quals, self.column_map)
        select = cols_to_socrata(columns, self.column_map)
        order = sortkeys_to_socrata(sortkeys, self.column_map)

        logging.debug("Socrata query: %r, select: %r, order: %r", query, select, order)

        # TODO offsets stop working after some point?
        result = self.client.get_all(
            dataset_identifier=self.table,
            where=query,
            select=select,
            limit=self.batch_size,
            order=order,
            exclude_system_fields="false",
        )

        for r in result:
            r = to_json(r, columns, self.column_map)
            yield r

    @property
    def table_meta(self):
        if not self._metadata:
            self._metadata = self.client.get_metadata(dataset_identifier=self.table)
        return self._metadata
