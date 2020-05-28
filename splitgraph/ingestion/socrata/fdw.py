"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper
that communicates to Socrata datasets using sodapy."""
import json
import logging

import splitgraph.config
from splitgraph.ingestion.socrata.querying import (
    estimate_socrata_rows_width,
    quals_to_socrata,
    cols_to_socrata,
)

try:
    from multicorn import ForeignDataWrapper, ANY
    from multicorn.utils import log_to_postgres
except ImportError:
    # Multicorn not installed (OK if we're not on the engine machine).
    pass

_PG_LOGLEVEL = logging.INFO


def to_json(row):
    return {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()}


class SocrataForeignDataWrapper(ForeignDataWrapper):
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
            return estimate_socrata_rows_width(columns, self.table_meta)
        except Exception:
            return 1000000, len(columns) * 10

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        return ["Socrata query to %s" % self.domain, "Socrata dataset ID: %s" % self.table]

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""
        query = quals_to_socrata(quals)
        select = cols_to_socrata(columns)

        result = self.client.get(dataset_identifier=self.table, where=query, select=select)

        for r in result:
            r = to_json(r)
            logging.info("returning %s", r)
            yield r

        # yield from result

    @property
    def table_meta(self):
        if not self._metadata:
            self._metadata = self.client.get_metadata(dataset_identifier=self.table)
        return self._metadata

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
            level=splitgraph.config.CONFIG["SG_LOGLEVEL"],
        )

        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self.table = self.fdw_options["table"]
        self.app_token = self.fdw_options.get("app_token")
        self.domain = self.fdw_options["domain"]
        self.client = Socrata(domain=self.domain, app_token=self.app_token)

        # Cached table metadata
        self._metadata = None
