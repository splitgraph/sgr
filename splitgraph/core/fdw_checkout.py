"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper that implements
layered querying (read-only queries to Splitgraph tables without materialization)."""
import logging
from typing import Optional

import splitgraph.config
from splitgraph.core.output import pretty_size
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import Repository, get_engine
from splitgraph.core.table import QueryPlan

try:
    from multicorn import ForeignDataWrapper, ANY
    from multicorn.utils import log_to_postgres
except ImportError:
    # Multicorn not installed (OK if we're not on the engine machine).
    pass

_PG_LOGLEVEL = logging.INFO


class QueryingForeignDataWrapper(ForeignDataWrapper):
    """The actual Multicorn LQ FDW class"""

    # Allow injecting custom object manager classes
    object_manager_class = ObjectManager

    @staticmethod
    def _quals_to_cnf(quals):
        def _qual_to_cnf(qual):
            if qual.is_list_operator:
                if any(v is None for v in qual.value):
                    # We don't keep track of NULLs so if we get a clause of type
                    # col IN (1,2,3,NULL) we have to look at all objects to see if they contain a NULL.
                    return [[]]
                if qual.list_any_or_all == ANY:
                    # Convert col op ANY(ARRAY[a,b,c...]) into (col op a) OR (col op b)...
                    # which is one single AND clause of multiple ORs
                    return [[(qual.field_name, qual.operator[0], v) for v in qual.value]]
                # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
                # which is multiple AND clauses of one OR each
                return [[(qual.field_name, qual.operator[0], v)] for v in qual.value]

            if qual.value is None:
                return [[]]
            return [[(qual.field_name, qual.operator, qual.value)]]

        return [q for qual in quals for q in _qual_to_cnf(qual) if q != []]

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
        cnf_quals = self._quals_to_cnf(quals)
        plan = self.table.get_query_plan(cnf_quals, columns)

        # Estimate the number of rows -- several precision levels here:
        #   * Number of rows in the actual fragments (using metadata -- no need to download
        #   anything) <- you are here
        #   * calling EXPLAIN on all fragments in filtered_objects (might be pretty expensive
        #     and requires the actual fragments to be present)
        #   * reading binary cstore files?

        return plan.estimated_rows, len(columns) * 10

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        cnf_quals = self._quals_to_cnf(quals)
        plan = self.table.get_query_plan(cnf_quals, columns)
        all_objects = plan.required_objects
        filtered_objects = plan.filtered_objects

        total_size = sum(
            o.size for o in self.table.repository.objects.get_object_meta(filtered_objects).values()
        )

        return [
            "Original Multicorn quals: %r" % quals,
            "CNF quals: %r" % cnf_quals,
            "Objects removed by filter: %d" % (len(all_objects) - len(filtered_objects)),
            "Scan through %d object(s) (%s)" % (len(filtered_objects), pretty_size(total_size)),
        ]

    def get_path_keys(self):
        # Return the PK of the table (unique path something)
        pks = [k[1] for k in self.table.table_schema if k[3]]
        if not pks:
            pks = [k[1] for k in self.table.table_schema]
        return [(tuple(pks), 1)]

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""
        # We assume that columns here is a list of columns (rather than a set)
        # For quals, the more elaborate ones (like table.id = table.name or similar) actually aren't passed here
        # at all and PG filters them out later on.
        cnf_quals = self._quals_to_cnf(quals)

        # Sometimes (e.g. with running a join) Postgres can ask us to "restart" the scan, which means
        # that it doesn't call our end_scan() method. If we have a scan in progress, make sure to
        # call the callback anyway, since otherwise we'll leak temporary tables.
        if self.end_scan_callback:
            self.end_scan_callback(from_fdw=True)
            self.end_scan_callback = None

        queries, self.end_scan_callback, self.plan = self.table.query_indirect(columns, cnf_quals)
        yield from queries

    def end_scan(self):
        if self.end_scan_callback:
            # Call the scan-end callback making sure to use
            # the special hack to avoid deadlocks with Multicorn.
            self.end_scan_callback(from_fdw=True)
            self.end_scan_callback = None

        self.engine.close()
        self.object_engine.close()

    def _initialize_engines(self):
        # Try using a UNIX socket if the engine is local to us
        self.engine = get_engine(
            self.fdw_options["engine"],
            bool(self.fdw_options.get("use_socket", False)),
            use_fdw_params=True,
        )
        if "object_engine" in self.fdw_options:
            self.object_engine = get_engine(
                self.fdw_options["object_engine"],
                bool(self.fdw_options.get("use_socket", False)),
                use_fdw_params=True,
            )
        else:
            self.object_engine = self.engine

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.
        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.

        """
        # Flip the global flag for engine constructors to use (see comment in splitgraph.config).
        splitgraph.config.IN_FDW = True

        # Initialize the logger that will log to the engine's stderr: log timestamp and PID.
        logging.basicConfig(
            format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
            level=splitgraph.config.CONFIG["SG_LOGLEVEL"],
        )

        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self._initialize_engines()

        repository = Repository(
            fdw_options["namespace"],
            self.fdw_options["repository"],
            engine=self.engine,
            object_engine=self.object_engine,
            object_manager=self.object_manager_class(
                object_engine=self.object_engine, metadata_engine=self.engine
            ),
        )
        self.table = repository.images[self.fdw_options["image_hash"]].get_table(
            self.fdw_options["table"]
        )

        # Callback that we have to call when end_scan() is called (release objects and temporary tables).
        self.end_scan_callback = None

        # A QueryPlan object for the last query with stats
        self.plan: Optional[QueryPlan] = None
