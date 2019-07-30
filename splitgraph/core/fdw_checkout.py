"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper that implements
layered querying (read-only queries to Splitgraph tables without materialization)."""
import json
import logging

from splitgraph import Repository, get_engine

try:
    from multicorn import ForeignDataWrapper, ANY
    from multicorn.utils import log_to_postgres
except ImportError:
    # Multicorn not installed (OK if we're not on the engine machine).
    pass

_PG_LOGLEVEL = logging.INFO


class QueryingForeignDataWrapper(ForeignDataWrapper):
    """The actual Multicorn LQ FDW class"""

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

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""

        # Multicorn passes a _set_ of columns to us instead of a list, so the order of iteration through
        # it can randomly change and the order in which we return the tuples might not be the one it expects.
        columns = list(columns)
        # For quals, the more elaborate ones (like table.id = table.name or similar) actually aren't passed here
        # at all and PG filters them out later on.
        cnf_quals = self._quals_to_cnf(quals)
        log_to_postgres("CNF quals: %r" % (cnf_quals,), _PG_LOGLEVEL)

        result = self.table.query(columns, cnf_quals)

        # Return the result back to Multicorn. For Json values, we need to turn them
        # back into Json strings (rather than Python dictionaries).
        result = [
            {k: v if not isinstance(v, dict) else json.dumps(v) for k, v in row.items()}
            for row in result
        ]
        return result

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.
        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.

        """
        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        # Try using a UNIX socket if the engine is local to us
        engine = get_engine(
            self.fdw_options["engine"], bool(self.fdw_options.get("use_socket", False))
        )
        if "object_engine" in self.fdw_options:
            object_engine = get_engine(
                self.fdw_options["object_engine"], bool(self.fdw_options.get("use_socket", False))
            )
        else:
            object_engine = engine
        repository = Repository(
            fdw_options["namespace"],
            self.fdw_options["repository"],
            engine=engine,
            object_engine=object_engine,
        )
        self.table = repository.images[self.fdw_options["image_hash"]].get_table(
            self.fdw_options["table"]
        )
