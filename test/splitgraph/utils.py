from splitgraph.core.types import TableColumn, TableSchema


def reassign_ordinals(schema: TableSchema) -> TableSchema:
    # When a table is created anew, its ordinals are made consecutive again.
    return [
        TableColumn(i + 1, col.name, col.pg_type, col.is_pk, col.comment)
        for i, col in enumerate(schema)
    ]


def drop_comments(schema: TableSchema) -> TableSchema:
    # For storing object schemata in JSON in /var/lib/splitgraph/objects,
    # we don't store the comments (they're a feature of the table, not the object).
    return [TableColumn(*(t[:4])) for t in schema]
