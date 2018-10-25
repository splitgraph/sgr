from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, Repository
from splitgraph.meta_handler.common import select, insert, ensure_metadata_schema
from splitgraph.meta_handler.images import get_closest_parent_image_object
from splitgraph.meta_handler.objects import register_object, register_table
from splitgraph.pg_utils import get_full_table_schema


def repository_exists(conn, repository):
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT 1 FROM {}.images WHERE namespace = %s AND repository = %s")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (repository.namespace, repository.repository))
        return cur.fetchone() is not None


def register_repository(conn, repository, initial_image, tables, table_object_ids):
    with conn.cursor() as cur:
        cur.execute(insert("images", ("image_hash", "namespace", "repository", "parent_id", "created")),
                    (initial_image, repository.namespace, repository.repository, None, datetime.now()))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        cur.execute(insert("snap_tags", ("namespace", "repository", "image_hash", "tag")),
                    (repository.namespace, repository.repository, initial_image, "HEAD"))
        for t, ti in zip(tables, table_object_ids):
            # Register the tables and the object IDs they were stored under.
            # They're obviously stored as snaps since there's nothing to diff to...
            register_object(conn, ti, 'SNAP', repository.namespace, None)
            register_table(conn, repository, t, initial_image, ti)


def unregister_repository(conn, repository):
    with conn.cursor() as cur:
        for meta_table in ["tables", "snap_tags", "images", "remotes"]:
            cur.execute(SQL("DELETE FROM {}.{} WHERE namespace = %s AND repository = %s")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA),
                                Identifier(meta_table)),
                        (repository.namespace, repository.repository))


def get_current_repositories(conn):
    ensure_metadata_schema(conn)
    with conn.cursor() as cur:
        cur.execute(select("snap_tags", "namespace, repository, image_hash", "tag = 'HEAD'"))
        return [(Repository(n, r), i) for n, r, i in cur.fetchall()]


def get_remote_for(conn, repository, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute(select("remotes", "remote_conn_string, remote_namespace, remote_repository",
                           "namespace = %s AND repository = %s AND remote_name = %s"),
                    (repository.namespace, repository.repository, remote_name))
        result = cur.fetchone()
        if result is None:
            return result
        cs, ns, re = result
        return cs, Repository(ns, re)


def add_remote(conn, repository, remote_conn, remote_repository, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute(insert("remotes", ("namespace", "repository", "remote_name", "remote_conn_string",
                                       "remote_namespace", "remote_repository")),
                    (repository.namespace, repository.repository, remote_name,
                     remote_conn, remote_repository.namespace, remote_repository.repository))


def table_schema_changed(conn, repository, table_name, image_1, image_2=None):
    snap_1 = get_closest_parent_image_object(conn, repository, table_name, image_1)[0]
    # image_2 = None here means the current staging area.
    if image_2 is not None:
        snap_2 = get_closest_parent_image_object(conn, repository, table_name, image_2)[0]
        return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
               get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_2)
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
           get_full_table_schema(conn, repository.to_schema(), table_name)


def get_schema_at(conn, repository, table_name, image_hash):
    snap_1 = get_closest_parent_image_object(conn, repository, table_name, image_hash)[0]
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1)
