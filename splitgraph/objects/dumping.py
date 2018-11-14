from splitgraph.constants import SPLITGRAPH_META_SCHEMA, PG_HOST, PG_DB, PG_USER, PG_PORT, PG_PWD
import logging
import subprocess


# Utilities to dump objects (SNAP/DIFF) into an external format.
# This used to be a SQL dump (inspect the schema and write a series of statements that recreate the table)
# but we now just shell out to pg_dump -- this does require pg_dump locally to be compatible with the
# driver (pg10).

def dump_object_to_file(object_id, path):
    # Shell out into pg_dump and use its custom binary dump format to dump the object table.
    subprocess.check_output(['pg_dump', '-h', PG_HOST, '-d', PG_DB, '-U', PG_USER, '-p', PG_PORT,
                             '-w',  # Prompt for a password without an extra roundtrip
                             '-t', SPLITGRAPH_META_SCHEMA + '.' + object_id,  # Only dump the single table
                             '-Fc',  # Use the PG compressed binary format
                             '-f', path],
                            input='{}\n'.format(PG_PWD).encode('utf-8'))  # Pipe the password to the process


def load_object_from_file(path):
    logging.info("Loading objects from %s", path)
    # Shell out into pg_restore to restore the object table from the archive.
    # We are basically running some SQL code we downloaded off the internet, so some quick precautions here:
    # we create only tables that don't exist and only in the splitgraph_meta schema.
    # We could also run this with -l to list the contents of the archive and raise if something looks dodgy.
    subprocess.check_output(['pg_restore', path, '-h', PG_HOST, '-d', PG_DB, '-U', PG_USER, '-p', PG_PORT,
                             '-w',  # prompt for a password without an extra roundtrip
                             '-e',  # exit on error
                             '-O',  # don't set ownership information
                             '-x',  # don't set grants
                             '-n', SPLITGRAPH_META_SCHEMA,  # pg_restore still doesn't support restoring into
                             # a different schema
                             # We could pass -t here to only specify a single table but then it wouldn't restore
                             # its indices/constraints etc.
                             '--no-data-for-failed-tables',  # don't append into tables that exist
                             '-Fc'],
                            input='{}\n'.format(PG_PWD).encode('utf-8'))  # Pipe the password to the process.
