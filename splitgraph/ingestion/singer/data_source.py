import json
import logging
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from io import StringIO
from threading import Thread
from typing import Dict, Any, Optional, cast

from psycopg2._json import Json
from psycopg2.sql import Identifier, SQL

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableSchema
from splitgraph.exceptions import DataSourceError
from splitgraph.hooks.data_source.base import (
    get_ingestion_state,
    INGESTION_STATE_TABLE,
    INGESTION_STATE_SCHEMA,
    TableInfo,
    prepare_new_image,
    SyncableDataSource,
    SyncState,
)
from splitgraph.ingestion.singer.db_sync import get_table_name, get_sg_schema, run_patched_sync

SingerConfig = Dict[str, Any]
SingerProperties = Dict[str, Any]
SingerState = Dict[str, Any]


class SingerDataSource(SyncableDataSource, ABC):
    @abstractmethod
    def get_singer_executable(self):
        raise NotImplementedError

    def get_singer_config(self):
        return {**self.params, **self.credentials}

    def _run_singer_discovery(self, config: Optional[SingerConfig] = None) -> SingerProperties:
        executable = self.get_singer_executable()
        args = [executable]

        with tempfile.TemporaryDirectory() as d:
            self._add_file_arg(config, "config", d, args)

            catalog = subprocess.check_output(args + ["--discover"])

        return cast(SingerProperties, json.loads(catalog))

    @staticmethod
    def _add_file_arg(var, var_name, dir, args):
        if var:
            path = os.path.join(dir, "%s.json" % var_name)
            with open(path, "w") as f:
                json.dump(var, f)
            args.extend(["--%s" % var_name, path])

    @contextmanager
    def _run_singer(
        self,
        config: Optional[SingerConfig] = None,
        state: Optional[SingerState] = None,
        properties: Optional[SingerProperties] = None,
        catalog: Optional[SingerProperties] = None,
    ):
        executable = self.get_singer_executable()

        args = [executable]

        with tempfile.TemporaryDirectory() as d:
            self._add_file_arg(config, "config", d, args)
            self._add_file_arg(state, "state", d, args)
            self._add_file_arg(properties, "properties", d, args)
            self._add_file_arg(catalog, "catalog", d, args)

            logging.info("Running Singer tap. Arguments: %s", args)
            proc = subprocess.Popen(
                args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            def _emit_output(pipe):
                for line in iter(pipe.readline, b""):
                    logging.info(line.decode("utf-8").rstrip("\n"))

            # Emit stderr as a separate thread.
            t = Thread(target=_emit_output, args=(proc.stderr,))
            t.daemon = True
            t.start()

            try:
                yield proc
            finally:
                proc.wait()
                if proc.returncode:
                    raise DataSourceError(
                        "Failed running Singer data source. Exit code: %d." % proc.returncode
                    )

    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> SyncState:
        # We override the main sync() instead
        pass

    def sync(
        self,
        repository: Repository,
        image_hash: Optional[str] = None,
        tables: Optional[TableInfo] = None,
    ) -> str:
        config = self.get_singer_config()
        catalog = self._run_singer_discovery(config)
        properties = select_streams(catalog, tables=tables)

        base_image, new_image_hash = prepare_new_image(repository, image_hash)
        state = get_ingestion_state(repository, image_hash)
        logging.info("State: %s", state)

        # Run the sink + target and capture the stdout (new state)
        output_stream = StringIO()

        # TODO some taps use catalog, some use properties
        with self._run_singer(config, state, properties=properties) as proc:
            run_patched_sync(
                repository,
                base_image,
                new_image_hash,
                delete_old=True,
                failure="keep_both",
                input_stream=proc.stdout,
                output_stream=output_stream,
            )

        new_state = output_stream.getvalue()
        logging.info("New state: %s", new_state)

        # Add a table to the new image with the new state
        repository.object_engine.create_table(
            schema=None,
            table=INGESTION_STATE_TABLE,
            schema_spec=INGESTION_STATE_SCHEMA,
            temporary=True,
        )
        # NB: new_state here is a JSON-serialized string, so we don't wrap it into psycopg2.Json()
        repository.object_engine.run_sql(
            SQL("INSERT INTO pg_temp.{} (timestamp, state) VALUES(now(), %s)").format(
                Identifier(INGESTION_STATE_TABLE)
            ),
            (new_state,),
        )

        object_id = repository.objects.create_base_fragment(
            "pg_temp",
            INGESTION_STATE_TABLE,
            repository.namespace,
            table_schema=INGESTION_STATE_SCHEMA,
        )

        # If the state exists already, overwrite it; otherwise, add new state table.
        if state:
            repository.objects.overwrite_table(
                repository,
                new_image_hash,
                INGESTION_STATE_TABLE,
                INGESTION_STATE_SCHEMA,
                [object_id],
            )
        else:
            repository.objects.register_tables(
                repository,
                [(new_image_hash, INGESTION_STATE_TABLE, INGESTION_STATE_SCHEMA, [object_id])],
            )

        repository.commit_engines()
        return new_image_hash

    def introspect(self) -> Dict[str, TableSchema]:
        config = self.get_singer_config()
        singer_schema = self._run_singer_discovery(config)

        result = {}
        for stream in singer_schema["streams"]:
            stream_name = get_table_name(stream)
            stream_schema = get_sg_schema(stream)
            result[stream_name] = stream_schema
        return result


def select_streams(
    catalog: SingerProperties, tables: Optional[TableInfo] = None
) -> SingerProperties:
    tables = list(tables) if tables else None

    for stream in catalog["streams"]:
        stream_name = get_table_name(stream)
        # TODO tmp hack
        # if stream_name == "issue_events": # not in [
        # #     "stargazers",
        # #     "assignees",
        # #     "team_memberships",
        # #     "teams",
        # #     "team_members",
        # #     "review_comments",
        # #     "reviews",
        # #     "collaborators",
        # #     "commit_comments",
        # #     "pull_requests",
        # # ]:
        #     continue
        if not tables or stream_name in tables:
            # TODO should be selecting the empty breadcrumb here instead?
            stream["metadata"][0]["metadata"]["selected"] = True
            stream["schema"]["selected"] = True

    return catalog


class GenericSingerDataSource(SingerDataSource):
    @classmethod
    def get_name(cls) -> str:
        return "Generic Singer tap"

    @classmethod
    def get_description(cls) -> str:
        return "Generic Singer tap"

    def get_singer_executable(self):
        return self.params["tap_path"]

    credentials_schema = {"type": "object"}
    params_schema = {
        "type": "object",
        "properties": {"tap_path": {"type": "string"}},
        "required": ["tap_path"],
    }
