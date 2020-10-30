import json
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from io import StringIO
from typing import Dict, Any, Optional

from psycopg2._json import Json

from splitgraph.core.repository import Repository
from splitgraph.core.sql import insert
from splitgraph.core.types import TableSchema
from splitgraph.hooks.data_source import DataSource
from splitgraph.hooks.data_source.base import (
    get_ingestion_state,
    INGESTION_STATE_TABLE,
    INGESTION_STATE_SCHEMA,
)
from splitgraph.ingestion.singer import prepare_new_image
from splitgraph.ingestion.singer.db_sync import get_table_name, get_sg_schema, run_patched_sync

SingerConfig = Dict[str, Any]
SingerProperties = Dict[str, Any]
SingerState = Dict[str, Any]


class SingerDataSource(DataSource, ABC):

    supports_load = True
    supports_sync = True

    @abstractmethod
    def get_singer_executable(self):
        pass

    def get_singer_config(self):
        return {**self.params, **self.credentials}

    def _run_singer_discovery(self, config: Optional[SingerConfig] = None):
        executable = self.get_singer_executable()
        args = []

        with tempfile.TemporaryDirectory() as d:
            if config:
                with open(os.path.join(d, "config.json")) as f:
                    json.dump(config, f)
                args.extend(["--config", "config.json"])

            catalog = subprocess.check_output([executable] + args)

        return json.loads(catalog)

    @contextmanager
    def _run_singer(
        self,
        config: Optional[SingerConfig] = None,
        state: Optional[SingerState] = None,
        properties: Optional[SingerProperties] = None,
    ):
        executable = self.get_singer_executable()

        args = [executable]

        with tempfile.TemporaryDirectory() as d:
            if config:
                with open(os.path.join(d, "config.json")) as f:
                    json.dump(config, f)
                args.extend(["--config", "config.json"])

            if state:
                with open(os.path.join(d, "state.json")) as f:
                    json.dump(state, f)
                args.extend(["--state", "state.json"])

            if properties:
                with open(os.path.join(d, "properties.json")) as f:
                    json.dump(properties, f)
                args.extend(["--properties", "properties.json"])

            proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            yield proc

    def sync(self, repository: Repository, image_hash: Optional[str]):
        config = self.get_singer_config()
        state = None
        properties = None

        state = get_ingestion_state(repository, image_hash)

        base_image, new_image_hash = prepare_new_image(repository, image_hash)

        # Run the sink + target and capture the stdout (new state)
        output_stream = StringIO()
        with self._run_singer(config, state, properties) as proc:
            run_patched_sync(
                repository,
                base_image,
                new_image_hash,
                delete_old=True,
                failure="keep_both",
                input_stream=proc.stdout,
                output_stream=output_stream,
            )

        new_state = output_stream.read()

        # Add a table to the new image with the new state
        repository.object_engine.create_table(
            schema=None,
            table=INGESTION_STATE_TABLE,
            schema_spec=INGESTION_STATE_SCHEMA,
            temporary=True,
        )
        repository.object_engine.run_sql(
            insert(INGESTION_STATE_TABLE, ["state"], "pg_temp"), (Json(new_state),)
        )

        object_id = repository.objects.create_base_fragment(
            "pg_temp",
            INGESTION_STATE_TABLE,
            repository.namespace,
            table_schema=INGESTION_STATE_SCHEMA,
        )
        # Overwrite the existing table
        repository.objects.overwrite_table(
            repository, new_image_hash, INGESTION_STATE_TABLE, INGESTION_STATE_SCHEMA, [object_id]
        )

        repository.commit_engines()

    def introspect(self) -> Dict[str, TableSchema]:
        config = self.get_singer_config()
        singer_schema = self._run_singer_discovery(config)

        result = {}
        for stream in singer_schema["streams"]:
            stream_name = get_table_name(stream)
            stream_schema = get_sg_schema(stream)
            result[stream_name] = stream_schema
        return result


class GenericSingerDataSource(SingerDataSource):
    def get_singer_executable(self):
        return self.params["tap_path"]

    credentials_schema = {"type": "object"}
    params_schema = {
        "type": "object",
        "properties": {"tap_path": {"type": "string"}},
        "required": "tap_path",
    }
