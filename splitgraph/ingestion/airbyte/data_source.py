import json
import logging
import os
import re
import socket
from abc import ABC
from contextlib import contextmanager
from random import getrandbits
from typing import Optional, Dict, cast, List, Tuple

import docker.errors
import pydantic
from airbyte_cdk.models import (
    AirbyteCatalog,
    ConfiguredAirbyteCatalog,
    AirbyteMessage,
)
from docker import DockerClient
from docker.models.containers import Container

from splitgraph.commandline.engine import get_docker_client, copy_to_container
from splitgraph.core.repository import Repository
from splitgraph.core.types import (
    SyncState,
    TableInfo,
    IntrospectionResult,
    TableParams,
)
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.hooks.data_source.base import (
    SyncableDataSource,
    get_ingestion_state,
    prepare_new_image,
)
from .docker_utils import add_files, remove_at_end, wait_not_failed, build_command
from .utils import (
    AirbyteConfig,
    _airbyte_message_reader,
    _store_raw_airbyte_tables,
    _store_processed_airbyte_tables,
    get_sg_schema,
    select_streams,
)
from ..singer.common import store_ingestion_state, add_timestamp_tags


class AirbyteDataSource(SyncableDataSource, ABC):
    """Generic data source for Airbyte-compliant sources.
    We run ingestion by combining an Airbyte source and the Airbyte Postgres destination.
    """

    docker_image: Optional[str] = None
    airbyte_name: Optional[str] = None
    receiver_image = "airbyte/destination-postgres:latest"
    normalization_image = "airbyte/normalization:0.1.36"
    cursor_overrides: Optional[Dict[str, List[str]]] = None
    primary_key_overrides: Optional[Dict[str, List[str]]] = None

    def get_airbyte_config(self) -> AirbyteConfig:
        return {**self.params, **self.credentials}

    def _sync(
        self,
        schema: str,
        state: Optional[SyncState] = None,
        tables: Optional[TableInfo] = None,
    ) -> SyncState:
        # We override the main sync() instead
        pass

    def load(self, repository: "Repository", tables: Optional[TableInfo] = None) -> str:
        return self.sync(repository, image_hash=None, tables=tables, use_state=False)

    def _make_postgres_config(self, engine: PostgresEngine, schema: str) -> AirbyteConfig:
        return {
            "host": engine.conn_params["SG_ENGINE_HOST"],
            "port": int(engine.conn_params["SG_ENGINE_PORT"] or 5432),
            "username": engine.conn_params["SG_ENGINE_USER"],
            "password": engine.conn_params["SG_ENGINE_PWD"],
            "database": engine.conn_params["SG_ENGINE_DB_NAME"],
            "schema": schema,
        }

    def _run_discovery(self, config: Optional[AirbyteConfig] = None) -> AirbyteCatalog:
        # Create Docker container
        client = get_docker_client()

        with self._source_container(
            client, config, catalog=None, state=None, discover=True
        ) as container:
            # Copy config into /
            copy_to_container(
                container,
                source_path=None,
                target_path="/config.json",
                data=json.dumps(config or {}).encode(),
            )

            container.start()
            wait_not_failed(container, mirror_logs=False)

            # Grab the catalog from stdout
            for line in container.logs(stream=True):
                message = AirbyteMessage.parse_raw(line)
                if message.catalog:
                    logging.info("Catalog: %s", message.catalog)
                    return message.catalog
        raise AssertionError("No catalog output!")

    def sync(
        self,
        repository: Repository,
        image_hash: Optional[str] = None,
        tables: Optional[TableInfo] = None,
        use_state: bool = True,
    ) -> str:
        # https://docs.airbyte.io/understanding-airbyte/airbyte-specification

        # Select columns and streams (full_refresh/incremental, cursors)
        src_config = self.get_airbyte_config()
        catalog = self._run_discovery(src_config)
        configured_catalog = select_streams(
            catalog,
            tables,
            sync=use_state,
            cursor_overrides=self.cursor_overrides,
            primary_key_overrides=self.primary_key_overrides,
        )
        logging.info("Configured catalog: %s", configured_catalog)

        # Load ingestion state
        base_image, new_image_hash = prepare_new_image(repository, image_hash)
        state = get_ingestion_state(repository, image_hash) if use_state else None
        logging.info("Current ingestion state: %s", state)

        # Set up a staging schema for the data
        # Delete the slashes or Airbyte will do it for us.
        staging_schema = "sg_tmp_" + repository.to_schema().replace("/", "_").replace("-", "_")
        repository.object_engine.delete_schema(staging_schema)
        repository.object_engine.create_schema(staging_schema)
        repository.commit_engines()

        dst_config = self._make_postgres_config(repository.object_engine, staging_schema)

        client = get_docker_client()

        # We want the receiver to connect to the same engine that we're connected to. If we're
        # running on the host, that means using our own connection parameters and running the
        # receiver with net:host. Inside Docker we have to use the host's Docker socket and
        # attach the container to our own network so that it can also use our own params.
        if os.path.exists("/.dockerenv"):
            our_container_id = client.containers.get(socket.gethostname()).id
            network_mode = f"container:{our_container_id}"
        else:
            network_mode = "host"

        # Run the Airbyte source and receiver and pipe data between them, writing it
        # out into a temporary schema.

        logging.info("Running Airbyte EL process")
        dest_files, new_state, sync_modes = self._run_airbyte_el(
            client, network_mode, src_config, dst_config, configured_catalog, state
        )

        # At this stage, Airbyte wrote out the raw tables into the staging schema: they have
        # the form _airbyte_tmp_STREAM_NAME and schema (hash, raw_json, date). These raw tables
        # are append-or-truncate only, so we append/replace them in the existing Splitgraph image
        # at this stage.

        logging.info("Storing raw tables as Splitgraph images")
        raw_tables = _store_raw_airbyte_tables(
            repository,
            new_image_hash,
            staging_schema,
            sync_modes,
            default_sync_mode="append" if use_state else "overwrite",
        )

        # Run normalization
        # This converts the raw Airbyte tables (with JSON) into actual tables with fields.
        # We first replace the raw table fragments that Airbyte wrote out with the actual full
        # tables, checked out via LQ so that dbt (run by Airbyte's normalization container) can
        # scan through them and build the actual ingested data.

        new_image = repository.images.by_hash(new_image_hash)
        repository.object_engine.delete_schema(staging_schema)
        repository.object_engine.create_schema(staging_schema)
        new_image.lq_checkout(staging_schema, only_tables=raw_tables)
        repository.commit_engines()

        # Now run the normalization container
        # This actually always recreates the normalized tables from scratch.
        # https://github.com/airbytehq/airbyte/issues/4286
        logging.info("Running Airbyte T step (normalization)")
        with self._normalization_container(client, network_mode) as normalization_container:
            add_files(normalization_container, dest_files)
            normalization_container.start()
            wait_not_failed(normalization_container, mirror_logs=True)

        logging.info("Storing processed Airbyte tables")
        _store_processed_airbyte_tables(repository, new_image_hash, staging_schema)

        store_ingestion_state(
            repository,
            new_image_hash,
            current_state=state,
            new_state=json.dumps(new_state) if new_state else "{}",
        )
        add_timestamp_tags(repository, new_image_hash)

        repository.commit_engines()

        return new_image_hash

    def _run_airbyte_el(
        self,
        client: docker.DockerClient,
        network_mode: str,
        src_config: AirbyteConfig,
        dst_config: AirbyteConfig,
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[SyncState],
    ) -> Tuple[List[Tuple[str, str]], Optional[SyncState], Dict[str, str]]:
        with self._source_container(
            client, src_config, catalog, state
        ) as source, self._destination_container(
            client, network_mode, dst_config, catalog
        ) as destination:

            # Set up the files in src/dest containers
            add_files(
                source,
                [
                    ("config", json.dumps(src_config)),
                    (
                        "catalog",
                        catalog.json(exclude_unset=True, exclude_defaults=True),
                    ),
                    ("state", json.dumps(state)),
                ],
            )

            dest_files = [
                ("config", json.dumps(dst_config)),
                (
                    "catalog",
                    catalog.json(exclude_unset=True, exclude_defaults=True),
                ),
            ]

            add_files(destination, dest_files)

            dest_socket = destination.attach_socket(params={"stdin": 1, "stream": 1})
            dest_socket._writing = True
            src_socket = source.attach(stdout=True, stream=True, logs=True)

            source.start()
            destination.start()

            # Pipe messages from the source to the destination
            for raw, message in _airbyte_message_reader(src_socket):
                if message.state or message.record:
                    out = (raw + "\n").encode()
                    logging.debug("Writing message %s", out)
                    while out:
                        written = dest_socket.write(out)
                        out = out[written:]

                    dest_socket.flush()
                elif message.log:
                    logging.info(message.log.message)

            # NB this is the magic thing that makes the socket actually close and kick the container so that
            # it sees that STDIN is closed too.
            # Neither of these work
            #   dest_socket.close()
            #   dest_socket._sock.close()
            # Thank you Docker.
            # https://github.com/d11wtq/dockerpty/blob/f8d17d893c6758b7cc25825e99f6b02202632a97/dockerpty/io.py#L182
            # https://github.com/docker/docker-py/issues/1507
            # https://github.com/docker/docker-py/issues/983#issuecomment-492513718
            os.close(dest_socket._sock.fileno())

            wait_not_failed(source)
            wait_not_failed(destination)
            dest_logs = destination.logs(stream=True)

            # Grab the state from stdout
            new_state: Optional[SyncState] = None
            table_sync_modes: Dict[str, str] = {}

            for line in dest_logs:
                line = line.decode()
                logging.info("%s: %s", destination.name, line)

                # Another thing we want to find out from the destination is how it normalized
                # raw stream names (which can be any UTF-8 string) into the output table names
                # (_airbyte_raw_xxx) and the sync mode (overwrite/append). This is because
                # we get Airbyte to always write into empty tables (merging them into the full
                # Splitgraph tables after the fact) but we need to know if it meant to truncate
                # or append to those tables.
                # The PG destination outputs a log message in this format:
                #
                # Write config: WriteConfig{streamName=sites, namespace=null, outputSchemaName=sg_tmp_airbyte_google_test, tmpTableName=_airbyte_tmp_cav_sites, outputTableName=_airbyte_raw_sites, syncMode=overwrite}
                #
                # So we can grab the outputTableName and syncMode to find these out.
                #
                # Other ways of doing this: detect TRUNCATE on our tables (this is probably the best
                # long-term solution, since we want to turn this into just writing to the DDN);
                # back out the table names from the stream names by reimplementing/copying
                # https://github.com/airbytehq/airbyte/blob/441435a373f03262ce87a53505b1863d5554cc6c/airbyte-integrations/bases/base-normalization/normalization/transform_catalog/destination_name_transformer.py#L53.
                match = re.match(r".*outputTableName=([^,]+), syncMode=(\w+)", line)
                if match:
                    raw_table, sync_mode = match.groups()
                    table_sync_modes[raw_table] = sync_mode

                # Also find the STATE message in the log denoting the new connector bookmark.
                if not line.startswith("{"):
                    continue
                try:
                    message = AirbyteMessage.parse_raw(line)
                except pydantic.ValidationError:
                    logging.warning("Couldn't parse message, continuing")
                    continue
                if message.state:
                    new_state = SyncState(message.state.data)
        logging.info("New state: %s", new_state)
        return dest_files, new_state, table_sync_modes

    @contextmanager
    def _source_container(
        self,
        client: DockerClient,
        config: Optional[AirbyteConfig],
        catalog: Optional[ConfiguredAirbyteCatalog],
        state: Optional[SyncState],
        discover: bool = False,
    ) -> Container:
        client.images.pull(self.docker_image)
        container_name = "sg-ab-src-{:08x}".format(getrandbits(64))
        if discover:
            command = ["discover"] + build_command([("config", config)])
        else:
            command = ["read"] + build_command(
                [("config", config), ("state", state), ("catalog", catalog)]
            )
        container = client.containers.create(
            image=self.docker_image, name=container_name, command=command
        )
        with remove_at_end(container):
            yield container

    @contextmanager
    def _destination_container(
        self,
        client: DockerClient,
        network_mode: str,
        config: AirbyteConfig,
        catalog: ConfiguredAirbyteCatalog,
    ) -> Container:
        # Create the Postgres receiver container
        client.images.pull(self.receiver_image)
        destination_container_name = "sg-ab-dst-{:08x}".format(getrandbits(64))
        command = ["write"] + build_command([("config", config), ("catalog", catalog)])
        container = client.containers.create(
            image=self.receiver_image,
            name=destination_container_name,
            command=command,
            network_mode=network_mode,
            stdin_open=True,
        )
        with remove_at_end(container):
            yield container

    @contextmanager
    def _normalization_container(self, client: DockerClient, network_mode: str) -> Container:
        client.images.pull(self.normalization_image)
        # https://github.com/airbytehq/airbyte/blob/830fac6b648263e1add3589294fcabf4bee6fd39/airbyte-workers/src/main/java/io/airbyte/workers/normalization/DefaultNormalizationRunner.java#L111
        command = [
            "run",
            "--integration-type",
            "postgres",
            "--config",
            "/config.json",
            "--catalog",
            "/catalog.json",
        ]
        container = client.containers.create(
            image=self.normalization_image,
            name="sg-ab-norm-{:08x}".format(getrandbits(64)),
            command=command,
            network_mode=network_mode,
        )

        with remove_at_end(container):
            yield container

    def introspect(self) -> IntrospectionResult:
        config = self.get_airbyte_config()
        catalog = self._run_discovery(config)

        result = IntrospectionResult({})
        for stream in catalog.streams:
            stream_name = stream.name
            stream_schema = get_sg_schema(stream)
            result[stream_name] = (stream_schema, cast(TableParams, {}))
        return result
