import logging
from typing import Dict, Any, Iterable, Generator, Tuple, Optional, List

from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStream,
    AirbyteCatalog,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    SyncMode,
    DestinationSyncMode,
)
from target_postgres.db_sync import column_type

from splitgraph.config import get_singleton, CONFIG
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableSchema, TableColumn, TableInfo
from splitgraph.exceptions import TableNotFoundError

AirbyteConfig = Dict[str, Any]
AIRBYTE_RAW = "_airbyte_raw"


def _airbyte_message_reader(
    stream: Iterable[bytes],
) -> Generator[AirbyteMessage, None, None]:
    buffer = b""
    for data in stream:
        # Accumulate data in a buffer until we get a newline, at which point we can
        # decode the message and filter out records/state.
        buffer = buffer + data
        if b"\n" not in data:
            continue

        delimiter = buffer.rindex(b"\n") + 1
        full_message = buffer[:delimiter]
        buffer = buffer[delimiter:]
        lines = full_message.decode().splitlines()

        for line in lines:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            message = AirbyteMessage.parse_raw(line)
            yield message


def _store_raw_airbyte_tables(
    repository: Repository,
    image_hash: str,
    staging_schema: str,
    sync_modes: Dict[str, str],
    default_sync_mode: str = "overwrite",
) -> List[str]:
    engine = repository.object_engine
    raw_tables = [t for t in engine.get_all_tables(staging_schema) if t.startswith(AIRBYTE_RAW)]
    current_image = repository.images[image_hash]
    for raw_table in raw_tables:
        sync_mode = sync_modes.get(raw_table)
        if not sync_mode:
            logging.warning(
                "Couldn't detect the sync mode for %s, falling back to %s",
                default_sync_mode,
            )
            sync_mode = default_sync_mode
        logging.info("Storing %s. Sync mode: %s", raw_table, sync_mode)

        # Make sure the raw table's schema didn't change (very rare, since it's
        # just hash, JSON, timestamp)
        new_schema = engine.get_full_table_schema(staging_schema, raw_table)
        if sync_mode != "overwrite":
            try:
                current_schema = current_image.get_table(raw_table).table_schema
                if current_schema != new_schema:
                    raise AssertionError(
                        "Schema for %s changed! Old: %s, new: %s",
                        raw_table,
                        current_schema,
                        new_schema,
                    )
            except TableNotFoundError:
                pass

        # If Airbyte meant to overwrite raw tables instead of append to them, we clear out the
        # current raw table so that record_table_as_base doesn't append objects to the existing
        # table.
        if sync_mode == "overwrite":
            repository.objects.overwrite_table(repository, image_hash, raw_table, new_schema, [])

        repository.objects.record_table_as_base(
            repository,
            raw_table,
            image_hash,
            chunk_size=int(get_singleton(CONFIG, "SG_COMMIT_CHUNK_SIZE")),
            source_schema=staging_schema,
            source_table=raw_table,
        )

    return raw_tables


def _store_processed_airbyte_tables(
    repository: Repository, image_hash: str, staging_schema: str
) -> None:
    engine = repository.object_engine
    # Save the processed tables in the image
    processed_tables = [
        t for t in engine.get_all_tables(staging_schema) if not t.startswith(AIRBYTE_RAW)
    ]
    for table in processed_tables:
        logging.info("Storing %s", table)
        schema_spec = engine.get_full_table_schema(staging_schema, table)
        repository.objects.overwrite_table(repository, image_hash, table, schema_spec, [])

        repository.objects.record_table_as_base(
            repository,
            table,
            image_hash,
            chunk_size=int(get_singleton(CONFIG, "SG_COMMIT_CHUNK_SIZE")),
            source_schema=staging_schema,
            source_table=table,
        )


def _column_type(schema_property) -> str:
    if "type" not in schema_property:
        # workaround for anyOf
        return "jsonb"
    return str(column_type(schema_property))


def get_sg_schema(stream: AirbyteStream) -> TableSchema:
    # NB Airbyte runs a normalization step after the ingestion that we can't easily predict,
    # since it involves unnesting some fields into separate tables and mapping column names.
    # This is given to the user for informational purposes.
    primary_key = [k for ks in stream.source_defined_primary_key or [] for k in ks]
    return [
        TableColumn(i, name, _column_type(schema_property), name in primary_key, None)
        for i, (name, schema_property) in enumerate(stream.json_schema["properties"].items())
    ]


def select_streams(
    catalog: AirbyteCatalog,
    tables: Optional[TableInfo],
    sync: bool = False,
    cursor_overrides: Optional[Dict[str, List[str]]] = None,
    primary_key_overrides: Optional[Dict[str, List[str]]] = None,
) -> ConfiguredAirbyteCatalog:
    streams: List[ConfiguredAirbyteStream] = []
    cursor_overrides = cursor_overrides or {}
    primary_key_overrides = primary_key_overrides or {}

    for stream in catalog.streams:
        if tables and stream.name not in tables:
            continue

        sync_configured = False

        if sync:
            if SyncMode.incremental not in stream.supported_sync_modes:
                logging.warning(
                    "Stream %s doesn't support incremental sync mode and sync=True. "
                    "Disabling append_dedup and falling back to refresh.",
                    stream.name,
                )
            else:
                # Some sources (like google search) issue duplicate fields which breaks mode=append,
                # so we have to use mode=append_dedup. However, that requires an explicit PK which
                # Airbyte currently doesn't extract from Singer-backed sources.
                # PR to fix: https://github.com/airbytehq/airbyte/pull/4789
                # In the meantime, we allow the plugin to override the cursor and the PK field.
                cursor_field = cursor_overrides.get(stream.name, stream.default_cursor_field)

                primary_key = stream.source_defined_primary_key
                if primary_key_overrides.get(stream.name):
                    primary_key = [[k] for k in primary_key_overrides[stream.name]]

                if not primary_key or not (cursor_field or stream.source_defined_cursor):
                    logging.warning(
                        "Stream %s doesn't have a primary key or a cursor field/source defined "
                        "cursor (PK: %s, cursor: %s). Disabling append_dedup and falling back "
                        "to refresh.",
                        stream.name,
                        primary_key,
                        cursor_field,
                    )
                else:
                    configured_stream = ConfiguredAirbyteStream(
                        stream=stream,
                        sync_mode=SyncMode.incremental,
                        destination_sync_mode=DestinationSyncMode.append_dedup,
                        # TODO dates aren't parsed properly (stay as strings)
                        cursor_field=stream.default_cursor_field,
                        primary_key=stream.source_defined_primary_key,
                    )
                    sync_configured = True

        # Fall back to configuring the stream for full refresh.
        if not sync_configured:
            configured_stream = ConfiguredAirbyteStream(
                stream=stream,
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.overwrite,
            )

        streams.append(configured_stream)

    return ConfiguredAirbyteCatalog(streams=streams)
