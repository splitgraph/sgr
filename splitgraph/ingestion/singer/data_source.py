import json
import logging
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from io import StringIO
from threading import Thread
from typing import Optional, cast

from splitgraph.core.repository import Repository
from splitgraph.core.types import IntrospectionResult, SyncState, TableInfo, TableParams
from splitgraph.exceptions import DataSourceError
from splitgraph.hooks.data_source.base import (
    SyncableDataSource,
    get_ingestion_state,
    prepare_new_image,
)
from splitgraph.ingestion.common import add_timestamp_tags
from splitgraph.ingestion.singer.common import (
    SingerCatalog,
    SingerConfig,
    SingerState,
    store_ingestion_state,
)
from splitgraph.ingestion.singer.db_sync import (
    get_key_properties,
    get_sg_schema,
    get_table_name,
    run_patched_sync,
    select_breadcrumb,
)


class SingerDataSource(SyncableDataSource, ABC):
    # Some taps (e.g. tap-github) use legacy --properties instead of --catalog
    use_properties = False

    # When True, the tap uses stream["schema"]["selected"] instead of
    #  stream["metadata"][breadcrumb == []]["selected"
    use_legacy_stream_selection = False

    @abstractmethod
    def get_singer_executable(self):
        raise NotImplementedError

    def get_singer_config(self):
        return {**self.params, **self.credentials}

    def _run_singer_discovery(self, config: Optional[SingerConfig] = None) -> SingerCatalog:
        executable = self.get_singer_executable()
        args = [executable]

        with tempfile.TemporaryDirectory() as d:
            self._add_file_arg(config, "config", d, args)

            catalog = subprocess.check_output(args + ["--discover"])

        return cast(SingerCatalog, json.loads(catalog))

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
        catalog: Optional[SingerCatalog] = None,
    ):
        executable = self.get_singer_executable()

        args = [executable]

        with tempfile.TemporaryDirectory() as d:
            self._add_file_arg(config, "config", d, args)
            self._add_file_arg(state, "state", d, args)
            self._add_file_arg(catalog, "properties" if self.use_properties else "catalog", d, args)
            logging.debug("Singer catalog: %s", catalog)
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

    def load(self, repository: "Repository", tables: Optional[TableInfo] = None) -> str:
        return self.sync(repository, image_hash=None, tables=tables, use_state=False)

    def sync(
        self,
        repository: Repository,
        image_hash: Optional[str] = None,
        tables: Optional[TableInfo] = None,
        use_state: bool = True,
    ) -> str:
        self._validate_table_params(tables)
        tables = tables or self.tables
        config = self.get_singer_config()
        catalog = self._run_singer_discovery(config)
        catalog = self.build_singer_catalog(catalog, tables)

        base_image, new_image_hash = prepare_new_image(repository, image_hash)
        state = get_ingestion_state(repository, image_hash) if use_state else None
        logging.info("Current ingestion state: %s", state)

        # Run the sink + target and capture the stdout (new state)
        output_stream = StringIO()

        failure: Optional[Exception] = None
        try:
            with self._run_singer(config, state, catalog=catalog) as proc:
                run_patched_sync(
                    repository,
                    base_image,
                    new_image_hash,
                    delete_old=True,
                    failure="keep_both",
                    input_stream=proc.stdout,
                    output_stream=output_stream,
                )
        except DataSourceError as e:
            logging.warning("Data source partially failed. Keeping the image anyway", exc_info=e)
            failure = e

        states = output_stream.getvalue()
        latest_state = states.splitlines()[-1]
        logging.info("State stream: %s", states)

        store_ingestion_state(repository, new_image_hash, state, latest_state)

        add_timestamp_tags(repository, new_image_hash)

        repository.commit_engines()

        if failure:
            raise failure

        return new_image_hash

    def build_singer_catalog(
        self, catalog: SingerCatalog, tables: Optional[TableInfo] = None
    ) -> SingerCatalog:
        return select_streams(
            catalog, tables=tables, use_legacy_stream_selection=self.use_legacy_stream_selection
        )

    def introspect(self) -> IntrospectionResult:
        config = self.get_singer_config()
        singer_schema = self._run_singer_discovery(config)

        result = IntrospectionResult({})
        for stream in singer_schema["streams"]:
            stream_name = get_table_name(stream)
            stream_schema = get_sg_schema(stream)
            result[stream_name] = (stream_schema, cast(TableParams, {}))
        return result


def select_streams(
    catalog: SingerCatalog, tables: Optional[TableInfo] = None, use_legacy_stream_selection=False
) -> SingerCatalog:
    tables = list(tables) if tables else None

    for stream in catalog["streams"]:
        stream_name = get_table_name(stream)
        if not tables or stream_name in tables:
            if use_legacy_stream_selection:
                # See https://github.com/singer-io/getting-started/blob/5182006a2bbe542d4e94e53ddc18b59c86fcd8a2/docs/SYNC_MODE.md#legacy-streamfield-selection
                stream["schema"]["selected"] = True
            else:
                stream["metadata"][0]["metadata"]["selected"] = True

    return catalog


class GenericSingerDataSource(SingerDataSource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tap_path = self.params.pop("tap_path")

    @classmethod
    def get_name(cls) -> str:
        return "Generic Singer tap"

    @classmethod
    def get_description(cls) -> str:
        return "Generic Singer tap"

    def get_singer_executable(self):
        return self.tap_path

    credentials_schema = {"type": "object"}
    params_schema = {
        "type": "object",
        "properties": {"tap_path": {"type": "string"}},
        "required": ["tap_path"],
    }


class MySQLSingerDataSource(SingerDataSource):
    params_schema = {
        "type": "object",
        "properties": {
            "replication_method": {
                "type": "string",
                "enum": ["INCREMENTAL", "LOG_BASED", "FULL TABLE"],
            },
            "host": {"type": "string"},
            "port": {"type": "integer"},
        },
        "required": ["host", "port", "replication_method"],
    }

    credentials_schema = {
        "type": "object",
        "properties": {"user": {"type": "string"}, "password": {"type": "string"}},
        "required": ["user", "password"],
    }

    use_properties = True
    use_legacy_stream_selection = False

    def get_singer_executable(self):
        return "tap-mysql"

    @classmethod
    def get_name(cls) -> str:
        return "Singer MySQL tap"

    @classmethod
    def get_description(cls) -> str:
        return "Singer MySQL tap"

    def build_singer_catalog(self, catalog: SingerCatalog, tables: Optional[TableInfo] = None):
        for stream in catalog["streams"]:
            stream_name = get_table_name(stream)

            # tap-mysql requires the table metadata to contain the replication type
            if not tables or stream_name in tables:
                select_breadcrumb(stream, [])["selected"] = True

                replication_method = self.params["replication_method"]
                replication_key: Optional[str] = None

                if replication_method == "INCREMENTAL":
                    key_properties = get_key_properties(stream)
                    if not key_properties:
                        logging.warning(
                            "Table %s has replication_method INCREMENTAL but no primary key. "
                            "Falling back to FULL TABLE",
                            stream_name,
                        )
                        replication_method = "FULL TABLE"
                        replication_key = None
                    elif len(key_properties) > 1:
                        logging.warning(
                            "Table %s has a composite primary key. Falling back to FULL TABLE",
                            stream_name,
                        )
                        replication_method = "FULL TABLE"
                        replication_key = None
                    else:
                        replication_key = key_properties[0]

                select_breadcrumb(stream, [])["replication-method"] = replication_method
                if replication_key:
                    select_breadcrumb(stream, [])["replication-key"] = replication_key
        return catalog
