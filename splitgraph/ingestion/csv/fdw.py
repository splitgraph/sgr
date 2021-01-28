import codecs
import io
import logging
from copy import deepcopy
from itertools import islice
from typing import Tuple

import requests
from minio import Minio

import splitgraph.config
import csv

from splitgraph.commandline import get_exception_name
from splitgraph.commandline.common import ResettableStream
from splitgraph.ingestion.inference import infer_sg_schema

try:
    from multicorn import (
        ForeignDataWrapper,
        ANY,
        TableDefinition,
        ColumnDefinition,
    )
except ImportError:
    # Multicorn not installed (OK if we're not on the engine -- tests).
    ForeignDataWrapper = object
    ANY = object()
    TableDefinition = dict
    ColumnDefinition = dict

try:
    from multicorn.utils import log_to_postgres
except ImportError:
    log_to_postgres = print

_PG_LOGLEVEL = logging.INFO


class CSVForeignDataWrapper(ForeignDataWrapper):
    """Foreign data wrapper for CSV files stored in S3 buckets or HTTP"""

    def can_sort(self, sortkeys):
        # Currently, can't sort on anything. In the future, we can infer which
        # columns a CSV is sorted on and return something more useful here.
        return []

    def get_rel_size(self, quals, columns):
        return 1000000, len(columns) * 10

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        if self.mode == "http":
            return ["HTTP request", f"URL: {self.url}"]
        else:
            return [
                "S3 request",
                f"Endpoint: {self.s3_client._base_url}",
                f"Bucket: {self.s3_bucket}",
                f"Object ID: {self.s3_object}",
            ]

    def _read_csv(self, csv_reader, header=True):
        header_skipped = False
        for row in csv_reader:
            if not header_skipped and header:
                header_skipped = True
                continue
            # TODO munge into self.fdw_columns / columns if need be
            yield row

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""

        if self.mode == "http":
            with requests.get(self.url, stream=True) as r:
                return self._read_csv(
                    csv.reader(
                        (line.decode("utf-8") for line in r.iter_lines()), delimiter=self.delimiter
                    )
                )
        else:
            response = None
            try:
                response = self.s3_client.get_object(
                    bucket_name=self.s3_bucket, object_name=self.s3_object
                )
                stream = ResettableStream(response)

                # TODO copypasted from introspection, decide if we want to let people use this
                #  at query time (perf overhead?), factor out so that HTTP can use it too
                sniffer_sample = stream.read(2048).decode("utf-8")
                dialect = csv.Sniffer().sniff(sniffer_sample)
                has_header = csv.Sniffer().has_header(sniffer_sample)
                stream.reset()

                reader = csv.reader(codecs.iterdecode(stream, "utf-8"), dialect=dialect,)
                return self._read_csv(
                    reader, header=has_header
                )  # TODO incorporate override for header
            finally:
                if response:
                    response.close()
                    response.release_conn()

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        # Implement IMPORT FOREIGN SCHEMA to instead scan an S3 bucket for CSV files
        # and infer their CSV schema.

        # Merge server options and options passed to IMPORT FOREIGN SCHEMA
        fdw_options = deepcopy(srv_options)
        for k, v in options.items():
            fdw_options[k] = v

        # Get S3 options
        client, bucket, prefix = cls._get_s3_params(fdw_options)

        # Note that we ignore the "schema" here (e.g. IMPORT FOREIGN SCHEMA some_schema)
        # and take all interesting parameters through FDW options.
        objects = client.list_objects(bucket_name=bucket, prefix=prefix or None, recursive=True)

        result = []

        for o in objects:
            if restriction_type == "limit" and o.object_name not in restricts:
                continue

            if restriction_type == "except" and o.object_name in restricts:
                continue

            try:
                result.append(cls._scan_object(client, o, fdw_options))
            except Exception as e:
                log_to_postgres(
                    "Error scanning object %s, ignoring: %s: %s"
                    % (o.object_name, get_exception_name(e), e)
                )

        return result

    @classmethod
    def _scan_object(cls, client, minio_object, fdw_options):
        response = None
        try:
            response = client.get_object(minio_object.bucket_name, minio_object.object_name)
            stream = ResettableStream(response)

            sniffer_sample = stream.read(2048).decode("utf-8")
            dialect = csv.Sniffer().sniff(sniffer_sample)
            has_header = csv.Sniffer().has_header(sniffer_sample)
            stream.reset()

            reader = csv.reader(codecs.iterdecode(stream, "utf-8"), dialect=dialect,)
            sample = list(islice(reader, 100))

            if not has_header:  # (fdw_options.get("header", "true").lower() == "true"):
                # Patch in a dummy header
                sample = [[str(i) for i in range(len(sample))]] + sample

            sg_schema = infer_sg_schema(sample, None, None)

            # Build Multicorn TableDefinition. ColumnDefinition takes in type OIDs,
            # typmods and other internal PG stuff but other FDWs seem to get by with just
            # the textual type name.
            td = TableDefinition(
                table_name=minio_object.object_name,
                schema=None,
                columns=[
                    ColumnDefinition(column_name=c.name, type_name=c.pg_type) for c in sg_schema
                ],
                options={"s3_object": minio_object.object_name},
            )
            return td
        finally:
            if response:
                response.close()
                response.release_conn()

    @classmethod
    def _get_s3_params(cls, fdw_options) -> Tuple[Minio, str, str]:
        s3_client = Minio(
            endpoint=fdw_options["s3_endpoint"],
            access_key=fdw_options.get("s3_access_key"),
            secret_key=fdw_options.get("s3_secret_key"),
            secure=fdw_options.get("s3_secure", "true").lower() == "true",
        )

        s3_bucket = fdw_options["s3_bucket"]

        # We split the object into a prefix + object ID to let us mount a bunch of objects
        # with the same prefix as CSV files.
        s3_object_prefix = fdw_options.get("s3_object_prefix", "")

        return s3_client, s3_bucket, s3_object_prefix

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.
        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.

        """
        # Initialize the logger that will log to the engine's stderr: log timestamp and PID.

        logging.basicConfig(
            format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
            level=splitgraph.config.CONFIG["SG_LOGLEVEL"],
        )

        # Dict of connection parameters
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self.delimiter = fdw_options.get("delimiter", ",")
        self.quotechar = fdw_options.get("quotechar", '"')

        self.header = fdw_options.get("header", "true").lower() == "true"

        # TODO automatic inference
        #   * how to specify?
        #   * OK at query time?
        #

        self.autodetect_header = fdw_options.get("autodetect_header", "true").lower() == "true"
        self.autodetect_dialect = fdw_options.get("autodetect_dialect", "true").lower() == "true"

        # For HTTP: use full URL
        if fdw_options.get("url"):
            self.mode = "http"
            self.url = fdw_options["url"]
        else:
            self.mode = "s3"
            self.s3_client, self.s3_bucket, self.s3_object_prefix = self._get_s3_params(fdw_options)

            # TODO need a way to pass the table params (e.g. the actual S3 object name which
            #  might be different from table) into the preview and return it from introspection.
            self.s3_object = fdw_options["s3_object"]
