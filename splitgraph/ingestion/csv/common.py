import csv
import io
from typing import TYPE_CHECKING, Any, Dict, NamedTuple, Tuple

from minio import Minio

if TYPE_CHECKING:
    import _csv

import chardet
from splitgraph.core.output import ResettableStream


class CSVOptions(NamedTuple):
    autodetect_header: bool = True
    autodetect_dialect: bool = True
    autodetect_encoding: bool = True
    autodetect_sample_size: int = 65536
    schema_inference_rows: int = 10000
    delimiter: str = ","
    quotechar: str = '"'
    header: bool = True
    encoding: str = "utf-8"
    ignore_decode_errors: bool = False

    @classmethod
    def from_fdw_options(cls, fdw_options):
        return cls(
            autodetect_header=get_bool(fdw_options, "autodetect_header"),
            autodetect_dialect=get_bool(fdw_options, "autodetect_dialect"),
            autodetect_encoding=get_bool(fdw_options, "autodetect_encoding"),
            autodetect_sample_size=int(fdw_options.get("autodetect_sample_size", 65536)),
            schema_inference_rows=int(fdw_options.get("schema_inference_rows", 10000)),
            header=get_bool(fdw_options, "header"),
            delimiter=fdw_options.get("delimiter", ","),
            quotechar=fdw_options.get("quotechar", '"'),
            encoding=fdw_options.get("encoding", "utf-8"),
            ignore_decode_errors=get_bool(fdw_options, "ignore_decode_errors", default=False),
        )

    def to_csv_kwargs(self):
        return {"delimiter": self.delimiter, "quotechar": self.quotechar}

    def to_table_options(self):
        """
        Turn this into a dict of table options that can be plugged back into CSVDataSource.
        """

        # The purpose is to return to the user the CSV dialect options that we inferred
        # so that they can freeze them in the table options (instead of rescanning the CSV
        # on every mount) + iterate on them.

        # We flip the autodetect flags to False here so that if we merge the new params with
        # the old params again, it won't rerun CSV dialect detection.
        return {
            "autodetect_header": "false",
            "autodetect_dialect": "false",
            "autodetect_encoding": "false",
            "header": bool_to_str(self.header),
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "encoding": self.encoding,
        }


def autodetect_csv(stream: io.RawIOBase, csv_options: CSVOptions) -> CSVOptions:
    """Autodetect the CSV dialect, encoding, header etc."""
    if not (
        csv_options.autodetect_encoding
        or csv_options.autodetect_header
        or csv_options.autodetect_dialect
    ):
        return csv_options

    data = stream.read(csv_options.autodetect_sample_size)
    assert data

    if csv_options.autodetect_encoding:
        encoding = chardet.detect(data)["encoding"]
        if encoding == "ascii" or encoding is None:
            # ASCII is a subset of UTF-8. For safety, if chardet detected
            # the encoding as ASCII, use UTF-8 (a valid ASCII file is a valid UTF-8 file,
            # but not vice versa)

            # If we can't detect the encoding, fall back to utf-8 too (hopefully the user
            # passed ignore_decode_errors=True
            encoding = "utf-8"
        csv_options = csv_options._replace(encoding=encoding)

    sample = data.decode(
        csv_options.encoding, errors="ignore" if csv_options.ignore_decode_errors else "strict"
    )
    # Emulate universal newlines mode (convert \r, \r\n, \n into \n)
    sample = "\n".join(sample.splitlines())

    if csv_options.autodetect_dialect:
        dialect = csv.Sniffer().sniff(sample)
        # These are meant to be set, but mypy claims they might not be.
        csv_options = csv_options._replace(
            delimiter=dialect.delimiter or ",", quotechar=dialect.quotechar or '"'
        )

    if csv_options.autodetect_header:
        has_header = csv.Sniffer().has_header(sample)
        csv_options = csv_options._replace(header=has_header)

    return csv_options


def get_bool(params: Dict[str, Any], key: str, default: bool = True) -> bool:
    if key not in params:
        return default
    if isinstance(params[key], bool):
        return bool(params[key])
    return bool(params[key].lower() == "true")


def bool_to_str(boolean: bool) -> str:
    return "true" if boolean else "false"


def make_csv_reader(
    response: io.IOBase, csv_options: CSVOptions
) -> Tuple[CSVOptions, "_csv._reader"]:
    stream = ResettableStream(response)
    csv_options = autodetect_csv(stream, csv_options)

    stream.reset()
    # https://docs.python.org/3/library/csv.html#id3
    # Open with newline="" for universal newlines
    io_stream = io.TextIOWrapper(
        io.BufferedReader(stream),
        encoding=csv_options.encoding,
        newline="",
        errors="ignore" if csv_options.ignore_decode_errors else "strict",
    )

    reader = csv.reader(io_stream, **csv_options.to_csv_kwargs())
    return csv_options, reader


def get_s3_params(fdw_options: Dict[str, Any]) -> Tuple[Minio, str, str]:
    s3_client = Minio(
        endpoint=fdw_options["s3_endpoint"],
        access_key=fdw_options.get("s3_access_key"),
        secret_key=fdw_options.get("s3_secret_key"),
        secure=get_bool(fdw_options, "s3_secure"),
        region=fdw_options.get("s3_region"),
    )

    s3_bucket = fdw_options["s3_bucket"]

    # We split the object into a prefix + object ID to let us mount a bunch of objects
    # with the same prefix as CSV files.
    s3_object_prefix = fdw_options.get("s3_object_prefix", "")

    return s3_client, s3_bucket, s3_object_prefix
