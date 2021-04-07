import csv
import io
from typing import Optional, Dict, Tuple, NamedTuple, Union, Type, TYPE_CHECKING

if TYPE_CHECKING:
    import _csv

import chardet

from splitgraph.commandline.common import ResettableStream


class CSVOptions(NamedTuple):
    autodetect_header: bool = True
    autodetect_dialect: bool = True
    autodetect_encoding: bool = True
    autodetect_sample_size: int = 65536
    delimiter: str = ","
    quotechar: str = '"'
    dialect: Optional[Union[str, Type[csv.Dialect]]] = None
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
            header=get_bool(fdw_options, "header"),
            delimiter=fdw_options.get("delimiter", ","),
            quotechar=fdw_options.get("quotechar", '"'),
            dialect=fdw_options.get("dialect"),
            encoding=fdw_options.get("encoding", "utf-8"),
            ignore_decode_errors=get_bool(fdw_options, "ignore_decode_errors", default=False),
        )

    def to_csv_kwargs(self):
        if self.dialect:
            return {"dialect": self.dialect}
        return {"delimiter": self.delimiter, "quotechar": self.quotechar}


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
        csv_options = csv_options._replace(dialect=dialect)

    if csv_options.autodetect_header:
        has_header = csv.Sniffer().has_header(sample)
        csv_options = csv_options._replace(header=has_header)

    return csv_options


def get_bool(params: Dict[str, str], key: str, default: bool = True) -> bool:
    if key not in params:
        return default
    return params[key].lower() == "true"


def make_csv_reader(
    response: io.IOBase, csv_options: CSVOptions
) -> Tuple[CSVOptions, "_csv._reader"]:
    stream = ResettableStream(response)
    csv_options = autodetect_csv(stream, csv_options)

    stream.reset()
    # https://docs.python.org/3/library/csv.html#id3
    # Open with newline="" for universal newlines
    io_stream = io.TextIOWrapper(io.BufferedReader(stream), encoding=csv_options.encoding, newline="", errors="ignore" if csv_options.ignore_decode_errors else "strict")  # type: ignore

    reader = csv.reader(io_stream, **csv_options.to_csv_kwargs())
    return csv_options, reader
