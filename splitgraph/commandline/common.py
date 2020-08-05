"""
Various common functions used by the command line interface.
"""
import json
from functools import wraps
from typing import Optional, Tuple, TYPE_CHECKING, Union, List, Any

import click
from click.core import Context, Parameter

from splitgraph.config import REMOTES
from splitgraph.exceptions import RepositoryNotFoundError

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.core.image import Image


class ImageType(click.ParamType):
    """Parser that extracts the full image specification (repository and hash/tag)."""

    name = "Image"

    def __init__(
        self,
        default: Optional[str] = "latest",
        repository_exists: bool = False,
        get_image: bool = False,
    ) -> None:
        """
        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default
        self.repository_exists = repository_exists
        self.get_image = get_image

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Tuple["Repository", Optional[Union["Image", str]]]:
        """
        Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

        The parser returns a tuple of (repository object, tag or hash).
        """
        from splitgraph.core.output import parse_repo_tag_or_hash

        repo, tag_or_hash = parse_repo_tag_or_hash(value, default=self.default)

        if self.get_image or self.repository_exists:
            # Check image/repo exists if we're asked (or if we need to produce
            # an actual Image object)
            from splitgraph.core.engine import repository_exists

            if not repository_exists(repo):
                raise RepositoryNotFoundError("Unknown repository %s" % repo)

        if tag_or_hash is not None and self.get_image:
            return repo, repo.images[tag_or_hash]
        else:
            return repo, tag_or_hash


class RepositoryType(click.ParamType):
    name = "Repository"

    def __init__(self, exists: bool = False) -> None:
        self.exists = exists

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> "Repository":
        from splitgraph.core.repository import Repository

        result = Repository.from_schema(value)
        if self.exists:
            from splitgraph.core.engine import repository_exists

            if not repository_exists(result):
                raise RepositoryNotFoundError("Unknown repository %s" % result)
        return result


def load_json_param(value: str, param: Optional[Parameter], ctx: Optional[Context]):
    if value.startswith("@"):
        fopt = click.File(mode="r")
        f = fopt.convert(value[1:], param, ctx)
        try:
            return json.load(f)
        finally:
            f.close()
    return json.loads(value)


class JsonType(click.ParamType):
    """Parser for Json -- a wrapper around json.loads because without specifying
    the name Click shows the type for the option/arg as LOADS.

    Also supports passing JSON files (pass in @filename.json).
    """

    name = "Json"

    def convert(self, value: str, param: Optional[Parameter], ctx: Optional[Context]):
        return load_json_param(value, param, ctx)


class Color:
    """
    An enumeration of console colors
    """

    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


def remote_switch_option(*names, **kwargs):
    """
    Adds an option to switch global SG_ENGINE for this invocation of sgr.

    This is useful for e.g. tagging or viewing image information on a remote
    registry. This is not used in operations like commit/checkout (even though
    nothing is preventing SG_ENGINE switch from working on that if the remote engine
    supports this), the user should switch SG_ENGINE envvar themselves in that case.

    :param names: Names
    :param kwargs: Passed to click.option
    """

    if not names:
        names = ["--remote", "-r"]

    kwargs.setdefault("default", None)
    kwargs.setdefault("expose_value", False)
    kwargs.setdefault("help", "Perform operation on a different remote engine")
    kwargs.setdefault("is_eager", True)
    kwargs.setdefault("type", click.Choice(REMOTES))

    def switch_engine_back(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            from splitgraph.engine import get_engine, set_engine

            engine = get_engine()
            try:
                f(*args, **kwargs)
                engine.commit()
            except Exception:
                engine.rollback()
                raise
            finally:
                engine.close()

                # In the context of a test run, we need to switch the global engine
                # back to LOCAL (since the engine-switching decorator doesn't
                # get control, so we can't do it there).
                set_engine(get_engine("LOCAL"))

        return wrapped

    def decorator(f):
        def _set_engine(ctx, param, value):
            if not value:
                return
            try:
                from splitgraph.engine import get_engine, set_engine

                engine = get_engine(value)

                set_engine(engine)
            except KeyError:
                raise click.BadParameter("Unknown remote %s!" % value)

        return click.option(*names, callback=_set_engine, **kwargs)(switch_engine_back(f))

    return decorator


def sql_results_to_str(results: List[Tuple[Any]], use_json: bool = False) -> str:
    if use_json:
        import json
        from splitgraph.core.common import coerce_val_to_json

        return json.dumps(coerce_val_to_json(results))

    from tabulate import tabulate

    return tabulate(results, tablefmt="plain")


def emit_sql_results(results, use_json=False, show_all=False):
    if results is None:
        return

    if len(results) > 10 and not show_all:
        click.echo(sql_results_to_str(results[:10], use_json))
        if not json:
            click.echo("...")
    else:
        click.echo(sql_results_to_str(results, use_json))
