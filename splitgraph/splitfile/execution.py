"""
Functions for executing Splitfiles.
"""

import json
import logging
from hashlib import sha256
from importlib import import_module
from random import getrandbits
from typing import Callable, Dict, List, Optional, cast, Tuple

from parsimonious.nodes import Node
from psycopg2.sql import SQL, Identifier

from splitgraph.config import CONFIG
from splitgraph.config.config import get_all_in_section, get_singleton
from splitgraph.core.engine import repository_exists, lookup_repository
from splitgraph.core.image import Image
from splitgraph.core.repository import Repository, clone
from splitgraph.core.sql import prepare_splitfile_sql, validate_import_sql
from splitgraph.engine import get_engine
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import ImageNotFoundError, SplitfileError
from splitgraph.hooks.mount_handlers import mount
from ._parsing import (
    parse_commands,
    extract_nodes,
    get_first_or_none,
    parse_image_spec,
    extract_all_table_aliases,
    parse_custom_command,
)
from ..core.output import pluralise, truncate_line, conn_string_to_dict
from ..core.types import ProvenanceLine


def _combine_hashes(hashes: List[str]) -> str:
    return sha256("".join(hashes).encode("ascii")).hexdigest()


def _checkout_or_calculate_layer(output: Repository, image_hash: str, calc_func: Callable) -> None:
    # Future optimization here: don't actually check the layer out if it exists -- only do it at Splitfile execution
    # end or when a command needs it.

    # Have we already calculated this hash?
    try:
        output.images.by_hash(image_hash).checkout()
        logging.info(" ---> Using cache")
    except ImageNotFoundError:
        try:
            calc_func()
        except Exception:
            # Some of the Splitfile commands have to use a commit
            # (e.g. IMPORT uses Image.query_schema context manager that mounts the source
            # image via LQ and commits), so a rollback won't delete this new image
            # that we might have created.
            output.images.delete([image_hash])
            raise
    logging.info(" ---> %s" % image_hash[:12])


def _get_local_image_for_import(hash_or_tag: str, repository: Repository) -> Tuple[Image, bool]:
    """
    Converts a remote repository and tag into an Image object that exists on the engine,
    optionally pulling the repository or cloning it into a temporary location.

    :param hash_or_tag: Hash/tag
    :param repository: Name of the repository (doesn't need to be local)
    :return: Image object and a boolean flag showing whether the repository should be deleted
    when the image is no longer needed.
    """
    tmp_repo = Repository(repository.namespace, repository.repository + "_tmp_clone")
    repo_is_temporary = False

    logging.info("Resolving repository %s", repository)
    source_repo = lookup_repository(repository.to_schema(), include_local=True)
    if source_repo.engine.name != "LOCAL":
        clone(source_repo, local_repository=tmp_repo, download_all=False)
        source_image = tmp_repo.images[hash_or_tag]
        repo_is_temporary = True
    else:
        # For local repositories, first try to pull them to see if they are clones of a remote.
        if source_repo.upstream:
            source_repo.pull()
        source_image = source_repo.images[hash_or_tag]

    return source_image, repo_is_temporary


class ImageMapper:
    def __init__(self, object_engine: "PostgresEngine"):
        self.object_engine = object_engine

        self.image_map: Dict[Tuple[Repository, str], Tuple[str, str, Image]] = {}

        self._temporary_repositories: List[Repository] = []

    def _calculate_map(self, repository: Repository, hash_or_tag: str) -> Tuple[str, str, Image]:
        source_image, repo_is_temporary = _get_local_image_for_import(hash_or_tag, repository)
        if repo_is_temporary:
            self._temporary_repositories.append(source_image.repository)

        canonical_form = "%s/%s:%s" % (
            repository.namespace,
            repository.repository,
            source_image.image_hash,
        )

        temporary_schema = sha256(canonical_form.encode("utf-8")).hexdigest()[:63]

        return temporary_schema, canonical_form, source_image

    def __call__(self, repository: Repository, hash_or_tag: str) -> Tuple[str, str]:
        key = (repository, hash_or_tag)
        if key not in self.image_map:
            self.image_map[key] = self._calculate_map(key[0], key[1])

        temporary_schema, canonical_form, _ = self.image_map[key]
        return temporary_schema, canonical_form

    def setup_lq_mounts(self) -> None:
        for temporary_schema, _, source_image in self.image_map.values():
            self.object_engine.delete_schema(temporary_schema)
            self.object_engine.create_schema(temporary_schema)
            source_image._lq_checkout(target_schema=temporary_schema)

    def teardown_lq_mounts(self) -> None:
        for temporary_schema, _, _ in self.image_map.values():
            self.object_engine.run_sql(
                SQL("DROP SERVER IF EXISTS {} CASCADE").format(
                    Identifier("%s_lq_checkout_server" % temporary_schema)
                )
            )
            self.object_engine.run_sql(
                SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(temporary_schema))
            )
        # Delete temporary repositories that were cloned just for the SQL execution.
        for repo in self._temporary_repositories:
            repo.delete()

    def get_provenance_data(self) -> ProvenanceLine:
        return {
            "sources": [
                {
                    "source_namespace": source_repo.namespace,
                    "source": source_repo.repository,
                    "source_hash": source_image.image_hash,
                }
                for (source_repo, _), (_, _, source_image) in self.image_map.items()
            ]
        }


def execute_commands(
    commands: str,
    params: Optional[Dict[str, str]] = None,
    output: Optional[Repository] = None,
    output_base: str = "0" * 32,
) -> None:
    """
    Executes a series of Splitfile commands.

    :param commands: A string with the raw Splitfile.
    :param params: A dictionary of parameters to be applied to the Splitfile (`${PARAM}` is replaced with the specified
        parameter value).
    :param output: Output repository to execute the Splitfile against.
    :param output_base: If not None, a revision that gets checked out for all Splitfile actions to be committed
        on top of it.
    """
    if params is None:
        params = {}
    if output and repository_exists(output) and output_base is not None:
        output.images.by_hash(output_base).checkout()
    # Use a random target schema if unspecified.
    output = output or Repository.from_schema("output_%0.2x" % getrandbits(16))

    # Don't initialize the output until a command writing to it asks us to
    # (otherwise we might have a FROM ... AS output_name change it).
    repo_created = False

    def _initialize_output(output):
        if not repository_exists(output):
            nonlocal repo_created
            output.init()
            repo_created = True

    from splitgraph.commandline.common import Color

    node_list = parse_commands(commands, params=params)

    # Record the internal structure of commands used to create the final image.
    provenance: List[ProvenanceLine] = []

    try:
        for i, node in enumerate(node_list):
            logging.info(
                Color.BOLD
                + "\nStep %d/%d : %s" % (i + 1, len(node_list), truncate_line(node.text, length=60))
                + Color.END
            )
            if node.expr_name == "from":
                output, maybe_provenance_line = _execute_from(node, output)
                if maybe_provenance_line:
                    provenance.append(maybe_provenance_line)

            elif node.expr_name == "import":
                _initialize_output(output)
                provenance_line = _execute_import(node, output)
                provenance.append(provenance_line)

            elif node.expr_name == "sql" or node.expr_name == "sql_file":
                _initialize_output(output)
                provenance_line = _execute_sql(node, output)
                provenance.append(provenance_line)

            elif node.expr_name == "custom":
                _initialize_output(output)
                provenance_line = _execute_custom(node, output)
                provenance.append(provenance_line)

        final_image = output.head_strict
        final_image.set_provenance(provenance)
        get_engine().commit()
        logging.info("Successfully built %s:%s." % (str(output), final_image.image_hash[:12]))

    except Exception:
        if repo_created and len(output.images()) == 1:
            # As a corner case, if we created a repository and there's been
            # a failure running the Splitfile (on the first command), we delete the dummy
            # 0000... image and the rest of the repository as part of cleanup.
            output.delete()
        get_engine().rollback()
        raise


def _execute_sql(node: Node, output: Repository) -> ProvenanceLine:
    # Calculate the hash of the layer we are trying to create.
    # Since we handle the "input" hashing in the import step, we don't need to care about the sources here.
    # Later on, we could enhance the caching and base the hash of the command on the hashes of objects that
    # definitely go there as sources.
    if node.expr_name == "sql_file":
        node_contents = extract_nodes(node, ["non_newline"])[0].text
        logging.info("Loading the SQL commands from %s" % node_contents)
        with open(node_contents, "r") as file:
            # Possibly use a different method to calculate the image hash for commands originating from
            # SQL files instead?
            # Don't "canonicalize" it here to get rid of whitespace, just hash the whole file.
            sql_command = file.read()
    else:
        # Support singleline and multiple SQL commands between braces
        nodes = extract_nodes(node, ["non_newline"])
        if not nodes:
            nodes = extract_nodes(node, ["non_curly_brace"])
        sql_command = nodes[0].text.replace("\\{", "{").replace("\\}", "}").replace("\\\\", "\\")

    # Prepare the SQL by rewriting outside image references into LQ schemas
    # Canonicalize the SQL command by formatting it.
    image_mapper = ImageMapper(output.object_engine)
    sql_rewritten, sql_canonical = prepare_splitfile_sql(sql_command, image_mapper)

    output_head = output.head_strict.image_hash
    target_hash = _combine_hashes([output_head, sha256(sql_canonical.encode("utf-8")).hexdigest()])

    def _calc():
        logging.info("Executing SQL...")
        try:
            image_mapper.setup_lq_mounts()
            output.object_engine.commit()
            sql_to_run = get_singleton(CONFIG, "SG_LQ_TUNING") + sql_rewritten
            logging.debug("Running rewritten SQL %s against %s", sql_to_run, output)
            output.run_sql(sql_to_run)
        finally:
            image_mapper.teardown_lq_mounts()
        output.commit(target_hash, comment=sql_command)

    _checkout_or_calculate_layer(output, target_hash, _calc)
    provenance: ProvenanceLine = {"type": "SQL", "sql": sql_canonical}
    provenance.update(image_mapper.get_provenance_data())
    return provenance


def _execute_from(node: Node, output: Repository) -> Tuple[Repository, Optional[ProvenanceLine]]:
    interesting_nodes = extract_nodes(node, ["repo_source", "repository"])
    repo_source = get_first_or_none(interesting_nodes, "repo_source")
    output_node = get_first_or_none(interesting_nodes, "repository")
    provenance: Optional[ProvenanceLine] = None

    if output_node:
        # AS (output) detected, change the current output repository to it.
        output = Repository.from_schema(output_node.match.group(0))
        logging.info("Changed output repository to %s" % str(output))

        # NB this destroys all data in the case where we ran some commands in the Splitfile and then
        # did FROM (...) without AS repository
        if repository_exists(output):
            logging.info("Clearing all output from %s" % str(output))
            output.delete()
    if not repository_exists(output):
        output.init()
    if repo_source:
        repository, tag_or_hash = parse_image_spec(repo_source)
        source_repo = lookup_repository(repository.to_schema(), include_local=True)

        if source_repo.engine.name == "LOCAL":
            # For local repositories, make sure to update them if they've an upstream
            if source_repo.upstream:
                source_repo.pull()

        # Get the target image hash from the source repo: otherwise, if the tag is, say, 'latest' and
        # the output has just had the base commit (000...) created in it, that commit will be the latest.
        clone(source_repo, local_repository=output, download_all=False)
        source_hash = source_repo.images[tag_or_hash].image_hash
        output.images.by_hash(source_hash).checkout()
        provenance = {
            "type": "FROM",
            "source_namespace": source_repo.namespace,
            "source": source_repo.repository,
            "source_hash": source_hash,
        }
    else:
        # FROM EMPTY AS repository -- initializes an empty repository (say to create a table or import
        # the results of a previous stage in a multistage build.
        # In this case, if AS repository has been specified, it's already been initialized. If not, this command
        # literally does nothing
        if not output_node:
            raise SplitfileError("FROM EMPTY without AS (repository) does nothing!")
    return output, provenance


def _execute_import(node: Node, output: Repository) -> ProvenanceLine:
    interesting_nodes = extract_nodes(node, ["repo_source", "mount_source", "tables"])
    table_names, table_aliases, table_queries = extract_all_table_aliases(interesting_nodes[-1])
    if interesting_nodes[0].expr_name == "repo_source":
        # Import from a repository (local or remote)
        repository, tag_or_hash = parse_image_spec(interesting_nodes[0])
        return _execute_repo_import(
            repository, table_names, tag_or_hash, output, table_aliases, table_queries
        )
    else:
        # Extract the identifier (FDW name), the connection string and the FDW params (JSON-encoded, everything
        # between the single quotes).
        mount_nodes = extract_nodes(
            interesting_nodes[0], ["identifier", "no_db_conn_string", "non_single_quote"]
        )
        fdw_name = mount_nodes[0].match.group(0)
        conn_string = mount_nodes[1].match
        fdw_params = mount_nodes[2].match.group(0).replace("\\'", "'")  # Unescape the single quote

        return _execute_db_import(
            conn_string, fdw_name, fdw_params, table_names, output, table_aliases, table_queries
        )


def _execute_db_import(
    conn_string, fdw_name, fdw_params, table_names, target_mountpoint, table_aliases, table_queries
) -> ProvenanceLine:
    tmp_mountpoint = Repository.from_schema(fdw_name + "_tmp_staging")
    try:
        handler_kwargs = json.loads(fdw_params)
        handler_kwargs.update(conn_string_to_dict(conn_string.group() if conn_string else None))
        mount(tmp_mountpoint.to_schema(), fdw_name, handler_kwargs)
        # The foreign database is a moving target, so the new image hash is random.
        # Maybe in the future, when the object hash is a function of its contents, we can be smarter here...
        target_hash = "{:064x}".format(getrandbits(256))
        target_mountpoint.import_tables(
            table_aliases,
            tmp_mountpoint,
            table_names,
            target_hash=target_hash,
            foreign_tables=True,
            table_queries=table_queries,
        )
        return {"type": "MOUNT"}
    finally:
        tmp_mountpoint.delete()


def prevalidate_imports(table_names: List[str], table_queries: List[bool]) -> List[str]:
    return [validate_import_sql(t) if q else t for t, q in zip(table_names, table_queries)]


def _execute_repo_import(
    repository: Repository,
    table_names: List[str],
    tag_or_hash: str,
    target_repository: Repository,
    table_aliases: List[str],
    table_queries: List[bool],
) -> ProvenanceLine:
    # Don't use the actual routine here as we want more control: clone the remote repo in order to turn
    # the tag into an actual hash
    local_image, repo_is_temporary = _get_local_image_for_import(tag_or_hash, repository)
    source_hash = local_image.image_hash
    source_mountpoint = local_image.repository
    try:

        # Perform validation here rather than in the import routine. This is to
        # canonicalize SQL that goes into provenance data so that we don't get cache misses
        # from formatting changes to SQL statements.
        table_names_canonical = prevalidate_imports(table_names, table_queries)

        # Calculate the hash of the new layer by combining the hash of the previous layer,
        # the hash of the source and all the table names/aliases getting imported.
        # This can be made more granular later by using, say, the object IDs of the tables
        # that are getting imported (so that if there's a new commit with some of the same objects,
        # we don't invalidate the downstream).
        # If table_names actually contains queries that generate data from tables, we can still use
        # it for hashing: we assume that the queries are deterministic, so if the query is changed,
        # the whole layer is invalidated.
        output_head = target_repository.head_strict.image_hash
        target_hash = _combine_hashes(
            [output_head, source_hash]
            + [sha256(n.encode("utf-8")).hexdigest() for n in table_names_canonical + table_aliases]
        )

        def _calc():
            logging.info(
                "Importing %s from %s:%s into %s",
                pluralise("table", len(table_names)),
                str(repository),
                source_hash[:12],
                str(target_repository),
            )
            target_repository.import_tables(
                table_aliases,
                source_mountpoint,
                table_names,
                image_hash=source_hash,
                target_hash=target_hash,
                table_queries=table_queries,
                skip_validation=True,
            )

        _checkout_or_calculate_layer(target_repository, target_hash, _calc)
        return {
            "type": "IMPORT",
            "source_namespace": repository.namespace,
            "source": repository.repository,
            "source_hash": source_hash,
            "tables": table_names_canonical,
            "table_aliases": table_aliases,
            "table_queries": table_queries,
        }
    finally:
        if repo_is_temporary:
            source_mountpoint.delete()


def _execute_custom(node: Node, output: Repository) -> ProvenanceLine:
    assert output.head is not None
    command, args = parse_custom_command(node)

    # Locate the command in the config file and instantiate it.
    cmd_fq_class: str = cast(str, get_all_in_section(CONFIG, "commands").get(command))
    if not cmd_fq_class:
        raise SplitfileError(
            "Custom command {0} not found in the config! Make sure you add an entry to your"
            " config like so:\n  [commands]  \n{0}=path.to.command.Class".format(command)
        )

    assert isinstance(cmd_fq_class, str)
    index = cmd_fq_class.rindex(".")
    try:
        cmd_class = getattr(import_module(cmd_fq_class[:index]), cmd_fq_class[index + 1 :])
    except AttributeError as e:
        raise SplitfileError("Error loading custom command {0}".format(command)) from e
    except ImportError as e:
        raise SplitfileError("Error loading custom command {0}".format(command)) from e

    get_engine().run_sql("SET search_path TO %s", (output.to_schema(),))
    command = cmd_class()

    # Pre-flight check: get the new command hash and see if we can short-circuit and just check the image out.
    command_hash = command.calc_hash(repository=output, args=args)
    output_head = output.head.image_hash

    if command_hash is not None:
        image_hash = _combine_hashes([output_head, command_hash])
        try:
            output.images.by_hash(image_hash).checkout()
            logging.info(" ---> Using cache")
            return {"type": "CUSTOM"}
        except ImageNotFoundError:
            pass

    logging.info(" Executing custom command...")
    exec_hash = command.execute(repository=output, args=args)
    command_hash = command_hash or exec_hash or "{:064x}".format(getrandbits(256))

    image_hash = _combine_hashes([output_head, command_hash])
    logging.info(" ---> %s" % image_hash[:12])

    # Check just in case if the new hash produced by the command already exists.
    try:
        output.images.by_hash(image_hash).checkout()
    except ImageNotFoundError:
        # Full command as a commit comment
        output.commit(image_hash, comment=node.text)
    return {"type": "CUSTOM"}


def rebuild_image(image: Image, source_replacement: Dict[Repository, str]) -> None:
    """
    Recreates the Splitfile used to create a given image and reruns it, replacing its dependencies with a different
    set of versions.

    :param image: Image object
    :param source_replacement: A map that specifies replacement images/tags for repositories that the image depends on
    """
    splitfile_commands = image.to_splitfile(
        ignore_irreproducible=False, source_replacement=source_replacement
    )
    # Params are supposed to be stored in the commands already (baked in) -- what if there's sensitive data there?
    execute_commands("\n".join(splitfile_commands), output=image.repository)
