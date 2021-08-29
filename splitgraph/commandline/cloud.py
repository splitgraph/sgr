"""Command line routines related to registering/setting up connections to the Splitgraph registry."""
import hashlib
import logging
import os
import shutil
import string
import subprocess
from copy import copy
from glob import glob
from typing import Dict, Optional, Tuple, cast
from urllib.parse import quote, urlparse

import click
from click import wrap_text
from splitgraph.cloud.models import (
    AddExternalRepositoryRequest,
    Metadata,
    RepositoriesYAML,
)
from splitgraph.commandline.common import ImageType, RepositoryType, emit_sql_results
from splitgraph.commandline.engine import inject_config_into_engines

# Hardcoded database name for the Splitgraph DDN (ddn instead of sgregistry)
from splitgraph.config.config import get_from_subsection
from splitgraph.config.management import patch_and_save_config
from splitgraph.core.output import Color, pluralise

_DDN_DBNAME = "ddn"

VALID_FILENAME_CHARACTERS = string.ascii_letters + string.digits + "-_."


@click.command("register")
@click.option("--username", prompt=True)
@click.password_option()
@click.option("--email", prompt=True)
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote registry to register on.",
)
@click.option("--accept-tos", is_flag=True, help="Accept the registry's Terms of Service")
@click.option(
    "-s", "--skip-inject", is_flag=True, help="Don't try to copy the config into all engines"
)
def register_c(username, password, email, remote, accept_tos, skip_inject):
    """
    Register the user on a Splitgraph registry.

    By default, this registers the user on data.splitgraph.com,
    obtains a set of machine (API) credentials for the client to communicate
    with the registry and configures the data.splitgraph.com engine.
    """
    from splitgraph.cloud import DEFAULT_REMOTES, RESTAPIClient
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    client = RESTAPIClient(remote)
    tos = client.tos()

    if tos and not accept_tos:
        click.echo("%s says: %s" % (client.endpoint, tos))
        click.confirm("Do you accept the Terms of Service?", default=False, abort=True)
    click.echo("Registering the user...")

    uuid, access, refresh = cast(
        Tuple[str, str, str], client.register(username, password, email, accept_tos=True)
    )
    click.echo("Registration successful. UUID %s" % uuid)

    key, secret = cast(Tuple[str, str], client.create_machine_credentials(access, password))
    click.echo("Acquired refresh token and API keys")

    repo_lookup = _update_repo_lookup(CONFIG, remote)

    # If remote isn't in the config (is one of the default ones), add the default parameters
    # to the new config -- otherwise only overwrite the new fields in the existing config.
    remote_params = (
        copy(DEFAULT_REMOTES.get(remote, {}))
        if not get_all_in_subsection(CONFIG, "remotes", remote)
        else {}
    )
    remote_params.update(
        {
            "SG_ENGINE_USER": key,
            "SG_ENGINE_PWD": secret,
            "SG_NAMESPACE": username,
            "SG_CLOUD_REFRESH_TOKEN": refresh,
            "SG_CLOUD_ACCESS_TOKEN": access,
            "SG_IS_REGISTRY": "true",
        }
    )

    config_patch = {
        "SG_REPO_LOOKUP": repo_lookup,
        "remotes": {remote: remote_params},
    }
    config_path = patch_and_save_config(CONFIG, config_patch)

    if not skip_inject:
        inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)

    click.echo("Done.")


def _update_repo_lookup(config, remote):
    repo_lookup = config.get("SG_REPO_LOOKUP")
    if repo_lookup:
        repo_lookup = repo_lookup.split(",")
        if remote not in repo_lookup:
            repo_lookup.append(remote)
    else:
        repo_lookup = [remote]
    return ",".join(repo_lookup)


def _construct_user_profile_url(auth_endpoint: str):
    parsed = urlparse(auth_endpoint)
    netloc_components = parsed.netloc.split(".")
    netloc_components[0] = "www"
    return "%s://%s/settings/security" % (parsed.scheme, ".".join(netloc_components))


def _construct_repo_url(gql_endpoint: str, full_repo: str):
    parsed = urlparse(gql_endpoint)
    netloc_components = parsed.netloc.split(".")
    netloc_components[0] = "www"
    return "%s://%s/%s" % (parsed.scheme, ".".join(netloc_components), full_repo)


def _construct_search_url(gql_endpoint: str, query: str):
    parsed = urlparse(gql_endpoint)
    netloc_components = parsed.netloc.split(".")
    netloc_components[0] = "www"
    return "%s://%s/search?q=%s" % (parsed.scheme, ".".join(netloc_components), quote(query))


@click.command("login")
@click.option("--username", prompt="Username or e-mail")
@click.option("--password", default=None)
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote registry to log into.",
)
@click.option(
    "--overwrite", is_flag=True, help="Overwrite old API keys in the config if they exist"
)
@click.option(
    "-s", "--skip-inject", is_flag=True, help="Don't try to copy the config into all engines"
)
def login_c(username, password, remote, overwrite, skip_inject):
    """Log into a Splitgraph registry with username/password.

    This will generate a new refresh token (to use the Splitgraph query API)
    and API keys to let sgr access the registry (if they don't already exist
    in the configuration file or if the actual username has changed).

    Note that if you already have generated an API key pair but it's not
    in the configuration file, this will generate a new pair instead of
    restoring the existing one, as the API secret is only stored in the configuration file.

    If you want to log in using an existing API key pair, use `sgr cloud login-api` instead.
    """
    from splitgraph.cloud import DEFAULT_REMOTES, RESTAPIClient, get_token_claim
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    client = RESTAPIClient(remote)

    if not password:
        profile_url = _construct_user_profile_url(client.endpoint)
        password = click.prompt(
            text="Password (visit %s if you don't have it)" % profile_url,
            confirmation_prompt=False,
            hide_input=True,
        )

    access, refresh = cast(Tuple[str, str], client.get_refresh_token(username, password))

    # Extract namespace from the access token since we might have logged in with an e-mail.
    namespace = get_token_claim(access, "username")

    click.echo("Logged into %s as %s" % (remote, namespace))

    config_remote_params = get_all_in_subsection(CONFIG, "remotes", remote)

    remote_params = copy(DEFAULT_REMOTES.get(remote, {})) if not config_remote_params else {}
    remote_params.update(
        {
            "SG_NAMESPACE": namespace,
            "SG_CLOUD_REFRESH_TOKEN": refresh,
            "SG_CLOUD_ACCESS_TOKEN": access,
        }
    )

    config_patch = {
        "SG_REPO_LOOKUP": _update_repo_lookup(CONFIG, remote),
        "remotes": {remote: remote_params},
    }

    # Get new tokens in any case if we're logging in under a different username.
    try:
        username_changed = namespace != get_from_subsection(
            CONFIG, "remotes", remote, "SG_NAMESPACE"
        )
    except KeyError:
        username_changed = False

    if (
        "SG_ENGINE_USER" not in config_remote_params
        or "SG_ENGINE_PWD" not in config_remote_params
        or overwrite
        or username_changed
    ):
        key, secret = cast(Tuple[str, str], client.create_machine_credentials(access, password))
        config_patch["remotes"][remote]["SG_ENGINE_USER"] = key
        config_patch["remotes"][remote]["SG_ENGINE_PWD"] = secret
        click.echo("Acquired new API keys")

    config_path = patch_and_save_config(CONFIG, config_patch)

    if not skip_inject:
        inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)


@click.command("login-api")
@click.option("--api-key", prompt="API key")
@click.option("--api-secret", prompt="API secret", confirmation_prompt=False, hide_input=True)
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote registry to log into.",
)
@click.option(
    "-s", "--skip-inject", is_flag=True, help="Don't try to copy the config into all engines"
)
def login_api_c(api_key, api_secret, remote, skip_inject):
    """Log into a Splitgraph registry using existing API keys.

    This will inject the API keys for the registry into the configuration file
    and generate a new access token.
    """
    from splitgraph.cloud import DEFAULT_REMOTES, RESTAPIClient, get_token_claim
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    client = RESTAPIClient(remote)
    access = cast(str, client.get_access_token_from_api(api_key, api_secret))

    namespace = get_token_claim(access, "username")

    click.echo("Logged into %s as %s" % (remote, namespace))
    remote_params = (
        copy(DEFAULT_REMOTES.get(remote, {}))
        if not get_all_in_subsection(CONFIG, "remotes", remote)
        else {}
    )
    remote_params.update(
        {
            "SG_NAMESPACE": namespace,
            "SG_CLOUD_ACCESS_TOKEN": access,
            "SG_ENGINE_USER": api_key,
            "SG_ENGINE_PWD": api_secret,
        }
    )
    config_patch = {
        "SG_REPO_LOOKUP": _update_repo_lookup(CONFIG, remote),
        "remotes": {remote: remote_params},
    }
    config_path = patch_and_save_config(CONFIG, config_patch)

    if not skip_inject:
        inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)


@click.command("curl", context_settings=dict(ignore_unknown_options=True))
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.option(
    "-t", "--request-type", default="postgrest", type=click.Choice(["postgrest", "splitfile"])
)
@click.argument("image", type=ImageType(default="latest"))
@click.argument("request_params", type=str, default="")
# This is kind of awkward: we want to support not passing in any request params to default
# them to / but also want to be able to support multiple curl args which click doesn't let
# us do (click.argument with nargs=-1 makes "request_params" consume the first arg even
# if we use the posix standard separator --).
@click.option(
    "-c", "--curl-args", type=str, multiple=True, help="Extra arguments to be passed to curl"
)
def curl_c(remote, request_type, image, request_params, curl_args):
    """
    Query a Splitgraph REST API.

    This is a thin wrapper around curl that performs an HTTP request to Splitgraph Cloud to
    interact with a dataset using PostgREST (http://postgrest.org) or the Splitfile execution service.

    The actual invocation is:

    ```
    curl [API endpoint][request] -H [access_token] [extra curl args].
    ```

    The image must be of the form `namespace/repository:[hash_or_tag (default latest)]`.

    The actual request parameters depend on the request type:

      * For PostgREST: `/table?[postgrest request]` or empty to get the OpenAPI spec for this image.
        For a reference on how to perform Postgrest requests, see http://postgrest.org/en/latest/api.html.
      * For the Splitfile executor: a JSON array to be POSTed to the executor, e.g.
        `'{"command": "FROM some/repo IMPORT some_table AS alias", "tag": "new_tag"}'`.

    `--curl-args` allows to pass extra arguments to curl. Note that every argument must be prefixed
    with `--curl-args`, e.g. `--curl-args --cacert --curl-args /path/to/ca.pem`.
    """
    from splitgraph.cloud import RESTAPIClient, get_headers
    from splitgraph.config import CONFIG

    repository, hash_or_tag = image

    # Craft a request
    config = cast(Dict[str, str], CONFIG["remotes"][remote])
    access_token = RESTAPIClient(remote).access_token
    headers = get_headers()
    headers.update({"Authorization": "Bearer " + access_token})

    if request_type == "postgrest":
        if request_params and not request_params.startswith("/"):
            request_params = "/" + request_params
        full_request = (
            config["SG_QUERY_API"]
            + "/%s/%s" % (str(repository), str(hash_or_tag))
            + "/-/rest"
            + request_params
        )
    else:
        full_request = (
            config["SG_QUERY_API"] + "/%s/%s" % (str(repository), str(hash_or_tag)) + "/-/splitfile"
        )
        curl_args = ["-X", "POST", "-d", request_params] + list(curl_args)
        headers.update({"Content-Type": "application/json"})

    header_invocation = [h for i in headers.items() for h in ("-H", "%s: %s" % i)]
    subprocess_args = ["curl", full_request] + header_invocation + list(curl_args)

    logging.debug("Calling %s", " ".join(subprocess_args))
    subprocess.call(subprocess_args)


def _get_ddn_conn_params(remote: str) -> Dict[str, Optional[str]]:
    from splitgraph.engine import get_engine

    try:
        engine = get_engine(remote)
    except KeyError as e:
        raise click.UsageError(
            "Remote %s or API key/secret not found in the config. "
            "Try registering with sgr cloud register or logging in "
            "with sgr cloud login / sgr cloud login-api."
        ) from e

    ddn_params = engine.conn_params.copy()
    ddn_params["SG_ENGINE_DB_NAME"] = _DDN_DBNAME
    return ddn_params


@click.command("sql")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.option("-a", "--show-all", is_flag=True, help="Return all results of the query.")
@click.option("-j", "--json", is_flag=True, help="Return results as JSON")
@click.argument("query", type=str, default="")
def sql_c(remote, show_all, json, query):
    """
    Run SQL on or connect to the Splitgraph Data Delivery Network.

    If a query isn't passed, this will return a libpq-compatible connection string to
    the registry's SQL endpoint. It can be used to connect to the endpoint with other SQL clients:

    ```
    pgcli $(sgr cloud sql)
    ```

    If a query is passed, this will run an SQL query against the SQL endpoint.
    """
    ddn_params = _get_ddn_conn_params(remote)
    from splitgraph.engine.postgres.engine import PostgresEngine, get_conn_str

    if not query:
        click.echo(get_conn_str(ddn_params))
        return

    # Build an engine to connect to the DDN, disable pre-flight API checks etc
    engine = PostgresEngine(
        name=remote, conn_params=ddn_params, registry=False, check_version=False
    )

    try:
        results = engine.run_sql(query)
        emit_sql_results(results, use_json=json, show_all=show_all)
    finally:
        engine.close()


@click.command("readme")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.argument("repository", type=RepositoryType(exists=False))
@click.argument("readme", type=click.File("r"))
def readme_c(remote, repository, readme):
    """Upload a README to a Splitgraph repository.

    The repository must have already been pushed. The README must be a file in Markdown format.
    """
    from splitgraph.cloud import GQLAPIClient

    client = GQLAPIClient(remote)
    client.upsert_readme(
        namespace=repository.namespace, repository=repository.repository, readme=readme.read()
    )
    click.echo("README updated for repository %s." % str(repository))


@click.command("description")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.argument("repository", type=RepositoryType(exists=False))
@click.argument("description", type=str)
def description_c(remote, repository, description):
    """Upload a description to a Splitgraph repository.

    The repository must have already been pushed. The description should be plain text, 160 characters or shorter.
    """
    from splitgraph.cloud import GQLAPIClient

    client = GQLAPIClient(remote)
    client.upsert_description(
        namespace=repository.namespace, repository=repository.repository, description=description
    )
    click.echo("Description updated for repository %s." % str(repository))


@click.command("metadata")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.argument("repository", type=RepositoryType(exists=False))
@click.argument("metadata_file", type=click.File("r"), default="splitgraph.yml")
@click.pass_context
def metadata_c(ctx, remote, repository, metadata_file):
    """Upload a metadata file to a Splitgraph repository.

    This can manipulate the repository's short description, README, topics, license, sources and extra metadata.

    The metadata file must be a YAML file. Omitting a key doesn't delete the value.

    For example:

    \b
    ```
    readme: dataset-readme.md
    description: Dataset description (160 characters max).
    topics:
      - topic_1
      - topic_2
    sources:
      - anchor: Source
        href: https://www.splitgraph.com
        isCreator: true
        isSameAs: false
      - anchor: Source 2
        href: https://www.splitgraph.com
        isCreator: false
        isSameAs: true
    license: Public Domain
    extra_metadata:
      key_1:
        key_1_1: value_1_1
        key_1_2: value_1_2
      key_2:
        key_2_1: value_2_1
        key_2_2: value_2_2
    ```
    """
    import yaml
    from splitgraph.cloud import GQLAPIClient

    metadata = Metadata.parse_obj(yaml.safe_load(metadata_file))
    metadata = _prepare_metadata(metadata)

    client = GQLAPIClient(remote)
    client.upsert_metadata(repository.namespace, repository.repository, metadata)
    click.echo("Metadata updated for repository %s." % str(repository))


def _prepare_metadata(metadata, readme_basedir="."):
    keys = ["readme", "description", "topics", "sources", "license", "extra_metadata"]
    if all(metadata.__getattribute__(k) is None for k in keys):
        raise click.UsageError(
            "Invalid metadata file. File must contain at least one of " f"{'/'.join(keys)} keys."
        )
    readme_file = None
    if isinstance(metadata.readme, str):
        readme_file = metadata.readme
    elif isinstance(metadata.readme, Metadata.Readme) and metadata.readme.file:
        readme_file = metadata.readme.file
    if readme_file:
        with open(os.path.join(readme_basedir, readme_file), "r") as f:
            metadata.readme = Metadata.Readme(text=f.read())

    return metadata


@click.command("search")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.option(
    "--limit", default=10, type=click.IntRange(10, 30), help="Number of results to return"
)
@click.argument("query", type=str)
def search_c(remote, query, limit=10):
    """
    Search for a repository on the Splitgraph registry.

    For more advanced search, including filtering by topics, go to the registry's website itself.
    """

    from splitgraph.cloud import GQLAPIClient

    client = GQLAPIClient(remote)
    total_count, repos_previews = client.find_repository(query=query, limit=limit)

    terminal_width, _ = shutil.get_terminal_size((80, 25))

    for ns, repo, preview in repos_previews:
        full_repo = ns + "/" + repo
        click.echo(Color.BOLD + full_repo + Color.END)
        click.echo(Color.BLUE + _construct_repo_url(client.endpoint, full_repo) + Color.END)
        click.echo(
            wrap_text(
                preview.replace("<<", Color.BOLD).replace(">>", Color.END),
                width=min(terminal_width, 100),
                initial_indent="  ",
                subsequent_indent="  ",
                preserve_paragraphs=True,
            )
        )
        click.echo("\n")

    click.echo("\nTotal results: %d" % total_count)
    if len(repos_previews) < total_count:
        click.echo(
            "Visit %s for more search options and full results."
            % (Color.BLUE + _construct_search_url(client.endpoint, query) + Color.END)
        )


def _normalise_filename(filename):
    """
    Strips out odd characters from a string so that it can be used as a valid filename.
    In order for the result to be unique, we also make a short hash of the original
    string and stick it on the end
    """
    base = "".join(c for c in filename if c in VALID_FILENAME_CHARACTERS)
    short_hash = hashlib.sha1(filename.encode()).hexdigest()[:4]  # nosec
    return f"{base}.{short_hash}"


@click.command("dump")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.option("--readme-dir", default="./readmes", help="Directory to dump the data into")
@click.option("--repositories-file", "-f", default="repositories.yml", type=click.File("w"))
@click.argument("limit_repositories", type=str, nargs=-1)
def dump_c(remote, readme_dir, repositories_file, limit_repositories):
    """
    Dump a Splitgraph catalog to a YAML file.

    This creates a repositories.yml file in the target directory as well as a subdirectory `readmes`
    with all the repository readmes. This file can be used to recreate all catalog metadata
    and all external data source settings for a repository using `sgr cloud load`.
    """
    import yaml
    from splitgraph.cloud import GQLAPIClient

    client = GQLAPIClient(remote)
    repositories = client.load_all_repositories(limit_to=limit_repositories)

    _dump_readmes_to_dir(repositories, readme_dir)

    yaml.dump(
        {"repositories": [r.dict(by_alias=True, exclude_unset=True) for r in repositories]},
        repositories_file,
        sort_keys=False,
    )


@click.command("load")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
@click.option(
    "--readme-dir", default="./readmes", help="Path to the directory with the README files"
)
@click.option("--repositories-file", "-f", default="repositories.yml", type=click.File("r"))
@click.argument("limit_repositories", type=str, nargs=-1)
def load_c(remote, readme_dir, repositories_file, limit_repositories):
    """
    Load a Splitgraph catalog from a YAML file.

    This will load a repositories.yml file and the `readmes` subdirectory produced by
    `sgr cloud dump` back into a remote Splitgraph catalog.

    The format is an extension of the format accepted by `sgr cloud metadata` to include multiple
    repositories. README files are read from the `readmes` subdirectory.

    \b
    ```
    credentials:      # Optional credentials to access remote data sources
      my_bucket:
        plugin: csv
        data:
          s3_access_key: ...
          s3_secret_key: ...
    repositories:
    - namespace: my_username
      repository: repository
      metadata:
        readme: dataset-readme.md
        description: Dataset description (160 characters max).
        topics:
          - topic_1
          - topic_2
        sources:
          - anchor: Source
            href: https://www.splitgraph.com
            isCreator: true
            isSameAs: false
          - anchor: Source 2
            href: https://www.splitgraph.com
            isCreator: false
            isSameAs: true
        license: Public Domain
        extra_metadata:
          key_1:
            key_1_1: value_1_1
            key_1_2: value_1_2
          key_2:
            key_2_1: value_2_1
            key_2_2: value_2_2
      external:
        credential: my_bucket
        plugin: csv
        params:
          s3_bucket: my_bucket
        tables:
          table_1:
            schema:
            - name: column_1
              type: text
            - name: column_2
              type: integer
            options:
              s3_object: some/s3_key.csv
    ```
    """
    import yaml
    from splitgraph.cloud import GQLAPIClient, RESTAPIClient

    repo_yaml = RepositoriesYAML.parse_obj(yaml.safe_load(repositories_file))

    # Set up and load credential IDs from the remote to allow users to refer to them by ID
    # or by a name.
    rest_client = RESTAPIClient(remote)
    gql_client = GQLAPIClient(remote)
    credential_map = _build_credential_map(rest_client, credentials=repo_yaml.credentials or {})

    repositories = repo_yaml.repositories

    if limit_repositories:
        repositories = [
            r for r in repositories if f"{r.namespace}/{r.repository}" in limit_repositories
        ]

    logging.info("Uploading images...")
    external_repositories = []
    for repository in repositories:
        if repository.external:
            external_repository = AddExternalRepositoryRequest.from_external(
                repository.namespace, repository.repository, repository.external, credential_map
            )
            external_repositories.append(external_repository)
    rest_client.bulk_upsert_external(repositories=external_repositories)
    logging.info(f"Uploaded images for {pluralise('repository', len(external_repositories))}")

    logging.info("Updating metadata...")
    namespace_list = []
    repository_list = []
    metadata_list = []
    for repository in repositories:
        if repository.metadata:
            namespace_list.append(repository.namespace)
            repository_list.append(repository.repository)

            metadata = _prepare_metadata(repository.metadata, readme_basedir=readme_dir)
            metadata_list.append(metadata)
    gql_client.bulk_upsert_metadata(namespace_list, repository_list, metadata_list)
    logging.info(f"Updated metadata for {pluralise('repository', len(repository_list))}")


def _build_credential_map(auth_client, credentials=None):
    credential_map = {}
    if credentials:
        logging.info("Setting up credentials on the remote...")
        for credential_name, credential in credentials.items():
            credential_id = auth_client.ensure_external_credential(
                credential_data=credential.data,
                credential_name=credential_name,
                plugin_name=credential.plugin,
            )
            logging.info("%s: %s", credential_name, credential_id)
            credential_map[credential_name] = credential_id

    # Load any other credentials too (so that we can use existing credentials by
    # name in YAML files without redefining them)
    response = auth_client.list_external_credentials()
    for credential in response.credentials:
        if credential.credential_name not in credential_map:
            credential_map[credential.credential_name] = credential.credential_id

    return credential_map


def _dump_readmes_to_dir(repositories, readme_dir):
    """
    READMEs aren't rendering very nicely in YAML so we instead write them out to
    individual files and replace the README contents with their file paths.

    :param repositories: will be modified by this function
    """
    try:
        os.mkdir(readme_dir)
    except FileExistsError:
        logging.info("Directory %s already exists, cleaning out", readme_dir)
        for path in glob(os.path.join(readme_dir, "*.md")):
            os.unlink(path)
    for repo in repositories:
        if not repo.metadata:
            continue

        readme_text = None
        if isinstance(repo.metadata.readme, str):
            readme_text = repo.metadata.readme
        if (
            isinstance(repo.metadata.readme, Metadata.Readme)
            and repo.metadata.readme.file is None
            and repo.metadata.readme.text is not None
        ):
            readme_text = repo.metadata.readme.text

        if readme_text:
            filename = f"{repo.namespace}-{repo.repository}"
            filename = _normalise_filename(filename)
            filename = f"{filename}.md"
            logging.info("Writing %s", filename)
            path = os.path.join(readme_dir, filename)
            with open(path, "w") as f:
                f.write(readme_text)
            repo.metadata.readme = Metadata.Readme(file=filename)


@click.command("token")
@click.option("--remote", default="data.splitgraph.com", help="Name of the remote registry to use.")
def token_c(remote):
    """Output an up-to-date access token for a remote."""
    from splitgraph.cloud import RESTAPIClient

    client = RESTAPIClient(remote)
    click.echo(client.access_token)


@click.command("add")
@click.option("--remote", help="Name for the remote (infer from the domain name by default)")
@click.option(
    "-s", "--skip-inject", is_flag=True, help="Don't try to copy the config into all engines"
)
@click.argument("domain_name")
def add_c(remote, skip_inject, domain_name):
    """Add a remote Splitgraph registry to .sgconfig with default parameters"""
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    if domain_name.startswith(("data", "www", "api")):
        raise click.BadArgumentUsage(
            "Use the base domain name, e.g. " "splitgraph.com instead of data.splitgraph.com"
        )

    if not remote:
        remote = domain_name.split(".")[0]

    remote_params_patch: Dict[str, str] = {
        "SG_IS_REGISTRY": "true",
        "SG_ENGINE_HOST": f"data.{domain_name}",
        "SG_ENGINE_PORT": "5432",
        "SG_ENGINE_DB_NAME": "sgregistry",
        "SG_AUTH_API": f"https://api.{domain_name}/auth",
        "SG_QUERY_API": f"https://data.{domain_name}",
        "SG_GQL_API": f"https://api.{domain_name}/gql/cloud/graphql",
    }

    click.echo("Adding remote %s to the config. Parameters: %s" % (remote, remote_params_patch))

    remote_params = get_all_in_subsection(CONFIG, "remotes", remote) or {}
    remote_params.update(remote_params_patch)

    config_patch = {
        "SG_REPO_LOOKUP": _update_repo_lookup(CONFIG, remote),
        "remotes": {remote: remote_params},
    }
    config_path = patch_and_save_config(CONFIG, config_patch)

    if not skip_inject:
        inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)

    click.echo(
        "Done. You can now register or "
        "log in to the instance with sgr cloud register/login(-api)."
    )


@click.group("cloud")
def cloud_c():
    """Manage connections to Splitgraph Cloud."""


cloud_c.add_command(login_c)
cloud_c.add_command(login_api_c)
cloud_c.add_command(register_c)
cloud_c.add_command(curl_c)
cloud_c.add_command(sql_c)
cloud_c.add_command(readme_c)
cloud_c.add_command(description_c)
cloud_c.add_command(metadata_c)
cloud_c.add_command(search_c)
cloud_c.add_command(dump_c)
cloud_c.add_command(load_c)
cloud_c.add_command(token_c)
cloud_c.add_command(add_c)
