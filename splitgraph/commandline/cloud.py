"""Command line routines related to registering/setting up connections to the Splitgraph registry."""
import logging
import shutil
import subprocess
from copy import copy
from typing import Dict, Optional
from urllib.parse import urlparse, quote

import click
from click import wrap_text

from splitgraph.commandline.common import (
    ImageType,
    RepositoryType,
    emit_sql_results,
    Color,
)
from splitgraph.commandline.engine import patch_and_save_config, inject_config_into_engines

# Hardcoded database name for the Splitgraph DDN (ddn instead of sgregistry)
_DDN_DBNAME = "ddn"


@click.command("register")
@click.option("--username", prompt=True)
@click.password_option()
@click.option("--email", prompt=True)
@click.option(
    "--remote", default="data.splitgraph.com", help="Name of the remote registry to register on.",
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
    from splitgraph.cloud import AuthAPIClient, DEFAULT_REMOTES
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    client = AuthAPIClient(remote)
    tos = client.tos()

    if tos and not accept_tos:
        click.echo("%s says: %s" % (client.endpoint, tos))
        click.confirm("Do you accept the Terms of Service?", default=False, abort=True)
    click.echo("Registering the user...")

    uuid, access, refresh = client.register(username, password, email, accept_tos=True)
    click.echo("Registration successful. UUID %s" % uuid)

    key, secret = client.create_machine_credentials(access, password)
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
    "--remote", default="data.splitgraph.com", help="Name of the remote registry to log into.",
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
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection
    from splitgraph.cloud import AuthAPIClient, get_token_claim, DEFAULT_REMOTES

    client = AuthAPIClient(remote)

    if not password:
        profile_url = _construct_user_profile_url(client.endpoint)
        password = click.prompt(
            text="Password (visit %s if you don't have it)" % profile_url,
            confirmation_prompt=False,
            hide_input=True,
        )

    access, refresh = client.get_refresh_token(username, password)

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
        username_changed = namespace != CONFIG["remotes"][remote]["SG_NAMESPACE"]
    except KeyError:
        username_changed = False

    if (
        "SG_ENGINE_USER" not in config_remote_params
        or "SG_ENGINE_PWD" not in config_remote_params
        or overwrite
        or username_changed
    ):
        key, secret = client.create_machine_credentials(access, password)
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
    "--remote", default="data.splitgraph.com", help="Name of the remote registry to log into.",
)
@click.option(
    "-s", "--skip-inject", is_flag=True, help="Don't try to copy the config into all engines"
)
def login_api_c(api_key, api_secret, remote, skip_inject):
    """Log into a Splitgraph registry using existing API keys.

    This will inject the API keys for the registry into the configuration file
    and generate a new access token.
    """
    from splitgraph.cloud import AuthAPIClient, get_token_claim, DEFAULT_REMOTES
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_all_in_subsection

    client = AuthAPIClient(remote)
    access = client.get_access_token_from_api(api_key, api_secret)

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
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient, get_headers

    repository, hash_or_tag = image

    # Craft a request
    config = CONFIG["remotes"][remote]
    access_token = AuthAPIClient(remote).access_token
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
    from splitgraph.engine.postgres.engine import get_conn_str, PostgresEngine

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
    """Upload or a README to a Splitgraph repository.

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

This can manipulate the repository's short description, README and topics.

The metadata file must be a YAML file with the keys `readme`, `description` and `topics`. Omitting a key doesn't delete the value.

For example:

\b
```
readme: dataset-readme.md
description: Dataset description (160 characters max).
topics:
  - topic_1
  - topic_2
```
    """
    import yaml
    from splitgraph.cloud import GQLAPIClient

    metadata = yaml.safe_load(metadata_file)

    if "readme" not in metadata and "description" not in metadata and "topics" not in metadata:
        raise click.UsageError(
            "Invalid metadata file. File must contain at least one of "
            "readme/description/topics keys."
        )

    if "readme" in metadata:
        with open(metadata["readme"], "r") as f:
            ctx.invoke(readme_c, remote=remote, repository=repository, readme=f)

    if "description" in metadata:
        ctx.invoke(
            description_c, remote=remote, repository=repository, description=metadata["description"]
        )

    if "topics" in metadata:
        # To clear out the topics, pass in an empty list instead of None.
        topics = metadata["topics"] or []
        client = GQLAPIClient(remote)
        client.upsert_topics(
            namespace=repository.namespace, repository=repository.repository, topics=topics,
        )
        click.echo("Topics updated for repository %s." % str(repository))


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
