"""Command line routines related to registering/setting up connections to the Splitgraph registry."""
import logging
import subprocess
from urllib.parse import urlsplit

import click

from splitgraph.cloud import get_token_claim, get_headers
from splitgraph.commandline.common import ImageType
from splitgraph.commandline.engine import patch_and_save_config, inject_config_into_engines


@click.command("register")
@click.option("--username", prompt=True)
@click.password_option()
@click.option("--email", prompt=True)
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote cloud engine to register on.",
)
@click.option("--accept-tos", is_flag=True, help="Accept the registry's Terms of Service")
def register_c(username, password, email, remote, accept_tos):
    """
    Register the user on a Splitgraph registry.

    By default, this registers the user on data.splitgraph.com,
    obtains a set of machine (API) credentials for the client to communicate
    with the registry and configures the data.splitgraph.com engine.
    """
    from splitgraph.cloud import AuthAPIClient
    from splitgraph.config import CONFIG

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

    config_patch = {
        "SG_REPO_LOOKUP": repo_lookup,
        "remotes": {
            remote: {
                "SG_ENGINE_USER": key,
                "SG_ENGINE_PWD": secret,
                "SG_NAMESPACE": username,
                "SG_CLOUD_REFRESH_TOKEN": refresh,
                "SG_CLOUD_ACCESS_TOKEN": access,
                "SG_IS_REGISTRY": "true",
            }
        },
    }
    config_path = patch_and_save_config(CONFIG, config_patch)
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


@click.command("login")
@click.option("--username", prompt="Username or e-mail")
@click.password_option(confirmation_prompt=False)
@click.option(
    "--remote", default="data.splitgraph.com", help="Name of the remote registry to log into.",
)
@click.option(
    "--overwrite", is_flag=True, help="Overwrite old API keys in the config if they exist"
)
def login_c(username, password, remote, overwrite):
    """Log into a Splitgraph registry.

    This will generate a new refresh token (to use the Splitgraph query API)
    and API keys to let sgr access the registry (if they don't already exist
    in the configuration file or if the actual username has changed).
    """
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient

    client = AuthAPIClient(remote)

    access, refresh = client.get_refresh_token(username, password)

    # Extract namespace from the access token since we might have logged in with an e-mail.
    namespace = get_token_claim(access, "username")

    click.echo("Logged into %s as %s" % (remote, namespace))
    config_patch = {
        "SG_REPO_LOOKUP": _update_repo_lookup(CONFIG, remote),
        "remotes": {
            remote: {
                "SG_NAMESPACE": namespace,
                "SG_CLOUD_REFRESH_TOKEN": refresh,
                "SG_CLOUD_ACCESS_TOKEN": access,
            }
        },
    }

    # Get new tokens in any case if we're logging in under a different username.
    try:
        username_changed = namespace != CONFIG["remotes"][remote]["SG_NAMESPACE"]
    except KeyError:
        username_changed = False

    if (
        "SG_ENGINE_USER" not in CONFIG["remotes"][remote]
        or "SG_ENGINE_PWD" not in CONFIG["remotes"][remote]
        or overwrite
        or username_changed
    ):
        key, secret = client.create_machine_credentials(access, password)
        config_patch["remotes"][remote]["SG_ENGINE_USER"] = key
        config_patch["remotes"][remote]["SG_ENGINE_PWD"] = secret
        click.echo("Acquired new API keys")

    config_path = patch_and_save_config(CONFIG, config_patch)
    inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)


@click.command("curl", context_settings=dict(ignore_unknown_options=True))
@click.option(
    "--remote", default="data.splitgraph.com", help="Name of the remote cloud engine to use."
)
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
    interact with a dataset using PostgREST (http://postgrest.org) or the Splitfile executor.

    The actual invocation is:

        curl [API endpoint][request] -H [access_token] [extra curl args].

    The image must be of the form `namespace/repository:[hash_or_tag (default latest)]`.

    The actual request parameters depend on the request type:

      * For PostgREST: `/table?[postgrest request]` or empty to get the OpenAPI spec for this image.
        For a reference on how to perform Postgrest requests, see http://postgrest.org/en/v6.0/api.html.
      * For the Splitfile executor: a JSON array to be POSTed to the executor, e.g.
        `'{"command": "FROM some/repo IMPORT some_table AS alias", "tag": "new_tag"}'`.

    --curl-args allows to pass extra arguments to curl. Note that every argument must be prefixed
    with --curl-args, e.g. --curl-args --cacert --curl-args /path/to/ca.pem
    """
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient

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


@click.group("cloud")
def cloud_c():
    """Manage connections to Splitgraph Cloud."""


cloud_c.add_command(login_c)
cloud_c.add_command(register_c)
cloud_c.add_command(curl_c)
