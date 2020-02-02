"""Command line routines related to registering/setting up connections to the Splitgraph registry."""
import logging
import subprocess
from urllib.parse import urlsplit

import click

from splitgraph.cloud import get_token_claim


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
    Register the user on splitgraph.com, obtain a set of machine credentials
    and configure the data.splitgraph.com engine.
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

    repo_lookup = CONFIG.get("SG_REPO_LOOKUP")
    if repo_lookup:
        repo_lookup = repo_lookup.split(",")
        if remote not in repo_lookup:
            repo_lookup.append(remote)
    else:
        repo_lookup = [remote]

    config_patch = {
        "SG_REPO_LOOKUP": ",".join(repo_lookup),
        "remotes": {
            remote: {
                "SG_ENGINE_USER": key,
                "SG_ENGINE_PWD": secret,
                "SG_NAMESPACE": username,
                "SG_CLOUD_REFRESH_TOKEN": refresh,
                "SG_CLOUD_ACCESS_TOKEN": access,
            }
        },
    }
    _patch_and_save_config(CONFIG, config_patch)

    click.echo("Done.")


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
    in the configuration file).
    """
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient

    client = AuthAPIClient(remote)

    access, refresh = client.get_refresh_token(username, password)

    # Extract namespace from the access token since we might have logged in with an e-mail.
    namespace = get_token_claim(access, "username")

    click.echo("Logged into %s as %s" % (remote, namespace))
    config_patch = {
        "remotes": {
            remote: {
                "SG_NAMESPACE": namespace,
                "SG_CLOUD_REFRESH_TOKEN": refresh,
                "SG_CLOUD_ACCESS_TOKEN": access,
            }
        },
    }

    if (
        "SG_ENGINE_USER" not in CONFIG["remotes"][remote]
        or "SG_ENGINE_PWD" not in CONFIG["remotes"][remote]
        or overwrite
    ):
        key, secret = client.create_machine_credentials(access, password)
        config_patch["remotes"][remote]["SG_ENGINE_USER"] = key
        config_patch["remotes"][remote]["SG_ENGINE_PWD"] = secret
        click.echo("Acquired new API keys")

    _patch_and_save_config(CONFIG, config_patch)


def _patch_and_save_config(config, patch):
    from splitgraph.config.config import patch_config
    from splitgraph.config.export import overwrite_config

    config_path = config["SG_CONFIG_FILE"]
    if not config_path:
        click.echo("No config file detected, creating one locally")
        config_path = ".sgconfig"
    else:
        click.echo("Updating the existing config file at %s" % config_path)
    new_config = patch_config(config, patch)
    overwrite_config(new_config, config_path)


@click.command("curl", context_settings=dict(ignore_unknown_options=True))
@click.option(
    "--remote", default="data.splitgraph.com", help="Name of the remote cloud engine to use."
)
@click.argument("request")
@click.argument("curl_args", nargs=-1, type=click.UNPROCESSED)
def curl_c(remote, request, curl_args):
    """A thin wrapper around curl that performs an HTTP request to Splitgraph Cloud to
    query a dataset using Postgrest (http://postgrest.org).

    The actual invocation is:

        curl [API endpoint][request] -H [access_token] [extra curl args].

    The request must be of the form `namespace/repository/hash_or_tag/table?[postgrest request]`.

    For a reference on how to perform Postgrest requests, see http://postgrest.org/en/v6.0/api.html.
    """
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient

    # Do some early validation
    request_parsed = urlsplit(request)
    path_segments = request_parsed.path.lstrip("/").split("/")

    # Craft a request
    config = CONFIG["remotes"][remote]
    full_request = (
        config["SG_QUERY_API"]
        + "/"
        + "/".join(path_segments)
        + (("?" + request_parsed.query) if request_parsed.query else "")
    )

    access_token = AuthAPIClient(remote).access_token
    subprocess_args = ["curl", full_request, "-H", "Authorization: Bearer " + access_token] + list(
        curl_args
    )

    logging.debug("Calling %s", " ".join(subprocess_args))
    subprocess.call(subprocess_args)


@click.group("cloud")
def cloud_c():
    """Manage connections to Splitgraph Cloud."""


cloud_c.add_command(login_c)
cloud_c.add_command(register_c)
cloud_c.add_command(curl_c)
