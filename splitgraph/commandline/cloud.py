"""Command line routines related to registering/setting up connections to the Splitgraph registry."""
import logging
import subprocess
from urllib.parse import urlsplit

import click


@click.command("register")
@click.option("--username", prompt=True)
@click.password_option()
@click.option("--email", prompt=True)
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote cloud engine to register on.",
)
def register_c(username, password, email, remote):
    """
    Register the user on splitgraph.com, obtain a set of machine credentials
    and configure the data.splitgraph.com engine.
    """
    from splitgraph.cloud import AuthAPIClient
    from splitgraph.config import CONFIG
    from splitgraph.config.config import patch_config
    from splitgraph.config.export import overwrite_config

    client = AuthAPIClient(remote)
    click.echo("Registering the user...")

    uuid = client.register(username, password, email)
    click.echo("Registration successful. UUID %s" % uuid)

    access, refresh = client.get_refresh_token(username, password)
    click.echo("Got access/refresh tokens")

    key, secret = client.create_machine_credentials(access, password)
    click.echo("Got key/secret: %s/%s" % (key, secret))

    repo_lookup = CONFIG.get("SG_REPO_LOOKUP")
    if repo_lookup:
        repo_lookup = repo_lookup.split(",")
        if remote not in repo_lookup:
            repo_lookup.append(remote)
    else:
        repo_lookup = [remote]

    config_patch = {
        "SG_REPO_LOOKUP": ",".join(repo_lookup),
        "SG_S3_HOST": "objectstorage",
        "SG_S3_PORT": "9000",
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

    config_path = CONFIG["SG_CONFIG_FILE"]

    if not config_path:
        click.echo("No config file detected, creating one locally")
        config_path = ".sgconfig"
    else:
        click.echo("Updating the existing config file at %s" % config_path)

    new_config = patch_config(CONFIG, config_patch)
    overwrite_config(new_config, config_path)

    click.echo("Done.")


@click.command("curl", context_settings=dict(ignore_unknown_options=True))
@click.option(
    "--remote",
    default="data.splitgraph.com",
    help="Name of the remote cloud engine to register on.",
)
@click.argument("request")
@click.argument("curl_args", nargs=-1, type=click.UNPROCESSED)
def curl_c(remote, request, curl_args):
    """A thin wrapper around curl that performs an HTTP request to Splitgraph Cloud to
    query a dataset using Postgrest. The actual invocation is:

    curl [API endpoint][request] -H [access_token] [extra curl args].

    The request must be of the form namespace/repository/hash_or_tag/table?[postgrest request]".
    Image hash or tag can be omitted, in which case "latest" is used.
    """
    from splitgraph.config import CONFIG
    from splitgraph.cloud import AuthAPIClient

    # Do some early validation
    request_parsed = urlsplit(request)
    path_segments = request_parsed.path.lstrip("/").split("/")

    if len(path_segments) == 3:
        path_segments = path_segments[:2] + ["latest"] + path_segments[2:]
    elif len(path_segments) != 4:
        raise click.BadArgumentUsage("Invalid request, see sgr cloud help for the correct format!")

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

    logging.info("Calling %s", " ".join(subprocess_args))
    subprocess.call(subprocess_args)


@click.group("cloud")
def cloud_c():
    """Manage connections to Splitgraph Cloud."""


cloud_c.add_command(register_c)
cloud_c.add_command(curl_c)
