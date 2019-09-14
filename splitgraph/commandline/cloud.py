"""Command line routines related to registering/setting up connections to the Splitgraph registry."""

import click

from splitgraph import CONFIG
from splitgraph.cloud import AuthAPIClient
from splitgraph.config.export import serialize_config, patch_config


@click.command("register")
@click.option("--username", prompt=True)
@click.password_option()
@click.option("--email", prompt=True)
def register_c(username, password, email):
    """
    Register the user on splitgraph.com, obtain a set of machine credentials
    and configure the data.splitgraph.com engine.
    """

    client = AuthAPIClient(endpoint=CONFIG["SG_AUTH_API"])
    print("Registering the user...")

    uuid = client.register(username, password, email)
    print("Registration successful. UUID %s" % uuid)

    access, refresh = client.get_refresh_token(username, password)
    print("Got access/refresh tokens")

    key, secret = client.create_machine_credentials(access, password)
    print("Got key/secret: %s/%s" % (key, secret))

    name = "data.splitgraph.com"

    repo_lookup = CONFIG.get("SG_REPO_LOOKUP")
    if repo_lookup:
        repo_lookup = repo_lookup.split(",")
        if name not in repo_lookup:
            repo_lookup.append(name)
    else:
        repo_lookup = [name]

    config_patch = {
        "SG_REPO_LOOKUP": ",".join(repo_lookup),
        "SG_S3_HOST": "objectstorage",
        "SG_S3_PORT": "9000",
        "remotes": {
            name: {
                "SG_ENGINE_HOST": "localhost",
                "SG_ENGINE_PORT": 5433,
                "SG_ENGINE_USER": key,
                "SG_ENGINE_PWD": secret,
                "SG_ENGINE_DB_NAME": "sgregistry",
            }
        },
    }

    config_path = CONFIG["SG_CONFIG_FILE"]

    if not config_path:
        print("No config file detected, creating one locally")
        config_path = ".sgconfig"
    else:
        print("Updating the existing config file at %s" % config_path)

    new_config = patch_config(CONFIG, config_patch)

    with open(config_path, "w") as f:
        f.write(
            serialize_config(
                new_config, config_format=True, no_shielding=True, include_defaults=False
            )
        )

    print("Done.")
