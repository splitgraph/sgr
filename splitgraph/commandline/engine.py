import os
import time
from io import BytesIO
from tarfile import TarFile, TarInfo

import click
import docker
from docker.types import Mount

from splitgraph import CONFIG
from splitgraph.config.export import serialize_config, overwrite_config
from splitgraph.config.config import patch_config
from splitgraph.engine.postgres.engine import PostgresEngine


DEFAULT_ENGINE = "default"


def copy_to_container(container, source_path, target_path):
    """
    Copy a file into a Docker container

    :param container: Container object
    :param source_path: Source file path
    :param target_path: Target file path (in the container)
    :return:
    """
    # https://github.com/docker/docker-py/issues/1771
    with open(source_path, "rb") as f:
        data = f.read()

    tarinfo = TarInfo(name=os.path.basename(target_path))
    tarinfo.size = len(data)
    tarinfo.mtime = time.time()

    stream = BytesIO()
    tar = TarFile(fileobj=stream, mode="w")
    tar.addfile(tarinfo, BytesIO(data))
    tar.close()

    stream.seek(0)
    container.put_archive(path=os.path.dirname(target_path), data=stream)


def get_container_name(engine_name):
    return "splitgraph_engine_" + engine_name


def get_data_volume_name(engine_name):
    return "splitgraph_engine_%s_data" % engine_name


def get_metadata_volume_name(engine_name):
    return "splitgraph_engine_%s_metadata" % engine_name


def print_table(rows, column_width=15):
    click.echo(
        "\n".join(["".join([("{:" + str(column_width) + "}").format(x) for x in r]) for r in rows])
    )


@click.group(name="engine")
def engine_c():
    """Manage running Splitgraph engines. This is a wrapper around the relevant Docker commands."""


@click.command(name="list")
@click.option(
    "-a", "--include-all", is_flag=True, default=False, help="Include stopped engine containers."
)
def list_engines_c(include_all):
    """List Splitgraph engines.

    This only lists Docker containers that were created by sgr engine
    (whose names start with "splitgraph_engine_". To operate other engines,
    use Docker CLI directly.
    """
    client = docker.from_env()
    containers = client.containers.list(filters={"ancestor": "splitgraph/engine"}, all=include_all)

    if containers:
        our_containers = []
        for container in containers:
            container_name = container.name
            if not container_name.startswith("splitgraph_engine_"):
                continue
            engine_name = container_name[18:]
            ports = container.attrs["NetworkSettings"]["Ports"]
            ports = ",".join("%s -> %s" % i for i in ports.items())
            our_containers.append((engine_name, container.short_id, container.status, ports))

        print_table([("Name", "Docker ID", "Status", "ports")] + our_containers)


@click.command(name="add")
@click.option(
    "-i",
    "--image",
    default="splitgraph/engine:latest",
    help="Docker image with the Splitgraph engine",
)
@click.option("-p", "--port", type=int, default=5432, help="Port to start the engine on")
@click.option("-u", "--username", default="sgr")
@click.option("--no-init", default=False, help="Don't run `sgr init` on the engine", is_flag=True)
@click.option(
    "--no-sgconfig", default=False, help="Don't add the engine to .sgconfig", is_flag=True
)
@click.option(
    "--inject-source",
    default=False,
    help="Inject the Splitgraph source code into the engine",
    is_flag=True,
)
@click.option("--no-pull", default=False, help="Don't pull the Docker image", is_flag=True)
@click.argument("name", default=DEFAULT_ENGINE)
@click.password_option()
def add_engine_c(
    image, port, username, no_init, no_sgconfig, inject_source, no_pull, name, password
):
    """
    Create and start a Splitgraph engine.

    This will pull the Splitgraph engine image, start it, create a Postgres user and initialize
    the engine.

    This also creates Docker volumes required to persist data/metadata.
    """

    client = docker.from_env()

    if not no_pull:
        click.echo("Pulling image %s..." % image)
        client.images.pull(image)

    container_name = get_container_name(name)
    data_name = get_data_volume_name(name)
    metadata_name = get_metadata_volume_name(name)

    # Setup required mounts for data/metadata
    data_volume = Mount(target="/var/lib/splitgraph/objects", source=data_name, type="volume")
    metadata_volume = Mount(target="/var/lib/postgresql/data", source=metadata_name, type="volume")
    mounts = [data_volume, metadata_volume]

    click.echo("Creating container %s." % container_name)
    click.echo("Data volume: %s." % data_name)
    click.echo("Metadata volume: %s." % metadata_name)

    if inject_source:
        source_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        source_volume = Mount(target="/splitgraph/splitgraph", source=source_path, type="bind")
        mounts.append(source_volume)
        click.echo("Source path: %s" % source_path)

    container = client.containers.run(
        image=image,
        detach=True,
        name=container_name,
        ports={"5432/tcp": port},
        mounts=mounts,
        environment={
            "POSTGRES_USER": username,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": "splitgraph",
            # Actual config to be injected later
            "SG_CONFIG_FILE": "/.sgconfig",
        },
    )

    click.echo("Container created, ID %s" % container.short_id)
    conn_params = {
        "SG_ENGINE_HOST": "localhost",
        "SG_ENGINE_PORT": port,
        "SG_ENGINE_USER": username,
        "SG_ENGINE_PWD": password,
        "SG_ENGINE_DB_NAME": "splitgraph",
        "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
        "SG_ENGINE_ADMIN_USER": username,
        "SG_ENGINE_ADMIN_PWD": password,
    }

    if not no_init:
        click.echo("Initializing the engine")

        engine = PostgresEngine(name=name, conn_params=conn_params)
        engine.initialize()
        engine.commit()
        click.echo("Engine initialized successfully.")

    config_path = CONFIG["SG_CONFIG_FILE"]
    if not no_sgconfig:
        if not config_path:
            click.echo("No config file detected, creating one locally")
            config_path = ".sgconfig"
        else:
            click.echo("Updating the existing config file at %s" % config_path)

        if name != DEFAULT_ENGINE:
            config_patch = {"remotes": {name: conn_params}}
        else:
            config_patch = conn_params

        new_config = patch_config(CONFIG, config_patch)
        overwrite_config(new_config, config_path)

    click.echo("Copying in the config file")
    copy_to_container(container, config_path, "/.sgconfig")
    click.echo("Done.")


@click.command(name="stop")
@click.argument("name", default=DEFAULT_ENGINE)
def stop_engine_c(name):
    """Stop a Splitgraph engine."""

    client = docker.from_env()
    container_name = get_container_name(name)
    container = client.containers.get(container_name)

    click.echo("Stopping Splitgraph engine %s..." % name)
    container.stop()
    click.echo("Engine stopped.")


@click.command(name="start")
@click.argument("name", default=DEFAULT_ENGINE)
def start_engine_c(name):
    """Stop a Splitgraph engine."""

    client = docker.from_env()
    container_name = get_container_name(name)
    container = client.containers.get(container_name)

    click.echo("Starting Splitgraph engine %s..." % name)
    container.start()
    click.echo("Engine started.")


@click.command(name="delete")
@click.option("-y", "--yes", default=False, is_flag=True, help="Do not prompt for confirmation.")
@click.option(
    "-f", "--force", default=False, is_flag=True, help="Delete the engine anyway if it's running."
)
@click.argument("name", default=DEFAULT_ENGINE)
def delete_engine_c(yes, force, name):
    """Stop a Splitgraph engine."""
    client = docker.from_env()
    container_name = get_container_name(name)
    container = client.containers.get(container_name)

    click.echo(
        "Splitgraph engine %s (container ID %s), together with all data, will be deleted."
        % (name, container.short_id)
    )
    if not yes:
        click.confirm("Continue? ", abort=True)

    container.remove(force=force)
    click.echo("Splitgraph engine %s has been removed." % name)

    metadata_volume = client.volumes.get(get_metadata_volume_name(name))
    data_volume = client.volumes.get(get_data_volume_name(name))

    metadata_volume.remove()
    data_volume.remove()
    click.echo("Volumes %s and %s have been removed." % (metadata_volume.name, data_volume.name))


engine_c.add_command(list_engines_c)
engine_c.add_command(add_engine_c)
engine_c.add_command(stop_engine_c)
engine_c.add_command(start_engine_c)
engine_c.add_command(delete_engine_c)
