import logging
import os
import platform
import time
from io import BytesIO
from pathlib import Path, PureWindowsPath
from tarfile import TarFile, TarInfo
from typing import Dict, TYPE_CHECKING
from urllib.parse import urlparse

import click
from tqdm import tqdm

from splitgraph.__version__ import __version__
from splitgraph.config import CONFIG, SG_CMD_ASCII
from splitgraph.exceptions import DockerUnavailableError

if TYPE_CHECKING:
    from docker.models.containers import Container


DEFAULT_ENGINE = "default"


def get_docker_client():
    """Wrapper around client.from_env() that also pings the daemon
    to make sure it can connect and if not, raises an error."""
    import docker

    try:
        client = docker.from_env()
        client.ping()
        return client
    except Exception as e:
        raise DockerUnavailableError("Could not connect to the Docker daemon") from e


def copy_to_container(container: "Container", source_path: str, target_path: str) -> None:
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
    tarinfo.mtime = int(time.time())

    stream = BytesIO()
    tar = TarFile(fileobj=stream, mode="w")
    tar.addfile(tarinfo, BytesIO(data))
    tar.close()

    stream.seek(0)
    container.put_archive(path=os.path.dirname(target_path), data=stream.read())


def patch_and_save_config(config, patch):
    from splitgraph.config.config import patch_config
    from splitgraph.config.system_config import HOME_SUB_DIR
    from splitgraph.config.export import overwrite_config
    from pathlib import Path
    import os

    config_path = config["SG_CONFIG_FILE"]
    if not config_path:
        # Default to creating a config in the user's homedir rather than local.
        config_dir = Path(os.environ["HOME"]) / Path(HOME_SUB_DIR)
        config_path = config_dir / Path(".sgconfig")
        logging.debug("No config file detected, creating one at %s" % config_path)
        config_dir.mkdir(exist_ok=True, parents=True)
    else:
        logging.debug("Updating the existing config file at %s" % config_path)
    new_config = patch_config(config, patch)
    overwrite_config(new_config, config_path)
    return str(config_path)


def inject_config_into_engines(engine_prefix, config_path):
    """
    Copy the current config into all engines that are managed by `sgr engine`. This
    is so that the engine has the right credentials and settings for when we do
    layered querying (a Postgres client queries the engine directly and it
    has to download objects etc).

    :param engine_prefix: Prefix for Docker containers that are considered to be engines
    :param config_path: Path to the config file.
    """
    engine_containers = list_engines(engine_prefix, include_all=True)
    if engine_containers:
        logging.info("Copying the config file at %s into all current engines", config_path)
        for container in engine_containers:
            copy_to_container(container, config_path, "/.sgconfig")
            logging.info("Config updated for container %s", container.name)


def _get_container_name(engine_name: str) -> str:
    from splitgraph.config import CONFIG

    return "%s%s" % (CONFIG["SG_ENGINE_PREFIX"], engine_name)


def _get_data_volume_name(engine_name: str) -> str:
    from splitgraph.config import CONFIG

    return "%s%s_data" % (CONFIG["SG_ENGINE_PREFIX"], engine_name)


def _get_metadata_volume_name(engine_name: str) -> str:
    from splitgraph.config import CONFIG

    return "%s%s_metadata" % (CONFIG["SG_ENGINE_PREFIX"], engine_name)


def _convert_source_path(path: str) -> str:
    """If we're passed a Windows-style path to mount into the container,
    we need to convert it to Unix-style for when the user is running Docker
    in a docker-machine VM on Windows. Docker mounts C:\\Users into /c/Users on the
    machine, so anything that's a subdirectory of that is available for bind mounts."""
    pathobj = Path(path)
    if isinstance(pathobj, PureWindowsPath):
        path = str(pathobj.as_posix())
    if path[1] == ":":
        # If the path has a drive letter (C:/Users/... is recognized as a PosixPath on Linux
        # but that's not what we want), we need to convert it into the /c/Users/... mount
        if not path.lower().startswith("c:/users"):
            logging.warning(
                "Windows-style path %s might not be available for bind mounting in Docker", path
            )
        drive_letter = path[0].lower()
        return "/" + drive_letter + path[2:]
    return path


@click.group(name="engine")
def engine_c():
    """Manage running Splitgraph engines. This is a wrapper around the relevant Docker commands."""


def list_engines(prefix, include_all=False, unavailable_ok=True):
    try:
        client = get_docker_client()
    except DockerUnavailableError:
        if not unavailable_ok:
            raise
        logging.warning(
            "Could not connect to the Docker daemon to enumerate engines managed by sgr. This is fine if you're managing the engine yourself with Docker or Compose."
        )
        return []
    containers = client.containers.list(all=include_all)

    return [c for c in containers if c.name.startswith(prefix)]


@click.command(name="list")
@click.option(
    "-a", "--include-all", is_flag=True, default=False, help="Include stopped engine containers."
)
def list_engines_c(include_all):
    """List Splitgraph engines.

    This only lists Docker containers that were created by sgr engine
    (whose names start with `splitgraph_engine_`. To manage other engines,
    use Docker CLI directly.
    """
    from splitgraph.config import CONFIG
    from tabulate import tabulate

    containers = list_engines(include_all=include_all, prefix=CONFIG["SG_ENGINE_PREFIX"])
    if containers:
        our_containers = []
        for container in containers:
            engine_name = container.name[len(CONFIG["SG_ENGINE_PREFIX"]) :]
            ports = container.attrs["NetworkSettings"]["Ports"]
            ports = ",".join("%s -> %s" % i for i in ports.items())
            our_containers.append((engine_name, container.short_id, container.status, ports))

        click.echo(
            tabulate(
                our_containers, headers=("Name", "Docker ID", "Status", "ports"), tablefmt="plain"
            )
        )


def _pretty_pull(client, image):
    # Use the details from the low-level docker API to give us a pull progressbar

    use_ascii = True if platform.system() == "Windows" else SG_CMD_ASCII

    with tqdm(
        total=1,
        desc="Downloading",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        position=0,
        ascii=use_ascii,
    ) as download_bar:
        with tqdm(
            total=1,
            desc="Extracting",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            position=1,
            ascii=use_ascii,
        ) as extract_bar:
            download_progress = {}
            extract_progress = {}
            for progress in client.api.pull(image, stream=True, decode=True):
                progress_text = "%s: %s" % (progress.get("id", ""), progress.get("status", ""))
                if progress["status"] == "Downloading":
                    _update_bar(progress, progress_text, download_bar, download_progress)
                elif progress["status"] == "Extracting":
                    _update_bar(progress, progress_text, extract_bar, extract_progress)


def _update_bar(progress, progress_text, bar, progress_data):
    if progress.get("progressDetail"):
        progress_data[progress["id"]] = progress["progressDetail"]

        bar.total = sum(p["total"] for p in progress_data.values())
        bar.update(sum(p["current"] for p in progress_data.values()) - bar.n)
    bar.set_description_str(progress_text)


@click.command(name="add")
@click.option(
    "-i",
    "--image",
    default="splitgraph/engine:%s" % __version__,
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
    help="Inject the current Splitgraph source code into the engine using Docker bind mounts",
    is_flag=True,
)
@click.option("--no-pull", default=False, help="Don't pull the Docker image", is_flag=True)
@click.option(
    "--set-default",
    default=False,
    help="Set the engine as the default engine in the config regardless of its name",
    is_flag=True,
)
@click.argument("name", default=DEFAULT_ENGINE)
@click.password_option()
def add_engine_c(
    image, port, username, no_init, no_sgconfig, inject_source, no_pull, name, password, set_default
):
    """
    Create and start a Splitgraph engine.

    This will pull the Splitgraph engine image, start it, create a Postgres user and initialize
    the engine.

    This also creates Docker volumes required to persist data/metadata.

    The engine Docker container by default will be named `splitgraph_engine_default` and
    its data and metadata volumes will have names `splitgraph_engine_default_data` and
    `splitgraph_engine_default_metadata`.
    """
    from splitgraph.engine.postgres.engine import PostgresEngine
    from splitgraph.config import CONFIG
    from docker.types import Mount

    client = get_docker_client()

    if not no_pull:
        click.echo("Pulling image %s..." % image)
        _pretty_pull(client, image)

    container_name = _get_container_name(name)
    data_name = _get_data_volume_name(name)
    metadata_name = _get_metadata_volume_name(name)

    # Setup required mounts for data/metadata
    data_volume = Mount(target="/var/lib/splitgraph/objects", source=data_name, type="volume")
    metadata_volume = Mount(target="/var/lib/postgresql/data", source=metadata_name, type="volume")
    mounts = [data_volume, metadata_volume]

    click.echo("Creating container %s." % container_name)
    click.echo("Data volume: %s." % data_name)
    click.echo("Metadata volume: %s." % metadata_name)

    if inject_source:
        source_path = _convert_source_path(
            os.getenv(
                "SG_SOURCE_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
            )
        )
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

    # Extract the host that we can reach the container on
    # (might be different from localhost if docker-machine is used)
    hostname = urlparse(client.api.base_url).hostname

    conn_params: Dict[str, str] = {
        "SG_ENGINE_HOST": hostname,
        "SG_ENGINE_PORT": str(port),
        # Even if the engine is exposed on a different port on the host,
        # need to make sure that it uses the default 5432 port to connect
        # to itself.
        "SG_ENGINE_FDW_HOST": "localhost",
        "SG_ENGINE_FDW_PORT": "5432",
        "SG_ENGINE_USER": username,
        "SG_ENGINE_PWD": password,
        "SG_ENGINE_DB_NAME": "splitgraph",
        "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
        "SG_ENGINE_ADMIN_USER": username,
        "SG_ENGINE_ADMIN_PWD": password,
    }

    if not no_sgconfig:
        if name != DEFAULT_ENGINE and not set_default:
            config_patch = {"remotes": {name: conn_params}}
        else:
            config_patch = conn_params

        config_path = patch_and_save_config(CONFIG, config_patch)
    else:
        config_path = CONFIG["SG_CONFIG_FILE"]

    if not no_init:
        engine = PostgresEngine(name=name, conn_params=conn_params)
        engine.initialize()
        engine.commit()
        click.echo("Engine initialized successfully.")

    inject_config_into_engines(CONFIG["SG_ENGINE_PREFIX"], config_path)
    click.echo("Done.")


@click.command(name="stop")
@click.argument("name", default=DEFAULT_ENGINE)
def stop_engine_c(name):
    """Stop a Splitgraph engine.

    This is a wrapper around the corresponding Docker command.
    """

    client = get_docker_client()
    container_name = _get_container_name(name)
    container = client.containers.get(container_name)

    click.echo("Stopping Splitgraph engine %s..." % name)
    container.stop()
    click.echo("Engine stopped.")


@click.command(name="start")
@click.argument("name", default=DEFAULT_ENGINE)
def start_engine_c(name):
    """Start a Splitgraph engine.

    This is a wrapper around the corresponding Docker command.
    """

    client = get_docker_client()
    container_name = _get_container_name(name)
    container = client.containers.get(container_name)

    click.echo("Starting Splitgraph engine %s..." % name)
    container.start()
    click.echo("Engine started.")


@click.command(name="delete")
@click.option("-y", "--yes", default=False, is_flag=True, help="Do not prompt for confirmation.")
@click.option(
    "-f", "--force", default=False, is_flag=True, help="Delete the engine anyway if it's running."
)
@click.option(
    "-v",
    "--with-volumes",
    default=False,
    is_flag=True,
    help="Include the engine's volumes (if not specified, volumes will be reattached when an engine"
    " with the same name is created).",
)
@click.argument("name", default=DEFAULT_ENGINE)
def delete_engine_c(yes, force, with_volumes, name):
    """Delete the Splitgraph engine container."""

    client = get_docker_client()
    container_name = _get_container_name(name)
    container = client.containers.get(container_name)

    click.echo(
        "Splitgraph engine %s (container ID %s)%s will be deleted."
        % (name, container.short_id, (", together with all data," if with_volumes else ""))
    )
    if not yes:
        click.confirm("Continue? ", abort=True)

    container.remove(force=force)
    click.echo("Splitgraph engine %s has been removed." % name)

    if with_volumes:
        metadata_volume = client.volumes.get(_get_metadata_volume_name(name))
        data_volume = client.volumes.get(_get_data_volume_name(name))

        metadata_volume.remove()
        data_volume.remove()
        click.echo(
            "Volumes %s and %s have been removed." % (metadata_volume.name, data_volume.name)
        )


@click.command("log")
@click.option("-f", "--follow", is_flag=True, help="Stream logs")
@click.argument("name", default=DEFAULT_ENGINE)
def log_engine_c(name, follow):
    """Get logs from a Splitgraph engine."""

    client = get_docker_client()
    container_name = _get_container_name(name)
    container = client.containers.get(container_name)

    if follow:
        for line in container.logs(stream=True):
            click.echo(line, nl=False)
    else:
        click.echo(container.logs())


@click.command("configure")
@click.argument("name", default=DEFAULT_ENGINE)
def configure_engine_c(name):
    """Inject a configuration file into an engine.

    This copies the current .sgconfig file (pointed to by SG_CONFIG_FILE) into
    the engine container, making it use that configuration for
    when it's queried through an application other than the sgr client
    (layered querying)."""

    client = get_docker_client()
    container_name = _get_container_name(name)
    container = client.containers.get(container_name)

    copy_to_container(container, CONFIG["SG_CONFIG_FILE"], "/.sgconfig")
    logging.info("Config updated for container %s", container.name)


@click.command("version")
@click.argument("name", default=DEFAULT_ENGINE)
def version_engine_c(name):
    """Get version of Splitgraph engine."""
    from splitgraph.engine import get_engine

    if name == DEFAULT_ENGINE:
        engine = get_engine()
    else:
        engine = get_engine(name)

    version = engine.splitgraph_version
    if version:
        click.echo("Splitgraph Engine %s" % version)


@click.command("upgrade")
@click.option(
    "-i",
    "--image",
    default="splitgraph/engine:%s" % __version__,
    help="Docker image with the Splitgraph engine",
)
@click.option("--no-pull", is_flag=True, help="Don't pull the new engine image")
@click.argument("name", default=DEFAULT_ENGINE)
@click.pass_context
def upgrade_engine_c(ctx, image, no_pull, name):
    """Upgrade a Splitgraph engine.

    This consists of shutting down the current Splitgraph engine,
    deleting its Docker container (keeping the actual data and
    metadata volumes intact), creating a container based on a newer
    image and finally reinitializing the engine to perform needed migrations.
    """
    from splitgraph.engine import get_engine

    # Get reference to engine to extract its connection params
    if name == DEFAULT_ENGINE:
        engine = get_engine()
    else:
        engine = get_engine(name)

    username = engine.conn_params["SG_ENGINE_USER"]
    password = engine.conn_params["SG_ENGINE_PWD"]
    port = engine.conn_params["SG_ENGINE_PORT"]

    # Stop the engine
    ctx.invoke(stop_engine_c, name=name)

    # Delete the container
    ctx.invoke(delete_engine_c, name=name, yes=True)

    # Create and start new engine
    ctx.invoke(
        add_engine_c,
        image=image,
        port=port,
        username=username,
        password=password,
        no_sgconfig=True,
        no_pull=no_pull,
        name=name,
    )

    version = engine.splitgraph_version
    if version:
        click.echo("Upgraded engine %s to %s" % (name, version))


engine_c.add_command(list_engines_c)
engine_c.add_command(add_engine_c)
engine_c.add_command(stop_engine_c)
engine_c.add_command(start_engine_c)
engine_c.add_command(delete_engine_c)
engine_c.add_command(upgrade_engine_c)
engine_c.add_command(log_engine_c)
engine_c.add_command(configure_engine_c)
engine_c.add_command(version_engine_c)
