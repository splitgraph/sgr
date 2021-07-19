import logging
import os
from contextlib import contextmanager
import socket
from typing import List, Tuple, Any

import docker.errors
from docker import DockerClient
from docker.models.containers import Container

from splitgraph.commandline.engine import copy_to_container
from splitgraph.exceptions import SplitGraphError


class SubprocessError(SplitGraphError):
    pass


def add_files(container: Container, files: List[Tuple[str, str]]) -> None:
    for var_name, var_data in files:
        if not var_data:
            continue
        copy_to_container(
            container,
            source_path=None,
            target_path=f"/{var_name}.json",
            data=var_data.encode(),
        )


@contextmanager
def remove_at_end(container: Container) -> Container:
    try:
        yield container
    finally:
        try:
            container.remove(force=True)
        except docker.errors.APIError as e:
            logging.warning("Error removing container at the end, continuing", exc_info=e)


def wait_not_failed(container: Container, mirror_logs: bool = False) -> None:
    """
    Block until a Docker container exits.

    :raises SubprocessError if the container exited with a non-zero code.
    """

    if mirror_logs:
        for line in container.logs(stream=True, follow=True):
            logging.info("%s: %s", container.name, line.decode().strip())

    result = container.wait()
    if result["StatusCode"] != 0:
        logging.error("Container %s exited with %d", container.name, result["StatusCode"])
        logs = container.logs(tail=1000) or b""
        for line in logs.decode().splitlines():
            logging.info("%s: %s", container.name, line)
        raise SubprocessError()


def build_command(files: List[Tuple[str, Any]]) -> List[str]:
    command: List[str] = []

    for var_name, var_data in files:
        if not var_data:
            continue
        command.extend([f"--{var_name}", f"/{var_name}.json"])
    return command


def detect_network_mode(client: DockerClient) -> str:
    # We want the receiver to connect to the same engine that we're connected to. If we're
    # running on the host, that means using our own connection parameters and running the
    # receiver with net:host. Inside Docker we have to use the host's Docker socket and
    # attach the container to our own network so that it can also use our own params.

    # This also applies in case we're running a source against a database that's also running
    # in Docker -- we want to mimic sgr too.
    if os.path.exists("/.dockerenv"):
        our_container_id = client.containers.get(socket.gethostname()).id
        return f"container:{our_container_id}"
    else:
        return "host"
