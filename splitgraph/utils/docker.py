import os
import time
from io import BytesIO
from tarfile import TarInfo, TarFile
from typing import Optional, TYPE_CHECKING, List

if TYPE_CHECKING:
    from docker.models.containers import Container

from splitgraph.exceptions import DockerUnavailableError


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


def copy_to_container(
    container: "Container",
    source_path: Optional[str],
    target_path: str,
    data: Optional[bytes] = None,
) -> None:
    """
    Copy a file into a Docker container

    :param container: Container object
    :param source_path: Source file path
    :param target_path: Target file path (in the container)
    :return:
    """

    if data is None:
        if not source_path:
            raise ValueError("One of source_path or data must be specified!")
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


def copy_dir_to_container(
    container: "Container",
    source_path: str,
    target_path: str,
    exclude_names: Optional[List[str]] = None,
) -> None:
    stream = BytesIO()
    tar = TarFile(fileobj=stream, mode="w")
    tar.add(
        name=source_path,
        arcname=".",
        recursive=True,
        filter=lambda ti: ti if ti.name in (exclude_names or []) else None,
    )
    tar.close()

    stream.seek(0)
    container.put_archive(path=os.path.dirname(target_path), data=stream.read())
