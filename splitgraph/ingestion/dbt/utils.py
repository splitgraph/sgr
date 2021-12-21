import json
import logging
import os
import shlex
import subprocess
from random import getrandbits
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from docker.types import LogConfig
from ruamel.yaml import YAMLError
from splitgraph.ingestion.airbyte.docker_utils import (
    detect_network_mode,
    remove_at_end,
    wait_not_failed,
)
from splitgraph.utils.docker import (
    copy_dir_to_container,
    get_docker_client,
    get_file_from_container,
)
from splitgraph.utils.yaml import safe_dump, safe_load

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine


def prepare_git_repo(url: str, target_path: str, ref: str = "master") -> None:
    """
    Clone a repository from Git and check out a specific branch.

    :param url: Git repository URL. If authentication is required, one may use e.g.
        `https://uname:pass_or_token@github.com/organisation/repository.git`
    :param target_path: Path to the repository. Must already exist and be empty.
    :param ref: Branch/commit hash to checkout
    """
    subprocess.check_call(["git", "init"], cwd=target_path)
    subprocess.check_call(["git", "remote", "add", "origin", url], cwd=target_path)
    subprocess.check_call(["git", "fetch", "origin", ref], cwd=target_path)
    subprocess.check_call(["git", "checkout", ref], cwd=target_path)
    logging.info(subprocess.check_output(["git", "log", "--oneline", "-5"], cwd=target_path))


def make_dbt_profile(conn_params: Dict[str, Any], schema: str, threads: int = 32) -> Dict[str, Any]:
    """Build a dbt profile dict (to go into profiles.yml) to output data into our staging schema.

    Note that this doesn't change the project's sources to read data from a schema."""
    return {
        "config": {
            "partial_parse": True,
            "printer_width": 120,
            "send_anonymous_usage_stats": False,
            "use_colors": True,
        },
        "splitgraph": {
            "outputs": {
                "splitgraph": {
                    "type": "postgres",
                    "dbname": conn_params["SG_ENGINE_DB_NAME"],
                    "host": conn_params["SG_ENGINE_HOST"],
                    "port": int(conn_params["SG_ENGINE_PORT"]),
                    "user": conn_params["SG_ENGINE_USER"],
                    "pass": conn_params["SG_ENGINE_PWD"],
                    "schema": schema,
                    "threads": threads,
                }
            },
            "target": "splitgraph",
        },
    }


def patch_dbt_project_sources(
    project_path: str,
    default_schema: str,
    source_schema_map: Optional[Dict[str, str]] = None,
) -> None:
    """
    Patch a dbt project's source definitions to point them to a different schema.

    :param project_path: Path to the dbt project
    :param source_schema_map: Map from dbt source names to the schema name to override
    :param default_schema: Default schema to use
    """

    source_schema_map = source_schema_map or {}

    # This is kind of crude but it doesn't look like dbt lets us override this on the CLI.
    # Basically, we want to make sure the project loads data just from the staging schema
    # (where we've put our LQ mounted foreign tables) as opposed to any schema on the engine.
    #
    # We do this by going through all YAML files in the project and seeing if they have the
    # sources: [...] dbt source spec, then patching the schema in there.
    for dirpath, _, filenames in os.walk(project_path):
        for filename in filenames:
            if not filename.endswith((".yml", ".yaml")):
                continue

            filepath = os.path.join(dirpath, filename)
            logging.debug("Checking %s", filepath)
            with open(filepath, "r") as f:
                try:
                    data = safe_load(f)
                except YAMLError as e:
                    logging.warning("Error loading %s, ignoring", filepath, exc_info=e)
                if not data or "sources" not in data:
                    continue

                for source in data["sources"]:
                    target = source_schema_map.get(source["name"], default_schema)

                    logging.info(
                        "Patching source %s in %s to point to schema %s",
                        source.get("name"),
                        filepath,
                        target,
                    )
                    source["schema"] = target

            with open(filepath, "w") as f:
                safe_dump(data, f)


def run_dbt_transformation_from_git(
    engine: "PsycopgEngine",
    target_schema: str,
    repository_url: str,
    repository_ref: str = "master",
    dbt_image: str = "airbyte/normalization:0.1.36",
    source_schema_map: Optional[Dict[str, str]] = None,
    default_source_schema: Optional[str] = None,
    models: Optional[List[str]] = None,
) -> None:
    """
    Run a dbt transformation from Git on a dataset checked out into a schema
    :param engine: Engine the data is on
    :param target_schema: The schema to write the dbt-built data out into.
    :param repository_url: URL of the Git repository with the dbt project (including the
        password/access token if required
    :param repository_ref: Branch or commit hash to check out.
    :param dbt_image: Docker image to use for dbt.
    :param source_schema_map: Map of dbt source names to schema names.
    :param default_source_schema: Default source schema for sources not in the map. If not specified,
        `target_schema` is used
    :param models: List of dbt models to limit the build to.
    """

    # Clone the repo into a temporary location and patch it to point to our staging schema
    client = get_docker_client()
    network_mode = detect_network_mode()
    with TemporaryDirectory() as tmp_dir:
        # Prepare the target directory that we'll copy into the dbt container.
        # We can't bind mount it since we ourselves could be running inside of Docker.
        project_path = os.path.join(tmp_dir, "dbt_project")
        os.mkdir(project_path)

        # Add a Git repo and switch all of its sources to use our staging schema
        prepare_git_repo(repository_url, project_path, repository_ref)
        patch_dbt_project_sources(
            project_path,
            source_schema_map=source_schema_map,
            default_schema=default_source_schema or target_schema,
        )

        # Make a dbt profile file that points to our engine
        profile = make_dbt_profile(engine.conn_params, target_schema)
        with open(os.path.join(tmp_dir, "profiles.yml"), "w") as f:
            safe_dump(profile, f)

        # Create the normalization container
        # We'll use Airbyte's image for now (since they have an image with dbt installed and our
        # Airbyte load is currently the only user of this function), but at this point
        # there isn't much that makes us require it.
        entrypoint = ["/bin/bash"]
        command = [
            "-c",
            "dbt run --profiles-dir /data --project-dir /data/dbt_project --profile splitgraph",
        ]
        if models is not None:
            command[-1] += " --models " + shlex.join(["+" + m for m in models])
        client.images.pull(dbt_image)
        container = client.containers.create(
            image=dbt_image,
            name="sg-dbt-{:08x}".format(getrandbits(64)),
            entrypoint=entrypoint,
            command=command,
            network_mode=network_mode,
            log_config=LogConfig(type="json-file", config={"max-size": "10m", "max-file": "3"}),
        )

        with remove_at_end(container):
            copy_dir_to_container(container, tmp_dir, "/data", exclude_names=[".git"])
            container.start()
            wait_not_failed(container, mirror_logs=True)


def compile_dbt_manifest(
    engine: "PsycopgEngine",
    repository_url: str,
    repository_ref: str = "master",
    dbt_image: str = "airbyte/normalization:0.1.36",
) -> Dict[str, Any]:

    # Clone the repo into a temporary location and patch it to point to our staging schema
    client = get_docker_client()
    network_mode = detect_network_mode()
    with TemporaryDirectory() as tmp_dir:
        # Prepare the target directory that we'll copy into the dbt container.
        # We can't bind mount it since we ourselves could be running inside of Docker.
        project_path = os.path.join(tmp_dir, "dbt_project")
        os.mkdir(project_path)

        # Add a Git repo
        prepare_git_repo(repository_url, project_path, repository_ref)

        # Make a dbt profile file that points to our engine
        profile = make_dbt_profile(engine.conn_params, "splitgraph")
        with open(os.path.join(tmp_dir, "profiles.yml"), "w") as f:
            safe_dump(profile, f)

        # Get the location of the target that the build outputs the manifest to
        with open(os.path.join(project_path, "dbt_project.yml"), "r") as f:
            project = safe_load(f)
            build_dir = project.get("target-path", "target")

        entrypoint = ["/bin/bash"]
        command = [
            "-c",
            "dbt compile --profiles-dir /data --project-dir /data/dbt_project --profile splitgraph",
        ]
        client.images.pull(dbt_image)
        container = client.containers.create(
            image=dbt_image,
            name="sg-dbt-{:08x}".format(getrandbits(64)),
            entrypoint=entrypoint,
            command=command,
            network_mode=network_mode,
            log_config=LogConfig(type="json-file", config={"max-size": "10m", "max-file": "3"}),
        )

        with remove_at_end(container):
            copy_dir_to_container(container, tmp_dir, "/data", exclude_names=[".git"])
            container.start()
            wait_not_failed(container, mirror_logs=True)
            # Extract the manifest
            return cast(
                Dict[str, Any],
                json.load(
                    get_file_from_container(
                        container, os.path.join("/data/dbt_project/", build_dir, "manifest.json")
                    )
                ),
            )
