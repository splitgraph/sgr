import logging
import os
import subprocess
from typing import Dict, Any

import yaml
from yaml import YAMLError


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
                    "port": conn_params["SG_ENGINE_PORT"],
                    "user": conn_params["SG_ENGINE_USER"],
                    "pass": conn_params["SG_ENGINE_PWD"],
                    "schema": schema,
                    "threads": threads,
                }
            },
            "target": "splitgraph",
        },
    }


def patch_dbt_project_sources(project_path: str, schema: str) -> None:
    """
    Patch a dbt project's source definitions to point them to a single schema.

    :param project_path: Path to the dbt project
    :param schema: New schema
    """

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
                    data = yaml.safe_load(f)
                except YAMLError as e:
                    logging.warning("Error loading %s, ignoring", filepath, exc_info=e)
                if not data or "sources" not in data:
                    continue

                for source in data["sources"]:
                    logging.info(
                        "Patching source %s in %s to point to schema %s",
                        source.get("name"),
                        filepath,
                        schema,
                    )
                    source["schema"] = schema

            with open(filepath, "w") as f:
                yaml.safe_dump(data, f, sort_keys=False)
