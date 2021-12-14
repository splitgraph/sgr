"""
Utilities to generate a sample dbt project that works on Splitgraph-loaded data
"""
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import ruamel.yaml
from splitgraph.cloud.project.templates import (
    DBT_PROJECT_TEMPLATE,
    SOURCE_TEMPLATE_NOCOM,
    SOURCES_YML_TEMPLATE,
)
from splitgraph.cloud.project.utils import get_source_name
from splitgraph.core.repository import Repository
from splitgraph.utils.yaml import safe_load


def generate_dbt_project(repositories: List[str], basedir: Path) -> None:
    """
    Generate a sample dbt project in a directory
    :param repositories: List of repository names used by this dbt project
    :param basedir: Directory to put the project in
    """

    # Generate dbt_project.yml
    with open(os.path.join(basedir, "dbt_project.yml"), "w") as f:
        f.write(DBT_PROJECT_TEMPLATE)

    # Generate models/staging/sources.yml
    yml = ruamel.yaml.YAML()
    sources_yml = yml.load(SOURCES_YML_TEMPLATE)
    sources_yml["sources"][0]["name"] = get_source_name(repositories[0])
    sources_yml["sources"][0]["schema"] = repositories[0]

    # Add the remaining sources without comments
    for repository in repositories[1:]:
        sources_yml_nocom = safe_load(SOURCE_TEMPLATE_NOCOM)
        sources_yml_nocom["name"] = get_source_name(repository)
        sources_yml_nocom["schema"] = repository
        sources_yml["sources"].append(sources_yml_nocom)

    os.makedirs(os.path.join(basedir, "models/staging"), exist_ok=True)
    with open(os.path.join(basedir, "models/staging/sources.yml"), "w") as f:
        yml.dump(sources_yml, f)

    # Generate models/staging/[source_name]/[source_name.sql]
    for repository in repositories:
        source_name = get_source_name(repository)
        model_path = os.path.join(basedir, "models/staging", source_name)
        os.makedirs(model_path, exist_ok=True)
        with open(os.path.join(model_path, source_name + ".sql"), "w") as f:
            f.write(  # nosec
                f"""SELECT 
  *
FROM {{{{ source('{source_name.replace("'", "''")}', 'some_table') }}}}
"""
            )


def _make_source(repository: str) -> Dict[str, str]:
    repo_obj = Repository.from_schema(repository)
    return {
        "dbt_source_name": get_source_name(repository),
        "namespace": repo_obj.namespace,
        "repository": repo_obj.repository,
        "hash_or_tag": "latest",
    }


def generate_dbt_plugin_params(repositories: List[str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Generate the required configuration parameters for the Splitgraph dbt plugin"""

    # At runtime (when we execute the GitHub action), if we're checking the dbt project
    # into the same repository where splitgraph.yml lives (they can coexist), we need a way
    # to send the repo over to Splitgraph Cloud. We put a placeholder here instead and construct
    # the Git pull URL at action runtime (using GITHUB_TOKEN).
    credentials = {"git_url": "$THIS_REPO_URL"}

    params = {"sources": [_make_source(r) for r in repositories]}

    return params, credentials
