"""
Utilities to generate a sample dbt project that works on Splitgraph-loaded data
"""
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import ruamel.yaml
from splitgraph.cloud.project.utils import get_source_name
from splitgraph.core.repository import Repository
from splitgraph.utils.yaml import safe_load

DBT_PROJECT_TEMPLATE = """# Sample dbt project referencing data from all ingested/added Splitgraph datasets.
# This is not exactly ready to run, as you'll need to:
#
#   * Manually define tables in your sources (see models/staging/sources.yml, "tables" sections)
#   * Reference the sources using the source(...) macros (see 
#     models/staging/(source_name)/source_name.sql for an example)
#   * Write the actual models
# 
name: 'splitgraph_template'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
# Note that the Splitgraph runner overrides this at runtime, so this is only useful
# if you are running a local Splitgraph engine and are developing this dbt model against it.
profile: 'splitgraph_template'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"


# Configuring models
# Full documentation: https://docs.getdbt.com/docs/configuring-models

models:
  splitgraph_template:
    # Staging data (materialized as CTEs) that references the source Splitgraph repositories.
    # Here as a starting point. You can reference these models downstream in models that actually
    # materialize as tables.
    staging:
      +materialized: cte
"""

SOURCES_YML_TEMPLATE = """# This file defines all data sources referenced by this model. The mapping
# between the data source name and the Splitgraph repository is in the settings of the dbt plugin
# in splitgraph.yml (see params -> sources)
version: 2
sources:
- name: SOURCE_NAME
  # Splitgraph will use a different temporary schema for this source by patching this project
  # at runtime, so this is for informational purposes only. 
  schema: SCHEMA_NAME
  # We can't currently infer the tables produced by a data source at project generation time,
  # so for now you'll need to define the tables manually.
  tables:
  - name: some_table
"""

SOURCE_TEMPLATE_NOCOM = """name: SOURCE_NAME
schema: SCHEMA_NAME
tables:
- name: some_table
"""


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
        model_path = os.path.join(basedir, "models/staging", get_source_name(repository))
        os.makedirs(model_path, exist_ok=True)
        with open(os.path.join(model_path, get_source_name(repository) + ".sql"), "w") as f:
            f.write(  # nosec
                f"""SELECT 
  *
FROM {{{{ source('{get_source_name(repository).replace("'", "''")}', 'some_table') }}}}
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
