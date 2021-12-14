SPLITGRAPH_YML_TEMPLATE = """credentials:
  CREDENTIAL_NAME:  # This is the name of this credential that "external" sections can reference.
    plugin: PLUGIN_NAME
    # Credential-specific data matching the plugin's credential schema
    data: {}
repositories:
- namespace: NAMESPACE
  repository: REPOSITORY
  # Catalog-specific metadata for the repository. Optional.
  metadata:
    readme:
      text: Readme
    description: Description of the repository
    topics:
    - sample_topic
  # Data source settings for the repository. Optional.
  external:
    # Name of the credential that the plugin uses. This can also be a credential_id if the
    # credential is already registered on Splitgraph.
    credential: CREDENTIAL_NAME
    plugin: PLUGIN_NAME
    # Plugin-specific parameters matching the plugin's parameters schema
    params: {}
    tables:
      sample_table:
        # Plugin-specific table parameters matching the plugin's schema
        options: {}

        # Schema of the table. If set to `[]`, will infer.
        schema:
          - name: col_1
            type: varchar
    # Whether live querying is enabled for the plugin (creates a "live" tag in the
    # repository proxying to the data source). The plugin must support live querying.
    is_live: false
    # Ingestion schedule settings. Disable this if you're using GitHub Actions or other methods
    # to trigger ingestion.
    schedule:
"""

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
