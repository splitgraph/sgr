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
        # Schema of the table, a list of objects with `name` and `type`. If set to `[]`, will infer. 
        schema: []
    # Whether live querying is enabled for the plugin (creates a "live" tag in the
    # repository proxying to the data source). The plugin must support live querying.
    is_live: false
    # Ingestion schedule settings. Disable this if you're using GitHub Actions or other methods
    # to trigger ingestion.
    schedule:
"""

DBT_PROJECT_TEMPLATE = """# Sample dbt project referencing data from all ingested/added Splitgraph datasets.
# This is not ready to run, as you'll need to:
#
#   * Manually define tables in your sources (see models/staging/sources.yml, "tables" sections)
#   * Reference the sources using the source(...) macros (see 
#     models/staging/(source_name)/source_name.sql for an example)
#   * Write the actual models

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

README_TEMPLATE = """# Sample Splitgraph Cloud project

Welcome to the sample Splitgraph Cloud project that we generated for your chosen data sources.

This project contains:

  * [`splitgraph.yml`](./splitgraph.yml): defines live and ingested data sources as well as other
    metadata for your data catalog.
  * [`splitgraph.credentials.yml`](./splitgraph.credentials.yml): defines credentials to your 
    data sources
  * [`build.moveme.yml`](./build.moveme.yml): GitHub Action that runs ingestion / metadata upload
    for all sources:
    * Adds data sources supporting "live" querying (PostgreSQL, MySQL, Elasticsearch, CSV-in-S3) to
      Splitgraph without ingestion, letting you query them at source
    * Runs a "sync" action for other data sources (SaaS etc) to load their data to Splitgraph  
    * Optionally, also runs a dbt project at the end of ingestion to build models.
    
All built repositories are going to be private to your account. You can manage access settings in
the UI by going to https://splitgraph.com/namespace/repository. 

## Required setup

Before you can run this project from GitHub Action, you need to perform a few extra setup steps.

### Add credentials to `splitgraph.credentials.yml`

Edit [`splitgraph.credentials.yml`](./splitgraph.credentials.yml) to add required credentials to
your data sources. **DO NOT COMMIT IT!** You'll add the contents of this file as a secret in the
next step.

### Set up GitHub secrets

Go to the [Secrets page](https://github.com/$GITHUB_REPO/settings/secrets/actions/new) for this
repository and create the following secrets:
  
  * `SPLITGRAPH_CREDENTIALS_YML`: contents of the `splitgraph.credentials.yml` with the data source
    credentals that you've edited in the previous step. 
  * `SPLITGRAPH_API_KEY` / `SPLITGRAPH_API_SECRET`: API keys to Splitgraph Cloud (also known as
    "SQL credentials"). You can get them at https://www.splitgraph.com/settings/sql-credentials (or
    your deployment URL if you're on a private deployment).

### Set up GitHub Actions

Because this repository was itself generated by a GitHub Actions job, we can't edit the workflow
files for this repository from within the action. You will need to move the job definition file
([`build.moveme.yml`](./build.moveme.yml)) to `.github/workflows/build.yml`.

Optionally, also delete the `seed.yml` file that was used to generate this project.

### Set up dbt and write the models

If you added dbt to this project, this repository also contains a sample dbt project that references
data from all the datasets you've added to it. See [`dbt_project.yml`](./dbt_project.yml) and the
[`models/staging/sources.yml`](models/staging/sources.yml) file for more information.

Currently, we can't infer the columns and the tables that your data sources will produce at this
project generation time, so this dbt project is here as a rough starting point. To get it working,
you will need to: 
 
* Manually define tables in your sources (see 
  [`models/staging/sources.yml`](models/staging/sources.yml), "tables" sections). You might want
  to run the ingestion GitHub Action once first without the dbt step in order to create the
  repositories on Splitgraph and see their tables and columns.
* Write the actual models that reference the sources using the `source(...)` macros (see 
  `models/staging/(source_name)/source_name.sql` for an example)

## Run the action

By default, the generated action waits for a manual trigger to run. You can trigger the action by
going to https://github.com/$GITHUB_REPO/actions/workflows/build.yml and clicking "Run workflow". 

## Next steps
 
  * Edit the GitHub Action to, for example, add a run schedule
  * Browse the ingested and built datasets at https://splitgraph.com/namespace/repository
  * Connect to Splitgraph with an SQL client (see [the docs](https://www.splitgraph.com/docs/splitgraph-cloud/data-delivery-network)) 
"""
