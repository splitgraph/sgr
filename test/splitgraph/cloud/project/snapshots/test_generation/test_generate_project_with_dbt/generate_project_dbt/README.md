# Sample Splitgraph Cloud project

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

Go to the [Secrets page](https://github.com/my/repo/settings/secrets/actions/new) for this
repository and create the following secrets:
  
  * `SPLITGRAPH_CREDENTIALS_YML`: contents of the `splitgraph.credentials.yml` with the data source
    credentals that you've edited in the previous step. 
  * `SPLITGRAPH_API_KEY` / `SPLITGRAPH_API_SECRET`: API keys to Splitgraph Cloud (also known as
    "SQL credentials"). You can get them at https://www.splitgraph.com/settings/sql-credentials (or
    your deployment URL if you're on a private deployment).

### Edit `splitgraph.yml`

We generated a [`splitgraph.yml`](./splitgraph.yml) file from your chosen plugins'
parameters JSONSchema. You should review it and add suitable plugin settings:

  - set `tables` to `tables: {}` to let the plugin automatically infer the schema and the
    options of the data source (by default, it adds a sample table into the project file)
  - change and customize the `metadata` block
  - set up the plugin parameters in `external.params`. Where the comment says `CHOOSE ONE`
    and offers a list of alternative subobjects, choose one entry from the list and delete
    the list itself, leaving the object at the top level.

Example:

```yaml
- namespace: my_namespace
  repository: csv
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
    credential: csv
    plugin: csv
    # Plugin-specific parameters matching the plugin's parameters schema
    params:
      connection:  # Choose one of:
      - connection_type: http  # REQUIRED. Constant
        url: '' # REQUIRED. HTTP URL to the CSV file
      - connection_type: s3  # REQUIRED. Constant
        s3_endpoint: '' # REQUIRED. S3 endpoint (including port if required)
        s3_bucket: '' # REQUIRED. Bucket the object is in
        s3_region: '' # Region of the S3 bucket
        s3_secure: false # Whether to use HTTPS for S3 access
        s3_object: '' # Limit the import to a single object
        s3_object_prefix: '' # Prefix for object in S3 bucket
      autodetect_header: true # Detect whether the CSV file has a header automatically
      autodetect_dialect: true # Detect the CSV file's dialect (separator, quoting characters etc) automatically
      autodetect_encoding: true # Detect the CSV file's encoding automatically
      autodetect_sample_size: 65536 # Sample size, in bytes, for encoding/dialect/header detection
      schema_inference_rows: 100000 # Number of rows to use for schema inference
      encoding: utf-8 # Encoding of the CSV file
      ignore_decode_errors: false # Ignore errors when decoding the file
      header: true # First line of the CSV file is its header
      delimiter: ',' # Character used to separate fields in the file
      quotechar: '"' # Character used to quote fields
    tables:
      sample_table:
        # Plugin-specific table parameters matching the plugin's schema
        options:
          url: ''  # HTTP URL to the CSV file
          s3_object: '' # S3 object of the CSV file
          autodetect_header: true # Detect whether the CSV file has a header automatically
          autodetect_dialect: true # Detect the CSV file's dialect (separator, quoting characters etc) automatically
          autodetect_encoding: true # Detect the CSV file's encoding automatically
          autodetect_sample_size: 65536 # Sample size, in bytes, for encoding/dialect/header detection
          schema_inference_rows: 100000 # Number of rows to use for schema inference
          encoding: utf-8 # Encoding of the CSV file
          ignore_decode_errors: false # Ignore errors when decoding the file
          header: true # First line of the CSV file is its header
          delimiter: ',' # Character used to separate fields in the file
          quotechar: '"' # Character used to quote fields
        # Schema of the table, a list of objects with `name` and `type`. If set to `[]`, will infer. 
        schema: []
    # Whether live querying is enabled for the plugin (creates a "live" tag in the
    # repository proxying to the data source). The plugin must support live querying.
    is_live: true
    # Ingestion schedule settings. Disable this if you're using GitHub Actions or other methods
    # to trigger ingestion.
    schedule:
```  

becomes:

```yaml
- namespace: my_namespace
  repository: csv
  metadata:
    readme:
      text: Readme
    description: Description of the repository
    topics:
    - sample_topic
  external:
    # No credential required since we're querying a CSV file over HTTP
    plugin: csv
    # Plugin-specific parameters matching the plugin's parameters schema
    params:
      connection:
        connection_type: http  # REQUIRED. Constant
        url: 'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv' # REQUIRED. HTTP URL to the CSV file
      autodetect_header: true # Detect whether the CSV file has a header automatically
      autodetect_dialect: true # Detect the CSV file's dialect (separator, quoting characters etc) automatically
      autodetect_encoding: true # Detect the CSV file's encoding automatically
      autodetect_sample_size: 65536 # Sample size, in bytes, for encoding/dialect/header detection
      schema_inference_rows: 100000 # Number of rows to use for schema inference
      encoding: utf-8 # Encoding of the CSV file
      ignore_decode_errors: false # Ignore errors when decoding the file
      header: true # First line of the CSV file is its header
      delimiter: ',' # Character used to separate fields in the file
      quotechar: '"' # Character used to quote fields
    # Automatically infer table parameters
    tables: {}
    is_live: true
```

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
going to https://github.com/my/repo/actions/workflows/build.yml and clicking "Run workflow". 

## Next steps
 
  * Edit the GitHub Action to, for example, add a run schedule
  * Browse the ingested and built datasets at https://splitgraph.com/namespace/repository
  * Connect to Splitgraph with an SQL client (see [the docs](https://www.splitgraph.com/docs/splitgraph-cloud/data-delivery-network)) 
