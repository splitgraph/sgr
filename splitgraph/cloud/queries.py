BULK_UPSERT_REPO_PROFILES = """mutation BulkUpsertRepoProfilesMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $descriptions: [String]
  $readmes: [String]
  $licenses: [String]
  $metadata: [JSON]
) {
  __typename
  bulkUpsertRepoProfiles(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      descriptions: $descriptions
      readmes: $readmes
      licenses: $licenses
      metadata: $metadata
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

BULK_UPDATE_REPO_SOURCES = """mutation BulkUpdateRepoSourcesMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $sources: [DatasetSourceInput]
) {
  __typename
  bulkUpdateRepoSources(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      sources: $sources
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

BULK_UPSERT_REPO_TOPICS = """mutation BulkUpsertRepoTopicsMutation(
  $namespaces: [String!]
  $repositories: [String!]
  $topics: [String]
) {
  __typename
  bulkUpsertRepoTopics(
  input: {
      namespaces: $namespaces
      repositories: $repositories
      topics: $topics
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

PROFILE_UPSERT = """mutation UpsertRepoProfile(
  $namespace: String!
  $repository: String!
  $description: String
  $readme: String
  $topics: [String]
  $sources: [DatasetSourceInput]
  $license: String
  $metadata: JSON
) {
  __typename
  upsertRepoProfileByNamespaceAndRepository(
    input: {
      repoProfile: {
        namespace: $namespace
        repository: $repository
        description: $description
        readme: $readme
        sources: $sources
        license: $license
        metadata: $metadata
      }
      patch: {
        namespace: $namespace
        repository: $repository
        description: $description
        readme: $readme
        sources: $sources
        license: $license
        metadata: $metadata
      }
    }
  ) {
    clientMutationId
    __typename
  }
  __typename
  createRepoTopicsAgg(
  input: {
    repoTopicsAgg: {
      namespace: $namespace
      repository: $repository
      topics: $topics
    }
  }
  ) {
    clientMutationId
    __typename
  }
}
"""

FIND_REPO = """query FindRepositories($query: String!, $limit: Int!) {
  findRepository(query: $query, first: $limit) {
    edges {
      node {
        namespace
        repository
        highlight
      }
    }
    totalCount
  }
}"""

REPO_PARAMS = "($namespace: String!, $repository: String!)"
REPO_CONDITIONS = ", condition: {namespace: $namespace, repository: $repository}"
GET_REPO_METADATA = """query GetRepositoryMetadata%s {
  repositories(first: 1000%s) {
    nodes {
      namespace
      repository
      repoTopicsByNamespaceAndRepository {
        nodes {
          topic
        }
      }
      repoProfileByNamespaceAndRepository {
        description
        license
        metadata
        readme
        sources {
          anchor
          href
          isCreator
          isSameAs
        }
      }
    }
  }
}"""

GET_REPO_SOURCE = """query GetRepositoryDataSource%s {
  repositoryDataSources(first: 1000%s) {
    nodes {
      namespace
      repository
      credentialId
      dataSource
      params
      tableParams
      externalImageByNamespaceAndRepository {
        imageByNamespaceAndRepositoryAndImageHash {
          tablesByNamespaceAndRepositoryAndImageHash {
            nodes {
              tableName
              tableSchema
            }
          }
        }
      }
      ingestionScheduleByNamespaceAndRepository {
        schema
        schedule
        enabled
      }
    }
  }
}"""

INGESTION_JOB_STATUS = """
query RepositoryIngestionJobStatus($namespace: String, $repository: String) {
  repositoryIngestionJobStatus(
    first: 1
    namespace: $namespace
    repository: $repository
  ) {
    nodes {
      taskId
      started
      finished
      isManual
      status
    }
  }
}
"""

JOB_LOGS = """
query JobLogs($namespace: String!, $repository: String!, $taskId: String!) {
  jobLogs(namespace: $namespace, repository: $repository, taskId: $taskId) {
    url
  }
}
"""

CSV_URL = """query CSVURLs {
  csvUploadDownloadUrls {
    upload
    download
  }
}
"""

START_LOAD = """mutation StartExternalRepositoryLoad(
  $namespace: String!
  $repository: String!
  $pluginName: String
  $params: JSON
  $tableParams: [ExternalTableInput!]
  $credentialData: JSON
  $credentialId: String
  $sync: Boolean! = true
  $initialVisibility: RepositoryVisibility! = PUBLIC
) {
  __typename
  startExternalRepositoryLoad(
    namespace: $namespace
    repository: $repository
    pluginName: $pluginName
    params: $params
    tables: $tableParams
    credentialData: $credentialData
    credentialId: $credentialId
    sync: $sync
    initialPermissions: { visibility: $initialVisibility }
  ) {
    taskId
  }
}
"""

GET_PLUGINS = """query ExternalPlugins {
  externalPlugins {
    pluginName
    name
    description
    paramsSchema
    credentialsSchema
    tableParamsSchema
    supportsSync
    supportsMount
    supportsLoad
  }
}
"""

GET_PLUGIN = """query ExternalPlugin($pluginName: String!) {
  externalPlugin(name: $pluginName) {
    pluginName
    name
    description
    paramsSchema
    credentialsSchema
    tableParamsSchema
    supportsSync
    supportsMount
    supportsLoad
  }
}"""

START_EXPORT = """mutation StartExport($query: String!) {
  exportQuery(query: $query, exportFormat: "csv") {
    id
  }
}
"""

EXPORT_JOB_STATUS = """query ExportJobStatus($taskId: UUID!) {
  exportJobStatus(taskId: $taskId) {
    taskId
    started
    finished
    status
    userId
    exportFormat
    output
  }
}
"""
