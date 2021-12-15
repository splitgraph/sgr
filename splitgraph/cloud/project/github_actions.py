"""
Generate a Github Actions workflow file that runs sgr cloud sync on select repositories, respecting
inter-job dependencies.
"""

from typing import Any, Dict, List, Tuple

from splitgraph.cloud.project.utils import get_source_name


def generate_job(
    repository: str, is_live: bool, is_dbt: bool, deploy_url: str = "splitgraph.com"
) -> Tuple[str, Dict[str, Any]]:
    job_id = get_source_name(repository)

    steps = [
        {"uses": "actions/checkout@v2"},
        {
            "name": "Set up Splitgraph",
            "uses": "splitgraph/setup-splitgraph@master",
            "with": {
                "splitgraph_deployment_url": deploy_url,
                "splitgraph_api_key": "${{ secrets.SPLITGRAPH_API_KEY }}",
                "splitgraph_api_secret": "${{ secrets.SPLITGRAPH_API_SECRET }}",
            },
        },
    ]
    if is_dbt:
        steps.append(
            {
                "name": "Set up dbt Git URL",
                "run": 'echo "$CREDENTIALS_YML" > splitgraph.credentials.yml'
                + '\nsed -i "s|\\$THIS_REPO_URL|https://$GITHUB_ACTOR:$GITHUB_TOKEN@github.com/$GITHUB_REPOSITORY|g" splitgraph.credentials.yml',
                "shell": "bash",
                "env": {
                    "CREDENTIALS_YML": "${{secrets.SPLITGRAPH_CREDENTIALS_YML}}",
                    "GITHUB_TOKEN": "${{secrets.GITHUB_TOKEN}}",
                },
            }
        )
    else:
        steps.append(
            {
                "name": "Set up data source credentials",
                "run": 'echo "$CREDENTIALS_YML" > splitgraph.credentials.yml',
                "shell": "bash",
                "env": {"CREDENTIALS_YML": "${{secrets.SPLITGRAPH_CREDENTIALS_YML}}"},
            }
        )

    if is_live:
        # For repositories that support mount, we use `sgr cloud load` to set them up
        # as live data sources and upload metadata.
        steps.append(
            {
                "name": "Run sgr cloud load to set up metadata and data source settings",
                "run": "sgr cloud load --remote splitgraph "
                f"-f splitgraph.yml -f splitgraph.credentials.yml {repository}",
                "shell": "bash",
            }
        )
    else:
        # For repositories that don't support mount, first run `sgr cloud sync` to load the data
        # and make the repository private by default. Then, run `sgr cloud load` to set up the
        # repo's README etc (without saving the ingestion settings).
        steps.append(
            {
                "name": "Run sgr cloud sync to perform the data load into a private repo",
                "run": "sgr cloud sync --remote splitgraph --initial-private --use-file "
                f"-f splitgraph.yml -f splitgraph.credentials.yml --wait {repository}",
                "shell": "bash",
            }
        )
        steps.append(
            {
                "name": "Run sgr cloud load to set up metadata",
                "run": "sgr cloud load --remote splitgraph --skip-external "
                f"-f splitgraph.yml -f splitgraph.credentials.yml {repository}",
                "shell": "bash",
            }
        )

    job_doc = {"name": f"Build {repository}", "runs-on": "ubuntu-20.04", "steps": steps}

    return job_id, job_doc


def generate_workflow(
    repositories: List[Tuple[str, bool, bool]], dependencies: Dict[str, List[str]]
) -> Dict[str, Any]:
    job_ids = {}
    job_docs = {}

    for repository, is_live, is_dbt in repositories:
        job_id, job_doc = generate_job(repository, is_live, is_dbt)
        job_ids[repository] = job_id
        job_docs[repository] = job_doc

    for repository, _, _ in repositories:
        repo_deps = dependencies.get(repository)
        if not repo_deps:
            continue

        job_docs[repository]["needs"] = [job_ids[d] for d in repo_deps]

    workflow_doc = {
        "name": "Build datasets on Splitgraph",
        "on": "workflow_dispatch",
        "jobs": {job_ids[r]: job_docs[r] for r, _, _ in repositories},
    }
    return workflow_doc
