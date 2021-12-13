"""
Generate a Github Actions workflow file that runs sgr cloud sync on select repositories, respecting
inter-job dependencies.
"""

from typing import Any, Dict, List, Tuple

from splitgraph.cloud.project.utils import get_source_name


def generate_job(
    repository: str, deploy_url: str = "data.splitgraph.com"
) -> Tuple[str, Dict[str, Any]]:
    job_id = get_source_name(repository)
    job_doc = {
        "name": f"Build {repository}",
        "runs-on": "ubuntu-20.04",
        "steps": [
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
            {
                "name": "Set up credentials for Splitgraph",
                "run": 'echo "$CREDENTIALS_YML" > splitgraph.credentials.yml',
                "shell": "bash",
                "env": {"CREDENTIALS_YML": "${{secrets.SPLITGRAPH_CREDENTIALS_YML}}"},
            },
            {
                "name": "Run sgr cloud sync",
                "run": f"sgr cloud sync --remote splitgraph {repository} --wait "
                f"--use-file -f splitgraph.yml -f splitgraph.credentials.yml",
            },
        ],
    }

    return job_id, job_doc


def generate_workflow(
    repositories: List[str], dependencies: Dict[str, List[str]]
) -> Dict[str, Any]:
    job_ids = {}
    job_docs = {}

    for repository in repositories:
        job_id, job_doc = generate_job(repository)
        job_ids[repository] = job_id
        job_docs[repository] = job_doc

    for repository in repositories:
        repo_deps = dependencies.get(repository)
        if not repo_deps:
            continue

        job_docs[repository]["needs"] = [job_ids[d] for d in repo_deps]

    workflow_doc = {
        "name": "Build datasets on Splitgraph",
        "on": "workflow_dispatch",
        "jobs": {job_ids[r]: job_docs[r] for r in repositories},
    }
    return workflow_doc
