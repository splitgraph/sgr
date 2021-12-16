import base64
import itertools
import os
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import ruamel.yaml
from pydantic import BaseModel
from ruamel.yaml import CommentedMap as CM
from ruamel.yaml import CommentedSeq as CS
from splitgraph.cloud import GQLAPIClient, Plugin
from splitgraph.cloud.project.dbt import (
    generate_dbt_plugin_params,
    generate_dbt_project,
)
from splitgraph.cloud.project.github_actions import generate_workflow
from splitgraph.cloud.project.templates import README_TEMPLATE, SPLITGRAPH_YML_TEMPLATE


def get_comment(jsonschema_object: Any) -> str:
    """
    Get a YAML comment to be attached to a JSONSchema property.
    """
    result = []
    if "title" in jsonschema_object:
        result.append(jsonschema_object["title"])
    if "description" in jsonschema_object:
        result.append(jsonschema_object["description"].strip().replace("\n", " "))
    if "enum" in jsonschema_object:
        enum = jsonschema_object["enum"]
        if len(enum) == 1:
            result.append("Constant")
        else:
            result.append("One of " + ", ".join(enum))
    if "const" in jsonschema_object and "Constant" not in result:
        result.append("Constant")
    if "oneOf" in jsonschema_object:
        result.append("Choose one of:")

    return ". ".join(result).strip()


def jsonschema_object_to_example(obj: Any, type_override=None) -> Any:
    """
    Get an example value for a JSONSchema property
    """
    obj_type = type_override or obj["type"]
    if obj_type in ("string", "integer", "boolean"):
        if "examples" in obj:
            return obj["examples"][0]
        elif "default" in obj:
            return obj["default"]
        elif "enum" in obj:
            return obj["enum"][0]
        elif "const" in obj:
            return obj["const"]
        else:
            return {"string": "", "integer": 0, "boolean": False}.get(obj_type)
    elif obj_type == "array":
        example = jsonschema_object_to_example(obj["items"])
        seq = CS([example])
        comment = get_comment(obj["items"])
        if comment:
            seq.yaml_set_start_comment(comment)
        return [example]
    elif obj_type == "object":
        if "properties" in obj:
            return _get_object_example(obj)
        elif "oneOf" in obj:
            return _get_oneof_example(obj)
        return {}


def _get_oneof_example(obj: Dict[str, Any]) -> CS:
    result = CS()
    for o in obj["oneOf"]:
        result.append(jsonschema_object_to_example(o, "object"))
        comment = get_comment(o)
        if comment:
            result.yaml_add_eol_comment(comment, key=len(result) - 1)
    return result


def _get_object_example(obj: Dict[str, Any]) -> CM:
    properties = obj["properties"]
    required = obj.get("required", [])
    required_items = []
    not_required_items = []
    result = CM()
    for p in properties:
        example = jsonschema_object_to_example(properties[p])
        comment = get_comment(properties[p])
        if p in required:
            if comment:
                comment = "REQUIRED. " + comment
            else:
                comment = "REQUIRED"
            required_items.append((p, example, comment))
        else:
            not_required_items.append((p, example, comment))
    for item, example, comment in itertools.chain(required_items, not_required_items):
        result[item] = example
        if comment:
            result.yaml_add_eol_comment(comment, key=item)
    return result


def stub_plugin(plugin: Plugin, namespace: str, repository: str, is_live: bool = False) -> CM:
    """
    Generate a splitgraph.yml file based on a plugin's JSONSchemas.
    """
    yml = ruamel.yaml.YAML()
    repositories_yaml = (
        SPLITGRAPH_YML_TEMPLATE.replace("CREDENTIAL_NAME", plugin.plugin_name)
        .replace("NAMESPACE", namespace)
        .replace("REPOSITORY", repository)
        .replace("PLUGIN_NAME", plugin.plugin_name)
    )
    ruamel_dict = yml.load(StringIO(repositories_yaml))
    assert isinstance(ruamel_dict, CM)

    ruamel_dict["credentials"][plugin.plugin_name]["data"] = jsonschema_object_to_example(
        plugin.credentials_schema
    )

    ruamel_dict["repositories"][0]["external"]["params"] = jsonschema_object_to_example(
        plugin.params_schema
    )
    ruamel_dict["repositories"][0]["external"]["tables"]["sample_table"][
        "options"
    ] = jsonschema_object_to_example(plugin.table_params_schema)
    ruamel_dict["repositories"][0]["external"]["is_live"] = is_live

    return ruamel_dict


class ProjectSeed(BaseModel):
    """
    Contains all information required to generate a Splitgraph project + optionally
    a dbt model for GitHub Actions
    """

    namespace: str
    plugins: List[str]
    include_dbt: bool = False

    def encode(self) -> str:
        return base64.b64encode(self.json(separators=(",", ":")).encode()).decode()

    @classmethod
    def decode(cls, encoded: str) -> "ProjectSeed":
        return ProjectSeed.parse_raw(base64.b64decode(encoded.encode()))


def generate_project(
    api_client: GQLAPIClient, seed: ProjectSeed, basedir: Path, github_repo: Optional[str] = None
) -> None:
    all_plugins = {p.plugin_name: p for p in api_client.get_all_plugins()}

    credentials, repositories, repository_info = generate_splitgraph_yml(all_plugins, seed)

    yml = ruamel.yaml.YAML()
    with open(os.path.join(basedir, "splitgraph.credentials.yml"), "w") as f:
        yml.dump(credentials, f)

    with open(os.path.join(basedir, "splitgraph.yml"), "w") as f:
        yml.dump(repositories, f)

    # Generate the dbt project
    if seed.include_dbt:
        dbt_repo, _, is_dbt = repository_info[-1]
        assert is_dbt
        dbt_sources = [r for r, _, is_dbt in repository_info if not is_dbt]
        dependencies = {dbt_repo: dbt_sources}
        generate_dbt_project(dbt_sources, basedir)
    else:
        dependencies = {}

    # Generate the Github workflow file
    github_root = os.path.join(basedir, ".github/workflows")
    os.makedirs(github_root, exist_ok=True)

    with open(os.path.join(github_root, "build.yml"), "w") as f:
        yml.dump(generate_workflow(repository_info, dependencies), f)

    # Generate the README
    with open(os.path.join(basedir, "README.md"), "w") as f:
        template = README_TEMPLATE
        # Add the GitHub repo to some places that might need it, e.g. URLs to the
        # repo settings page
        if github_repo:
            template = template.replace("$GITHUB_REPO", github_repo)
        f.write(template)


def generate_splitgraph_yml(
    all_plugins: Mapping[str, Plugin], seed: ProjectSeed
) -> Tuple[CM, CM, List[Tuple[str, bool, bool]]]:
    repository_info: List[Tuple[str, bool, bool]] = []
    repository_names: List[str] = []

    credentials = CM({"credentials": CM({})})
    repositories = CM({"repositories": CS()})
    for plugin_name in seed.plugins:
        plugin = all_plugins[plugin_name]
        stub = stub_plugin(
            plugin, namespace=seed.namespace, repository=plugin_name, is_live=plugin.supports_mount
        )

        target_repo = f"{seed.namespace}/{plugin_name}"
        repository_info.append((target_repo, plugin.supports_mount, False))
        repository_names.append(target_repo)
        credentials["credentials"].update(stub["credentials"])
        repositories["repositories"].extend(stub["repositories"])

    if seed.include_dbt:
        dbt_params, dbt_credentials = generate_dbt_plugin_params(repository_names)
        dbt_repo = f"{seed.namespace}/dbt-sample"
        credential_name = "dbt-sample"
        repositories["repositories"].append(
            CM(
                {
                    "namespace": seed.namespace,
                    "repository": "dbt-sample",
                    "external": CM(
                        {
                            "plugin": "dbt",
                            "credential": credential_name,
                            "params": dbt_params,
                            "is_live": False,
                            "tables": CM({}),
                        }
                    ),
                    "metadata": CM(
                        {
                            "description": "Sample dbt model",
                            "readme": "## Sample dbt model\n\n"
                            "This is an autogenerated model referencing data from:\n\n"
                            + "\n".join(f"  * [/{r}](/{r})" for r in repository_names),
                        }
                    ),
                }
            )
        )
        credentials["credentials"][credential_name] = CM({"plugin": "dbt", "data": dbt_credentials})
        repository_info.append((dbt_repo, False, True))

    return (
        credentials,
        repositories,
        repository_info,
    )
