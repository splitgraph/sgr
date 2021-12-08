import itertools
from io import StringIO
from typing import Any, Dict

import ruamel.yaml
from ruamel.yaml import CommentedMap as CM, CommentedSeq as CS

from splitgraph.cloud import Plugin


def get_comment(jsonschema_object: Any) -> str:
    """
    Get a YAML comment to be attached to a JSONSchema property.
    """
    result = []
    if "title" in jsonschema_object:
        result.append(jsonschema_object["title"])
    if "description" in jsonschema_object:
        result.append(jsonschema_object["description"])
    if "enum" in jsonschema_object:
        enum = jsonschema_object["enum"]
        if len(enum) == 1:
            result.append("Constant")
        else:
            result.append("One of " + ", ".join(enum))
    if "const" in jsonschema_object:
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
            if obj_type == "string":
                return ""
            elif obj_type == "integer":
                return 0
            elif obj_type == "boolean":
                return False
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
        raise AssertionError


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


TEMPLATE = """credentials:
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


def stub_plugin(plugin: Plugin, namespace: str, repository: str) -> CM:
    """
    Generate a repositories.yml file based on a plugin's JSONSchemas.
    """
    yml = ruamel.yaml.YAML()
    repositories_yaml = (
        TEMPLATE.replace("CREDENTIAL_NAME", plugin.plugin_name)
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

    return ruamel_dict
