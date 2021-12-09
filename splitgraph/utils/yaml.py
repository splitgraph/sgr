from typing import Any

from ruamel.yaml import YAML


def safe_load(stream) -> Any:
    yaml = YAML(typ="safe")
    return yaml.load(stream)


def safe_dump(obj: Any, stream, **kwargs) -> None:
    yaml = YAML(typ="safe")
    yaml.default_flow_style = False
    yaml.dump(obj, stream, **kwargs)
