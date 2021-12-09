from typing import Any

from ruamel.yaml import YAML


def safe_load(stream) -> Any:
    yaml = YAML(typ="safe", pure=True)
    return yaml.load(stream)


def safe_dump(obj: Any, stream, **kwargs) -> None:
    yaml = YAML(typ="safe", pure=True)
    yaml.dump(obj, stream, **kwargs)
