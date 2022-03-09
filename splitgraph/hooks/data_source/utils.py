from copy import deepcopy
from typing import Any, Dict


def merge_jsonschema(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(left)
    result["properties"] = {**result["properties"], **right.get("properties", {})}
    result["required"] = result.get("required", []) + [
        r for r in right.get("required", []) if r not in result.get("required", [])
    ]
    return result
