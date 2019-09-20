import sys
from typing import List, Optional, Tuple

from .keys import ARG_KEYS, ARGUMENT_KEY_MAP


def get_arg_tuples() -> List[Tuple[str, str]]:
    """
        Get the raw (argN, argN+1) tuples from sys.argv

        We could also use click to parse the flags ahead of time, and then pass
        the parsed flag object into SystemConfigGetters. But this way, we avoid
        having to pass down variables from click just to get config values.
    """

    # This one is for when we are on the engine -- then sys.argv doesn't exist in embedded Python
    # and importing the config fails
    sys_args = sys.argv[1:] if hasattr(sys, "argv") else [""]

    return [(a1, a2) for a1, a2 in zip(sys_args, sys_args[1:]) if a1 in ARG_KEYS]


def get_argument_config_value(key: str, default_return: Optional[str] = None) -> Optional[str]:
    """
        Get get the value of an argument, where value is the argument
        immediately following the argument matching a key in ARG_KEYS, e.g.:

            SYS_ARGS = ["--namespace", "foo"]
            --> return "foo"

        Otherwise, return default_return
    """

    arg_tuples = get_arg_tuples()
    matching_values = [v for k, v in arg_tuples if ARGUMENT_KEY_MAP[k] == key]
    num_matching_values = len(matching_values)

    if num_matching_values == 0:
        return default_return

    sys.stderr.write("Warning: multiple values specified for %s \n" % key)
    sys.stderr.write("Using %s \n" % matching_values[0])
    return matching_values[0]
