import os
import sys

# "Export" an object of SystemConfigGetters, for getting config values that
# require operations with the system, like checking if files exist.
#
# Don't forget to add each method definition to the object after defining it.
from typing import List, Optional

from .argument_config import get_argument_config_value
from .environment_config import get_environment_config_value

SYSTEM_CONFIG_GETTERS = {}

VALID_CONFIG_FILE_NAMES = [".sgconfig", ".sgrc", ".splitgraph.config"]

HOME_SUB_DIR = ".splitgraph"


def is_file(filename: str) -> bool:
    return os.path.isfile(filename)


def file_exists(_dir: str, filename: str) -> bool:
    return is_file(os.path.join(_dir, filename))


def get_explicit_config_file_location() -> Optional[str]:
    """ Get the explicitly defined location of config file, if defined.

        The location will either be defined in:

           * argument flag --config-file
           * or environment key SG_CONFIG_FILE

        In keeping with assumptions about priority, argument flag has
        higher priority than environment value.

        If the location is set, and points to an existing file, return location.

        Otherwise return None

        Print a warning if location is set but points to non-existing file.
    """

    key = "SG_CONFIG_FILE"

    explicit_location = get_argument_config_value(key, None) or get_environment_config_value(
        key, None
    )

    if explicit_location and is_file(explicit_location):
        return explicit_location
    if explicit_location and not is_file(explicit_location):
        sys.stderr.write("Warning: %s = %s is not a file" % (key, explicit_location))

    return None


def get_explicit_config_file_dirs() -> List[str]:
    """ Get any explicitly defined config file directories,
        which are directories where we should search for files from
        VALID_CONFIG_FILE_NAMES.

        This list is defined similar to $PATH, as a colon (:) delimited
        string, either in:

            * argument flag --config-dirs
            * or environment key SG_CONFIG_DIRS

        Or a single directory defined in either

            * argument flag --config-dir
            * environment key SG_CONFIG_DIR

        If both plural and single are defined, join them together.

        Return a list of valid paths that are set, or an empty list.

        Print a warning if any paths to not exist.
    """

    plural_key = "SG_CONFIG_DIRS"
    single_key = "SG_CONFIG_DIR"

    explicit_plural_string = get_argument_config_value(
        plural_key, None
    ) or get_environment_config_value(plural_key, None)

    explicit_single_string = get_argument_config_value(
        single_key, None
    ) or get_environment_config_value(single_key, None)

    explicit_plural_list = explicit_plural_string.split(":") if explicit_plural_string else []
    explicit_single_list = [explicit_single_string] if explicit_single_string else []
    explicit_dir_list = explicit_plural_list + explicit_single_list
    unique_explicit_dir_list = list(set(explicit_dir_list))

    if len(unique_explicit_dir_list) != len(explicit_dir_list):
        num_duplicates = len(explicit_dir_list) - len(unique_explicit_dir_list)

        sys.stderr.write("Warning: %d duplicate SG_CONFIG_DIRS values found.\n" % num_duplicates)

    existing_unique_dir_list = [d for d in unique_explicit_dir_list if os.path.isdir(d)]

    if len(existing_unique_dir_list) != len(unique_explicit_dir_list):
        num_non_existing = len(unique_explicit_dir_list) - len(existing_unique_dir_list)

        sys.stderr.write(
            "Warning: %d non-existing SG_CONFIG_DIRS values found.\n" % num_non_existing
        )

    return existing_unique_dir_list


def get_config_file(default_return: None = None) -> Optional[str]:
    """
        Get the location of an existing SG_CONFIG_FILE on the system with
        a valid name. Do not attempt to parse the config file, just return
        its location.

        Otherwise, return default_return
    """
    explicit_location = get_explicit_config_file_location()

    if explicit_location:
        return explicit_location

    valid_dirs = [os.getcwd()]

    if os.environ.get("HOME", None) and os.path.isdir(
        os.path.join(os.environ["HOME"], HOME_SUB_DIR)
    ):
        valid_dirs.append(os.path.join(os.environ["HOME"], HOME_SUB_DIR))

    explicit_dirs = get_explicit_config_file_dirs()

    # Put explicit_dirs first to ensure higher priority given to explicit
    valid_dirs = explicit_dirs + valid_dirs

    matching_files = []
    for _dir in valid_dirs:
        matching_files.extend(
            [
                os.path.join(_dir, name)
                for name in VALID_CONFIG_FILE_NAMES
                if file_exists(_dir, name)
            ]
        )

    num_matching_files = len(matching_files)

    if num_matching_files > 0:
        first_matching_file = matching_files[0]

        if num_matching_files > 1:
            sys.stderr.write(
                "Warning: %d splitgraph config files found. \n Using %s \n"
                % (num_matching_files, first_matching_file)
            )

        return first_matching_file

    return default_return


# Don't forget to do this for each method you want in the object
SYSTEM_CONFIG_GETTERS["SG_CONFIG_FILE"] = get_config_file


def get_system_config_value(key: str, default_return: None = None) -> Optional[str]:
    return SYSTEM_CONFIG_GETTERS.get(key, lambda: default_return)()
