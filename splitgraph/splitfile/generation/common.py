# Separate module that doesn't pull in `prepare_splitfile_sql` so that the parent repo can
# use it without transitively pulling in pglast.

from typing import Callable, List, Optional, cast

from splitgraph.core.types import ProvenanceLine
from splitgraph.exceptions import SplitGraphError


def prov_command_to_splitfile(
    prov_data: ProvenanceLine,
    postprocess_sql: Optional[Callable[[str], str]] = None,
    postprocess_repo: Optional[Callable[[str, str], Optional[str]]] = None,
) -> str:
    """
    Converts the image's provenance data stored by the Splitfile executor back to a Splitfile used to
    reconstruct it.

    :param prov_data: Provenance line for one command
    :param postprocess_sql: Function to postprocess the SQL Splitfile command
    :param postprocess_repo: Function to override image references for a given repository
    :return: String with the Splitfile command.
    """

    def _get_image_for_repo(namespace: str, repository: str, image_hash: str) -> str:
        return (
            image_hash
            if not postprocess_repo
            else (postprocess_repo(namespace, repository) or image_hash)
        )

    prov_type = prov_data["type"]
    assert isinstance(prov_type, str)

    if prov_type == "IMPORT":
        namespace = cast(str, prov_data["source_namespace"])
        repository = cast(str, prov_data["source"])
        result = "FROM %s/%s:%s IMPORT " % (
            namespace,
            repository,
            _get_image_for_repo(namespace, repository, str(prov_data["source_hash"])),
        )
        result += ", ".join(
            "%s AS %s" % (tn if not q else "{" + tn.replace("}", "\\}") + "}", ta)
            for tn, ta, q in zip(
                cast(List[str], prov_data["tables"]),
                cast(List[str], prov_data["table_aliases"]),
                cast(List[bool], prov_data["table_queries"]),
            )
        )
        return result
    if prov_type == "FROM":
        namespace = cast(str, prov_data["source_namespace"])
        repository = cast(str, prov_data["source"])
        return "FROM %s/%s:%s" % (
            namespace,
            repository,
            _get_image_for_repo(namespace, repository, str(prov_data["source_hash"])),
        )
    if prov_type == "SQL":
        return (
            "SQL "
            + "{"
            + (
                postprocess_sql(str(prov_data["sql"])) if postprocess_sql else str(prov_data["sql"])
            ).replace("}", "\\}")
            + "}"
        )
    raise SplitGraphError("Cannot reconstruct provenance %s!" % prov_type)


def reconstruct_splitfile(
    provenance_data: List[ProvenanceLine],
    ignore_irreproducible: bool = False,
    postprocess_sql: Optional[Callable[[str], str]] = None,
    postprocess_repo: Optional[Callable[[str, str], Optional[str]]] = None,
) -> List[str]:
    """
    Recreate the Splitfile that can be used to reconstruct an image.
    """
    splitfile_commands = []
    for provenance_line in provenance_data:
        prov_type = provenance_line["type"]
        assert isinstance(prov_type, str)
        if prov_type in ("IMPORT", "SQL", "FROM"):
            splitfile_commands.append(
                prov_command_to_splitfile(provenance_line, postprocess_sql, postprocess_repo)
            )
        elif prov_type in ("MOUNT", "CUSTOM"):
            if not ignore_irreproducible:
                raise SplitGraphError(
                    "Image used a Splitfile command %s" " that can't be reproduced!" % prov_type
                )
            splitfile_commands.append("# Irreproducible Splitfile command of type %s" % prov_type)
    return splitfile_commands
