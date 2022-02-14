from typing import TYPE_CHECKING, Dict, List

from splitgraph.core.sql.splitfile_validation import prepare_splitfile_sql
from splitgraph.core.types import ProvenanceLine
from splitgraph.splitfile.generation.common import reconstruct_splitfile

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


def reconstruct_splitfile_with_replacement(
    provenance_data: List[ProvenanceLine],
    source_replacement: Dict["Repository", str],
    ignore_irreproducible: bool = False,
) -> List[str]:
    # circular import
    from splitgraph.core.repository import Repository

    def _postprocess_sql(sql: str):
        # Use the SQL validator/replacer to rewrite old image hashes into new hashes/tags.
        def image_mapper(repository: "Repository", image_hash: str):
            new_image = (
                repository.to_schema() + ":" + source_replacement.get(repository, image_hash)
            )
            return new_image, new_image

        _, replaced_sql = prepare_splitfile_sql(sql, image_mapper)
        return replaced_sql

    return reconstruct_splitfile(
        provenance_data,
        ignore_irreproducible,
        postprocess_sql=_postprocess_sql,
        postprocess_repo=lambda n, r: source_replacement.get(Repository(n, r)),
    )
