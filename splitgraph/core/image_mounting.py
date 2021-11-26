import logging
from abc import ABC, abstractmethod
from hashlib import sha256
from typing import TYPE_CHECKING, Dict, List, Tuple

from splitgraph.core.common import get_temporary_table_id, unmount_schema
from splitgraph.core.engine import lookup_repository
from splitgraph.core.image import Image
from splitgraph.core.repository import Repository, clone
from splitgraph.core.types import ProvenanceLine

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


def _get_local_image_for_import(hash_or_tag: str, repository: Repository) -> Tuple[Image, bool]:
    """
    Converts a remote repository and tag into an Image object that exists on the engine,
    optionally pulling the repository or cloning it into a temporary location.

    :param hash_or_tag: Hash/tag
    :param repository: Name of the repository (doesn't need to be local)
    :return: Image object and a boolean flag showing whether the repository should be deleted
    when the image is no longer needed.
    """
    tmp_repo = Repository(repository.namespace, repository.repository + "_tmp_clone")
    repo_is_temporary = False

    logging.info("Resolving repository %s", repository)
    source_repo = lookup_repository(repository.to_schema(), include_local=True)
    if source_repo.engine.name != "LOCAL":
        clone(source_repo, local_repository=tmp_repo, download_all=False)
        source_image = tmp_repo.images[hash_or_tag]
        repo_is_temporary = True
    else:
        # For local repositories, first try to pull them to see if they are clones of a remote.
        if source_repo.upstream:
            source_repo.pull(single_image=hash_or_tag)
        source_image = source_repo.images[hash_or_tag]

    return source_image, repo_is_temporary


# TODO: eventually we'll need to reconcile these two classes. They differ in the sequencing and some
#  behaviour:
#  - ImageMapper returns schemas as it gets called; it also clones repos that don't yet exist
#    locally and runs the LQ checkout into a well-known location. It's used by Splitfiles. It can
#    also return provenance for built images.
#  - ImageMounter mounts and returns the schemas in a single bulk call and (the default
#    implementation) puts the schemas in a temporary location, letting checkouts from multiple
#    images co-exist. Used by other transformation plugins, like dbt.


class ImageMapper:
    """
    Image "mapper" used in Splitfile execution that returns the canonical form of an image
    and where it's mounted.
    """

    def __init__(self, object_engine: "PostgresEngine"):
        self.object_engine = object_engine

        self.image_map: Dict[Tuple[Repository, str], Tuple[str, str, Image]] = {}

        self._temporary_repositories: List[Repository] = []

    def _calculate_map(self, repository: Repository, hash_or_tag: str) -> Tuple[str, str, Image]:
        source_image, repo_is_temporary = _get_local_image_for_import(hash_or_tag, repository)
        if repo_is_temporary:
            self._temporary_repositories.append(source_image.repository)

        canonical_form = "%s/%s:%s" % (
            repository.namespace,
            repository.repository,
            source_image.image_hash,
        )

        temporary_schema = sha256(canonical_form.encode("utf-8")).hexdigest()[:63]

        return temporary_schema, canonical_form, source_image

    def __call__(self, repository: Repository, hash_or_tag: str) -> Tuple[str, str]:
        key = (repository, hash_or_tag)
        if key not in self.image_map:
            self.image_map[key] = self._calculate_map(key[0], key[1])

        temporary_schema, canonical_form, _ = self.image_map[key]
        return temporary_schema, canonical_form

    def setup_lq_mounts(self) -> None:
        for temporary_schema, _, source_image in self.image_map.values():
            self.object_engine.delete_schema(temporary_schema)
            self.object_engine.create_schema(temporary_schema)
            source_image.lq_checkout(target_schema=temporary_schema)

    def teardown_lq_mounts(self) -> None:
        for temporary_schema, _, _ in self.image_map.values():
            unmount_schema(self.object_engine, temporary_schema)
        # Delete temporary repositories that were cloned just for the SQL execution.
        for repo in self._temporary_repositories:
            repo.delete()

    def get_provenance_data(self) -> ProvenanceLine:
        return {
            "sources": [
                {
                    "source_namespace": source_repo.namespace,
                    "source": source_repo.repository,
                    "source_hash": source_image.image_hash,
                }
                for (source_repo, _), (_, _, source_image) in self.image_map.items()
            ]
        }


class ImageMounter(ABC):
    """
    Image "mounter" that's used for non-Splitfile transformation jobs (data source plugins that
    implement TransformingDataSource)
    """

    @abstractmethod
    def mount(self, images: List[Tuple[str, str, str]]) -> Dict[Tuple[str, str, str], str]:
        """Mount images into arbitrary schemas and return the image -> schema map"""
        raise NotImplementedError

    @abstractmethod
    def unmount(self) -> None:
        """Unmount the previously mounted images"""
        raise NotImplementedError


class DefaultImageMounter(ImageMounter):
    def __init__(self, engine: "PostgresEngine"):
        self.engine = engine
        self._image_map: Dict[Tuple[str, str, str], str] = {}

    def mount(self, images: List[Tuple[str, str, str]]) -> Dict[Tuple[str, str, str], str]:
        for image in images:
            if image not in self._image_map:
                ns, repo, hash_or_tag = image
                schema = get_temporary_table_id()
                self.engine.delete_schema(schema)
                self.engine.create_schema(schema)
                Repository(ns, repo, engine=self.engine).images[hash_or_tag].lq_checkout(schema)
                self._image_map[image] = schema
        self.engine.commit()
        return self._image_map

    def unmount(self) -> None:
        for tmp_schema in self._image_map.values():
            unmount_schema(self.engine, tmp_schema)
        self._image_map = {}
        self.engine.commit()
