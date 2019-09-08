import itertools
from datetime import datetime

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from splitgraph import select, SPLITGRAPH_API_SCHEMA, ResultShape, SPLITGRAPH_META_SCHEMA
from splitgraph.core import repository_exists
from splitgraph.core.image import IMAGE_COLS, Image
from splitgraph.exceptions import ImageNotFoundError


class ImageManager:
    """Collects various image-related functions."""

    def __init__(self, repository):
        self.repository = repository
        self.engine = repository.engine

    def __call__(self):
        """Get all Image objects in the repository, ordered by their creation time (earliest first)."""
        result = []
        for image in self.engine.run_sql(
            select(
                "get_images",
                ",".join(IMAGE_COLS),
                schema=SPLITGRAPH_API_SCHEMA,
                table_args="(%s, %s)",
            ),
            (self.repository.namespace, self.repository.repository),
        ):
            result.append(self._make_image(image))
        return result

    def _make_image(self, img_tuple):
        r_dict = {k: v for k, v in zip(IMAGE_COLS, img_tuple)}
        r_dict.update(repository=self.repository)
        return Image(**r_dict)

    def by_tag(self, tag, raise_on_none=True):
        """
        Returns an image with a given tag

        :param tag: Tag. 'latest' is a special case: it returns the most recent image in the repository.
        :param raise_on_none: Whether to raise an error or return None if the tag doesn't exist.
        """
        engine = self.engine
        if not repository_exists(self.repository) and raise_on_none:
            raise ImageNotFoundError("%s does not exist!" % str(self))

        if tag == "latest":
            # Special case, return the latest commit from the repository.
            result = self.engine.run_sql(
                select(
                    "get_images",
                    ",".join(IMAGE_COLS),
                    schema=SPLITGRAPH_API_SCHEMA,
                    table_args="(%s,%s)",
                )
                + SQL(" ORDER BY created DESC LIMIT 1"),
                (self.repository.namespace, self.repository.repository),
                return_shape=ResultShape.ONE_MANY,
            )
            if result is None:
                raise ImageNotFoundError("No images found in %s!" % self.repository.to_schema())
            return self._make_image(result)

        result = engine.run_sql(
            select(
                "get_tagged_images",
                "image_hash",
                "tag = %s",
                schema=SPLITGRAPH_API_SCHEMA,
                table_args="(%s,%s)",
            ),
            (self.repository.namespace, self.repository.repository, tag),
            return_shape=ResultShape.ONE_ONE,
        )
        if result is None:
            if raise_on_none:
                schema = self.repository.to_schema()
                if tag == "HEAD":
                    raise ImageNotFoundError(
                        'No current checked out revision found for %s. Check one out with "sgr '
                        'checkout %s image_hash".' % (schema, schema)
                    )
                raise ImageNotFoundError("Tag %s not found in repository %s" % (tag, schema))
            return None
        return self.by_hash(result)

    def by_hash(self, image_hash, raise_on_none=True):
        """
        Returns an image corresponding to a given (possibly shortened) image hash. If the image hash
        is ambiguous, raises an error. If the image does not exist, raises an error or returns None.

        :param image_hash: Image hash (can be shortened).
        :param raise_on_none: Whether to raise if the image doesn't exist.
        :return: Image object or None
        """
        result = self.engine.run_sql(
            select(
                "get_images",
                ",".join(IMAGE_COLS),
                schema=SPLITGRAPH_API_SCHEMA,
                table_args="(%s, %s)",
                where="image_hash LIKE %s",
            ),
            (self.repository.namespace, self.repository.repository, image_hash.lower() + "%"),
            return_shape=ResultShape.MANY_MANY,
        )
        if not result:
            if raise_on_none:
                raise ImageNotFoundError("No images starting with %s found!" % image_hash)
            return None
        if len(result) > 1:
            result = "Multiple suitable candidates found: \n * " + "\n * ".join(result)
            raise ImageNotFoundError(result)
        return self._make_image(result[0])

    def __getitem__(self, key):
        """Resolve an Image object from its tag or hash."""
        # Things we can have here: full hash, shortened hash or tag.
        # Users can always use by_hash or by_tag to be explicit -- this is just a shorthand. There's little
        # chance for ambiguity (why would someone have a hexadecimal tag that can be confused with a hash?)
        # so we can detect what the user meant in the future.
        return self.by_tag(key, raise_on_none=False) or self.by_hash(key)

    def get_all_child_images(self, start_image):
        """Get all children of `start_image` of any degree."""
        all_images = self()
        result_size = 1
        result = {start_image}
        while True:
            # Keep expanding the set of children until it stops growing
            for image in all_images:
                if image.parent_id in result:
                    result.add(image.image_hash)
            if len(result) == result_size:
                return result
            result_size = len(result)

    def get_all_parent_images(self, start_images):
        """Get all parents of the 'start_images' set of any degree."""
        parent = {image.image_hash: image.parent_id for image in self()}
        result = set(start_images)
        result_size = len(result)
        while True:
            # Keep expanding the set of parents until it stops growing
            result.update({parent[image] for image in result if parent[image] is not None})
            if len(result) == result_size:
                return result
            result_size = len(result)

    def add(
        self,
        parent_id,
        image,
        created=None,
        comment=None,
        provenance_type=None,
        provenance_data=None,
    ):
        """
        Registers a new image in the Splitgraph image tree.

        Internal method used by actual image creation routines (committing, importing or pulling).

        :param parent_id: Parent of the image
        :param image: Image hash
        :param created: Creation time (defaults to current timestamp)
        :param comment: Comment (defaults to empty)
        :param provenance_type: Image provenance that can be used to rebuild the image
            (one of None, FROM, MOUNT, IMPORT, SQL)
        :param provenance_data: Extra provenance data (dictionary).
        """
        self.engine.run_sql(
            SQL("SELECT {}.add_image(%s, %s, %s, %s, %s, %s, %s, %s)").format(
                Identifier(SPLITGRAPH_API_SCHEMA)
            ),
            (
                self.repository.namespace,
                self.repository.repository,
                image,
                parent_id,
                created or datetime.now(),
                comment,
                provenance_type,
                Json(provenance_data),
            ),
        )

    def delete(self, images):
        """
        Deletes a set of Splitgraph images from the repository. Note this doesn't check whether
        this will orphan some other images in the repository and can make the state of the repository
        invalid.

        Image deletions won't be replicated on push/pull (those can only add new images).

        :param images: List of image IDs
        """
        if not images:
            return
        # Maybe better to have ON DELETE CASCADE on the FK constraints instead of going through
        # all tables to clean up -- but then we won't get alerted when we accidentally try
        # to delete something that does have FKs relying on it.
        args = tuple([self.repository.namespace, self.repository.repository] + list(images))
        for table in ["tags", "tables", "images"]:
            self.engine.run_sql(
                SQL(
                    "DELETE FROM {}.{} WHERE namespace = %s AND repository = %s "
                    "AND image_hash IN (" + ",".join(itertools.repeat("%s", len(images))) + ")"
                ).format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table)),
                args,
            )

    def __iter__(self):
        return iter(self())
