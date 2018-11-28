import logging

from splitgraph.constants import Repository
from splitgraph.exceptions import SplitGraphException
from splitgraph.meta_handler.images import get_image


def provenance(repository, image_hash):
    """
    Inspects the parent chain of an sgfile-generated image to come up with a set of repositories and their hashes
    that it was created from.

    :param repository: Mountpoint that contains the image
    :param image_hash: Image hash to inspect
    :return: List of (repository, image_hash)
    """
    result = set()
    while image_hash:
        image = get_image(repository, image_hash)
        parent, prov_type, prov_data = image.parent_id, image.provenance_type, image.provenance_data
        if prov_type == 'IMPORT':
            result.add((Repository(prov_data['source_namespace'], prov_data['source']), prov_data['source_hash']))
        elif prov_type == 'FROM':
            # If we reached "FROM", then that's the first statement in the image build process (as it bases the build
            # on a completely different base image). Otherwise, let's say we have several versions of the source
            # repo and base some sgfile builds on each of them sequentially. In that case, the newest build will
            # have all of the previous FROM statements in it (since we clone the upstream commit history locally
            # and then add the FROM ... provenance data into it).
            result.add((Repository(prov_data['source_namespace'], prov_data['source']), image_hash))
            break
        elif prov_type in (None, 'MOUNT'):
            logging.warning("Image %s has provenance type %s, which means it might not be rederiveable.",
                            image_hash[:12], prov_type)
        image_hash = parent
    return list(result)


def prov_command_to_sgfile(prov_type, prov_data, image_hash, source_replacement):
    """
    Converts the image's provenance data stored by the sgfile executor back to an sgfile used to
    reconstruct it.

    :param prov_type: Provenance type (one of 'IMPORT' or 'SQL'). Any other provenances can't be reconstructed.
    :param prov_data: Provenance data as stored in the database.
    :param image_hash: Hash of the image
    :param source_replacement: Replace repository imports with different versions
    :return: String with the sgfile command.
    """
    if prov_type == "IMPORT":
        repo, image = Repository(prov_data['source_namespace'], prov_data['source']), prov_data['source_hash']
        result = "FROM %s:%s IMPORT " % (str(repo), source_replacement.get(repo, image))
        result += ", ".join("%s AS %s" % (tn if not q else "{" + tn.replace("}", "\\}") + "}", ta) for tn, ta, q
                            in zip(prov_data['tables'], prov_data['table_aliases'], prov_data['table_queries']))
        return result
    elif prov_type == "FROM":
        repo = Repository(prov_data['source_namespace'], prov_data['source'])
        return "FROM %s:%s" % (str(repo), source_replacement.get(repo, image_hash))
    elif prov_type == "SQL":
        return "SQL " + prov_data.replace("\n", "\\\n")
    raise SplitGraphException("Cannot reconstruct provenance %s!" % prov_type)


def image_hash_to_sgfile(repository, image_hash, err_on_end=True, source_replacement=None):
    """
    Crawls the image's parent chain to recreates an sgfile that can be used to reconstruct it.

    :param repository: Repository where the image is located.
    :param image_hash: Image hash to reconstruct.
    :param err_on_end: If False, when an image with no provenance is reached and it still has a parent, then instead of
        raising an exception, it will base the sgfile (using the FROM command) on that image.
    :param source_replacement: A dictionary of repositories and image hashes/tags specifying how to replace the
        dependencies of this sgfile (table imports and FROM commands).
    :return: A list of sgfile commands that can be fed back into the executor.
    """

    if source_replacement is None:
        source_replacement = {}
    sgfile_commands = []
    while image_hash:
        image = get_image(repository, image_hash)
        parent, prov_type, prov_data = image.parent_id, image.provenance_type, image.provenance_data
        if prov_type in ('IMPORT', 'SQL', 'FROM'):
            sgfile_commands.append(prov_command_to_sgfile(prov_type, prov_data, image_hash, source_replacement))
            if prov_type == 'FROM':
                break
        elif prov_type in (None, 'MOUNT') and parent:
            if err_on_end:
                raise SplitGraphException("Image %s is linked to its parent with provenance %s"
                                          " that can't be reproduced!" % (image_hash, prov_type))
            else:
                sgfile_commands.append("FROM %s:%s" % (repository, image_hash))
                break
        image_hash = parent
    return list(reversed(sgfile_commands))
