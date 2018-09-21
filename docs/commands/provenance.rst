Provenance tracking
===================

Every :ref:`SGFile command<sgfile>` is recorded in the image metadata so that it's possible to track which datasets an
image depends on, as well as how it can be recreated. Images that are created by `MOUNT` commands
(data import from a mounted database) aren't currently supported, as it's assumed that those databases
aren't publicly accessible.

Provenance tracking allows Splitgraph to recreate the SGFile the image was made with, as well as rebase the image to
use a different version of the datasets it was made from.

`sg provenance MOUNTPOINT IMAGE_OR_TAG`
    Inspects the image's parents and outputs a list of datasets and their versions
    that were used to create this image (via `IMPORT` or `FROM` commands). If the `-f (--full)` flag is passed, then the
    command will try to reconstruct the full sgfile used to create the image, raising an error if there's a break in the
    provenance chain (e.g. the `MOUNT` command or a SQL query outside of the sgfile interpreter was used somewhere
    in the history of the image). If the `-e` flag is passed, the command will instead stop at the first break in the chain
    and base the resulting sgfile before the break (using the `FROM` command).

`sg rerun MOUNTPOINT IMAGE_OR_TAG -i DATASET1 IMAGE_OR_TAG1 -i ...`
    Recreates the SGFile used to derive a given image
    and reruns it, replacing its dependencies as specified by the `-i` options. If the `-u` flag is passed, the image
    is rederived based on the `latest` tag of all its dependencies.

    For example, if `pgderiv:v1` was created with `pgorigin:v1` and `pgorigin` has been updated on the remote to tag `v2`,
    then both `sg rerun pgderiv v1 -i pgorigin v2` and `sg rerun -u pgderiv v1` will have the same effect of rerunning
    the sgfile used to create `pgderiv` based on the latest version of `pgorigin`.
