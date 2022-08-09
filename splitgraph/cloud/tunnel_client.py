import os
import subprocess
import sys
from os import path
from typing import IO, Optional, cast

from splitgraph.cloud.project.models import Repository

RATHOLE_CLIENT_FILENAME = "rathole-client.toml"

RATHOLE_CLIENT_TEMPLATE = """
[client]
remote_addr = "{rathole_server_management_address}"

[client.transport]
type = "tls"

[client.transport.tls]
{trusted_root_line}
hostname = "{hostname}"

[client.services."{namespace}/{repository}"]
local_addr = "{local_address}"
# token is provisioner JWT token
token = "{provisioning_token}"

"""


def get_rathole_client_config(
    rathole_server_management_address: str,
    hostname: str,
    local_address: str,
    provisioning_token: str,
    namespace: str,
    repository: str,
    trusted_root: Optional[str],
) -> str:
    trusted_root_line = f'trusted_root = "{trusted_root}"' if trusted_root else ""
    return RATHOLE_CLIENT_TEMPLATE.format(
        rathole_server_management_address=rathole_server_management_address,
        hostname=hostname,
        local_address=local_address,
        provisioning_token=provisioning_token,
        namespace=namespace,
        repository=repository,
        trusted_root_line=trusted_root_line,
    )


def write_rathole_client_config(
    provisioning_token: str, repository: Repository, config_dir: str
) -> str:
    # verify repository is external
    if not repository.external or not repository.external.tunnel:
        raise Exception("Repository %s not a tunneled external repository" % (repository))
    # TODO: instead of printing token, make gql call to provision tunnel
    print("Got provisioning token %s" % provisioning_token)
    # in production, this will be None, but for dev instances, we need to
    # specify rootCA.pem
    trusted_root = os.environ["REQUESTS_CA_BUNDLE"] or os.environ["SSL_CERT_FILE"]
    rathole_client_config = get_rathole_client_config(
        # TODO: replace these stub values with response of provisioning call
        rathole_server_management_address="34.70.46.40:2333",
        hostname="www.splitgraph.test",
        local_address=f"{repository.external.params['host']}:{repository.external.params['port']}",
        provisioning_token=provisioning_token,
        namespace=repository.namespace,
        repository=repository.repository,
        trusted_root=trusted_root,
    )
    config_filename = path.join(config_dir, RATHOLE_CLIENT_FILENAME)
    with open(config_filename, "w") as f:
        f.write(rathole_client_config)
    return config_filename


# inspired by https://stackoverflow.com/questions/18421757/live-output-from-subprocess-command
def launch_rathole_client(rathole_client_binary_path, rathole_client_config_path):
    process = subprocess.Popen(
        [rathole_client_binary_path, "--client", rathole_client_config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # pipe rathole process output to stdout
    for c in iter(lambda: cast(IO[bytes], process.stdout).read(1), b""):  # nomypy
        sys.stdout.buffer.write(c)
