import os
import subprocess
import sys
from os import path
from typing import IO, Any, Dict, Optional, cast

from splitgraph.core.repository import Repository

RATHOLE_CLIENT_CONFIG_FILENAME = "rathole-client.toml"

RATHOLE_CLIENT_CONFIG_TEMPLATE = """
[client]
remote_addr = "{tunnel_server_management_address}"

[client.transport]
type = "tls"

[client.transport.tls]
{trusted_root_line}
hostname = "{tls_hostname}"

[client.services."{namespace}/{repository}"]
local_addr = "{local_address}"
# token is provisioner JWT token
token = "{provisioning_token}"

"""


def get_rathole_client_config(
    tunnel_server_management_address: str,
    tls_hostname: str,
    local_address: str,
    provisioning_token: str,
    namespace: str,
    repository: str,
    trusted_root: Optional[str],
) -> str:
    trusted_root_line = f'trusted_root = "{trusted_root}"' if trusted_root else ""
    return RATHOLE_CLIENT_CONFIG_TEMPLATE.format(
        tunnel_server_management_address=tunnel_server_management_address,
        tls_hostname=tls_hostname,
        local_address=local_address,
        provisioning_token=provisioning_token,
        namespace=namespace,
        repository=repository,
        trusted_root_line=trusted_root_line,
    )


def write_rathole_client_config(
    provisioning_token: str,
    tunnel_server_management_host: str,
    tunnel_server_management_port: int,
    tls_hostname: Optional[str],
    repository: Repository,
    params: Dict[str, Any],
    config_dir: str,
) -> str:
    # TODO: instead of printing token, make gql call to provision tunnel
    print("Got provisioning token %s" % provisioning_token)
    # in production, this will be None, but for dev instances, we need to
    # specify rootCA.pem
    trusted_root = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
    rathole_client_config = get_rathole_client_config(
        # TODO: replace these stub values with response of provisioning call
        tunnel_server_management_address=f"{tunnel_server_management_host}:{tunnel_server_management_port}",
        tls_hostname=tls_hostname or tunnel_server_management_host,
        local_address=f"{params['host']}:{params['port']}",
        provisioning_token=provisioning_token,
        namespace=repository.namespace,
        repository=repository.repository,
        trusted_root=trusted_root,
    )
    config_filename = path.join(config_dir, RATHOLE_CLIENT_CONFIG_FILENAME)
    with open(config_filename, "w") as f:
        f.write(rathole_client_config)
    return config_filename


# inspired by https://stackoverflow.com/questions/18421757/live-output-from-subprocess-command
def launch_rathole_client(rathole_client_binary_path: str, rathole_client_config_path: str) -> None:
    process = subprocess.Popen(
        [rathole_client_binary_path, "--client", rathole_client_config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # pipe rathole process output to stdout
    for c in iter(lambda: cast(IO[bytes], process.stdout).read(1), b""):  # nomypy
        sys.stdout.buffer.write(c)
