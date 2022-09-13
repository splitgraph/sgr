import os
import subprocess
from os import path
from typing import Optional

from click import echo

RATHOLE_CLIENT_CONFIG_FILENAME = "rathole-client.toml"

RATHOLE_CLIENT_CONFIG_TEMPLATE = """
[client]
remote_addr = "{tunnel_connect_address}"

[client.transport]
type = "tls"

[client.transport.tls]
{trusted_root_line}
hostname = "{tls_hostname}"

[client.services."{section_id}"]
local_addr = "{local_address}"
token = "{secret_token}"

"""


def get_rathole_client_config(
    tunnel_connect_address: str,
    tls_hostname: str,
    local_address: str,
    secret_token: str,
    section_id: str,
    trusted_root: Optional[str],
) -> str:
    trusted_root_line = f'trusted_root = "{trusted_root}"' if trusted_root else ""
    return RATHOLE_CLIENT_CONFIG_TEMPLATE.format(
        tunnel_connect_address=tunnel_connect_address,
        tls_hostname=tls_hostname,
        local_address=local_address,
        secret_token=secret_token,
        section_id=section_id,
        trusted_root_line=trusted_root_line,
    )


def get_config_dir():
    from splitgraph.config import CONFIG
    from splitgraph.config.config import get_singleton

    return os.path.dirname(get_singleton(CONFIG, "SG_CONFIG_FILE"))


def get_rathole_client_binary_path():
    return os.path.join(get_config_dir(), "rathole")


def write_rathole_client_config(
    section_id: str,
    secret_token: str,
    tunnel_connect_host: str,
    tunnel_connect_port: int,
    local_address: str,
    tls_hostname: Optional[str],
) -> str:
    # If a specific root CA file is used (eg: for self-signed hosts), reference
    # it in the rathole client config.
    trusted_root = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
    rathole_client_config = get_rathole_client_config(
        tunnel_connect_address=f"{tunnel_connect_host}:{tunnel_connect_port}",
        tls_hostname=tls_hostname or tunnel_connect_host,
        local_address=local_address,
        secret_token=secret_token,
        section_id=section_id,
        trusted_root=trusted_root,
    )
    config_filename = path.join(get_config_dir(), RATHOLE_CLIENT_CONFIG_FILENAME)
    with open(config_filename, "w") as f:
        f.write(rathole_client_config)
    return config_filename


def launch_rathole_client(rathole_client_config_path: str) -> None:
    echo("launching rathole client")
    command = [get_rathole_client_binary_path(), "--client", rathole_client_config_path]
    subprocess.check_call(command)
