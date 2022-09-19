import hashlib
import os
import platform
import subprocess
from io import BytesIO
from os import path
from typing import Optional
from zipfile import ZipFile

import requests
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

# Download URL and SHA256 hash of rathole build ZIP archive for each supported platform.
RATHOLE_BUILDS = {
    "Darwin": (
        "https://github.com/rapiz1/rathole/releases/download/v0.4.4/rathole-x86_64-apple-darwin.zip",
        "c1e6d0a41a0af8589303ab6940937d9183b344a62283ff6033a17e82c357ce17",
    ),
    "Windows": (
        "https://github.com/rapiz1/rathole/releases/download/v0.4.4/rathole-x86_64-pc-windows-msvc.zip",
        "92cc3feb57149c0b4dba7ec198dbda26c4831cde0a7c74a7d9f51e0002f65ead",
    ),
    "Linux-x86_64-glibc": (
        "https://github.com/rapiz1/rathole/releases/download/v0.4.4/rathole-x86_64-unknown-linux-gnu.zip",
        "fef39ed9d25e944711e2a27d5a9c812163ab184bf3f703827fca6bbf54504fbf",
    ),
    "Linux-x86_64-musl": (
        "https://github.com/rapiz1/rathole/releases/download/v0.4.4/rathole-x86_64-unknown-linux-musl.zip",
        "fc6b0a57727383a1491591f8e9ee76b1e0e25ecf7c2736b803d8f4411f651a15",
    ),
}


def get_sha256(stream):
    return hashlib.sha256(stream.read()).hexdigest()


def get_rathole_build_key():
    system = platform.system()
    # Currently only x86_64 macos builds exist for Windows and MacOS.
    # It works on Apple Silicon due to Rosetta 2.
    if system in ["Windows", "Darwin"]:
        return system
    if system == "Linux":
        # python 3.8 somtimes reports '' instead of 'musl' for musl libc (https://bugs.python.org/issue43248)
        return "Linux-%s-%s" % (
            platform.machine(),
            "glibc" if platform.libc_ver()[0] == "glibc" else "musl",
        )
    return None


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


def get_rathole_client_binary_path(config_dir: Optional[str] = None) -> str:
    filename = "rathole.exe" if get_rathole_build_key() == "Windows" else "rathole"
    return os.path.join(config_dir or get_config_dir(), filename)


def get_config_filename(config_dir: Optional[str] = None) -> str:
    return path.join(config_dir or get_config_dir(), RATHOLE_CLIENT_CONFIG_FILENAME)


def download_rathole_binary(
    build: Optional[str] = None, rathole_path: Optional[str] = None
) -> None:
    rathole_binary_path = rathole_path or get_rathole_client_binary_path()
    (url, sha256) = RATHOLE_BUILDS.get(build or get_rathole_build_key(), ("", ""))
    if not url:
        raise Exception("No rathole build found for this architecture")
    content = BytesIO(requests.get(url).content)
    assert get_sha256(content) == sha256
    content.seek(0)
    zipfile = ZipFile(content)
    assert len(zipfile.filelist) == 1
    assert zipfile.filelist[0].filename == path.basename(rathole_binary_path)
    zipfile.extract(path.basename(rathole_binary_path), path.dirname(rathole_binary_path))
    if get_rathole_build_key() != "Windows":
        os.chmod(rathole_binary_path, 0o500)


def write_rathole_client_config(
    section_id: str,
    secret_token: str,
    tunnel_connect_host: str,
    tunnel_connect_port: int,
    local_address: str,
    tls_hostname: Optional[str],
) -> str:
    # If a specific root CA file is used (eg: for self-signed hosts), reference
    # it in the rathole client config. Otherwise use system default trust root.
    trusted_root = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
    rathole_client_config = get_rathole_client_config(
        tunnel_connect_address=f"{tunnel_connect_host}:{tunnel_connect_port}",
        tls_hostname=tls_hostname or tunnel_connect_host,
        local_address=local_address,
        secret_token=secret_token,
        section_id=section_id,
        trusted_root=trusted_root,
    )
    config_filename = get_config_filename()
    with open(config_filename, "w") as f:
        f.write(rathole_client_config)
    return config_filename


def launch_rathole_client(
    rathole_client_binary_path: Optional[str] = None,
    rathole_client_config_path: Optional[str] = None,
) -> None:
    binary_path = rathole_client_binary_path or get_rathole_client_binary_path()
    if not path.isfile(binary_path):
        download_rathole_binary()
    echo("launching rathole client")
    command = [
        binary_path,
        "--client",
        rathole_client_config_path or get_config_filename(),
    ]
    subprocess.check_call(command)
