# -*- mode: python -*-
# Version of splitgraph.spec that builds in a "single directory mode" to avoid a 10s startup
# on OSX Catalina where it scans binaries every time sgr extracts them:
# https://github.com/docker/compose/pull/7010

import os
import importlib

block_cipher = None

datas = []

a = Analysis(
    ["bin/sgr"],
    pathex=["."],
    # Imports that aren't toplevel or explicit
    hiddenimports=[
        "splitgraph.hooks.s3",
        "splitgraph.hooks.splitfile_commands",
        "splitgraph.ingestion.socrata.mount",
        "splitgraph.ingestion.socrata.querying",
        "splitgraph.ingestion.dbt.data_source",
        "splitgraph.ingestion.snowflake",
        "splitgraph.ingestion.athena",
        "splitgraph.ingestion.bigquery",
        # https://github.com/pypa/setuptools/issues/1963#issuecomment-574265532
        "pkg_resources.py2_warn",
        "target_postgres",
    ],
    hookspath=[],
    # Linux build on Travis pulls in numpy for no obvious reason
    excludes=["numpy"],
    runtime_hooks=[],
    cipher=block_cipher,
    datas=datas,
)

a.datas += Tree("./splitgraph/resources", "splitgraph/resources")

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='sgr',
          debug=False,
          strip=False,
          upx=True,
          console=True,
          bootloader_ignore_signals=True)


coll = COLLECT(exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    strip=False,
    upx=True,
    upx_exclude=[],
    name='sgr'
)
