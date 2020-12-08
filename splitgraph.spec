# -*- mode: python -*-
# Running:
# * LD_LIBRARY_PATH=`echo $(python3-config --prefix)/lib` pyinstaller -F splitgraph.spec produces a single sgr binary in the dist/ folder
#   with libc being the only dynamic dependency (python interpreter included)
# * can also do poetry install && poetry run pyinstaller -F splitgraph.spec to build the binary inside of the poetry's venv.

import os
import importlib

block_cipher = None

datas = []
# Commented out, here for reference (adding extra package files to PyInstaller)
# for package, files in [("singer", ["logging.conf"])]:
#     proot = os.path.dirname(importlib.import_module(package).__file__)
#     datas.extend((os.path.join(proot, f), package) for f in files)

a = Analysis(['bin/sgr'],
             pathex=['.'],
             # Imports that aren't toplevel or explicit (e.g. pyyaml is imported inline in sgr to speed sgr invocations up)
             hiddenimports=["splitgraph.hooks.s3", "splitgraph.hooks.splitfile_commands",
             "splitgraph.ingestion.socrata.mount", "splitgraph.ingestion.socrata.querying",
             # https://github.com/pypa/setuptools/issues/1963#issuecomment-574265532
             "pkg_resources.py2_warn",
             "target_postgres"],
             hookspath=[],
             # Linux build on Travis pulls in numpy for no obvious reason
             excludes=['numpy'],
             runtime_hooks=[],
             cipher=block_cipher,
             datas=datas)

a.datas += Tree('./splitgraph/resources', 'splitgraph/resources')

pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='sgr',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )
