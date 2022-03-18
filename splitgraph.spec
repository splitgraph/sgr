# -*- mode: python -*-
# Running:
# * LD_LIBRARY_PATH=`echo $(python3-config --prefix)/lib` pyinstaller -F splitgraph.spec produces a single sgr binary in the dist/ folder
#   with libc being the only dynamic dependency (python interpreter included)
# * can also do poetry install && poetry run pyinstaller -F splitgraph.spec to build the binary inside of the poetry's venv.
# * specifying `--onedir` instead of `-F` will compile a multi-file executable with COLLECT()
# * e.g. : pyinstaller --clean --noconfirm --onedir splitgraph.spec

import sys
import os
import importlib

# Pass --onedir or -D to build a multi-file executable (dir with shlibs + exe)
# Note: This is the same flag syntax as pyinstaller uses, but when using a
#       .spec file with pyinstaller, the flag is normally ignored, so we
#       explicitly check for it here. This way we can pass different arguments
#       to EXE() and conditionally call COLLECTION() without duplicating code.
MAKE_EXE_COLLECTION = False
if "--onedir" in sys.argv or "-D" in sys.argv:
    print("splitgraph.spec : --onedir was specified. Will build a multi-file executable...")
    MAKE_EXE_COLLECTION = True

block_cipher = None

datas = []
# Commented out, here for reference (adding extra package files to PyInstaller)
# for package, files in [("singer", ["logging.conf"])]:
#     proot = os.path.dirname(importlib.import_module(package).__file__)
#     datas.extend((os.path.join(proot, f), package) for f in files)

a = Analysis(
    ["bin/sgr"],
    pathex=["."],
    # Imports that aren't toplevel or explicit (e.g. pyyaml is imported inline in sgr to speed sgr invocations up)
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

# Note: `exe` global is injected by pyinstaller. Can see the code for EXE at:
# https://github.com/pyinstaller/pyinstaller/blob/15f23a8a89be5453b3520df8fc3e667346e103a6/PyInstaller/building/api.py

# TOCS to include in EXE() when in single-file mode (default, i.e. no --onedir flag)
all_tocs = [a.scripts, a.binaries, a.zipfiles, a.datas]
# TOCS to include in EXE() when in multi-file mode (i.e., --onedir flag)
exe_tocs = [a.scripts]
# TOCS to include in COLL() when in multi-file mode (i.e., --onedir flag)
coll_tocs = [a.binaries, a.zipfiles, a.datas]

# When compiling single-file executable, we include every TOC in the EXE
# When compiling multi-file executable, include some TOC in EXE, and rest in COLL
exe_args = [pyz, *exe_tocs, []] if MAKE_EXE_COLLECTION else [pyz, *all_tocs, []]

exe_kwargs_base = {
    "name": "sgr",
    "debug": False,
    "bootloader_ignore_signals": False,
    "strip_binaries": False,
    "runtime_tmpdir": None,
    "console": True,
}
# In multi-file mode, we exclude_binaries from EXE since they will be in COLL
exe_kwargs_onedir = {**exe_kwargs_base, "upx": False, "exclude_binaries": True}
# In single-file mode, we set upx: true because it works. (It might actually work in multi-file mode too)
exe_kwargs_onefile = {**exe_kwargs_base, "upx": True, "exclude_binaries": False}
exe_kwargs = exe_kwargs_onedir if MAKE_EXE_COLLECTION else exe_kwargs_onefile

exe = EXE(*exe_args, **exe_kwargs)

if MAKE_EXE_COLLECTION:
    coll = COLLECT(exe, *coll_tocs, name="sgr-pkg", strip=False, upx=False)
