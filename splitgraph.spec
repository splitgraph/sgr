# -*- mode: python -*-
# Running:
# * pyinstaller -F splitgraph.spec produces a single sgr binary in the dist/ folder
#   with libc being the only dynamic dependency (python interpreter included)
# * can also do poetry install && poetry run pyinstaller -F splitgraph.spec to build the binary inside of the poetry's venv.

block_cipher = None

a = Analysis(['bin/sgr'],
             pathex=['.'],
             hiddenimports=[],
             # Disable ingestion extras for now (we're looking at replacing this
             # with something more lightweight)
             excludes=['pandas','numpy','scipy', 'sqlalchemy'],
             hookspath=[],
             runtime_hooks=[],
             cipher=block_cipher)

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
