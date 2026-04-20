# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path


project_root = Path.cwd().resolve()


a = Analysis(
    [str(project_root / "run.py")],
    pathex=[str(project_root), str(project_root / "src")],
    binaries=[],
    datas=[],
    hiddenimports=["cloudscraper", "h2"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="OldIronCrawler",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    disable_windowed_traceback=False,
    icon=str(project_root / "packaging" / "OldIronCrawler.ico"),
)
