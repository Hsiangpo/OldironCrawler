from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType


try:
    import resource as _RESOURCE_MODULE
except ImportError:
    _RESOURCE_MODULE = None


def configure_stdio_utf8() -> None:
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


def resolve_runtime_root(*, entry_file: Path, frozen: bool, executable_path: Path | None) -> Path:
    if frozen and executable_path is not None:
        preferred = executable_path.resolve().parent
        if _directory_is_usable(preferred):
            return preferred
        local_appdata = str(os.getenv("LOCALAPPDATA", "") or "").strip()
        if local_appdata:
            fallback = Path(local_appdata) / "OldIronCrawler"
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback
    return entry_file.resolve().parent


def raise_nofile_soft_limit(
    target: int = 65536,
    *,
    resource_module: ModuleType | None = None,
) -> None:
    resource_module = _RESOURCE_MODULE if resource_module is None else resource_module
    if resource_module is None:
        return
    soft, hard = resource_module.getrlimit(resource_module.RLIMIT_NOFILE)
    desired = max(int(target), 1024)
    if soft >= desired:
        return
    next_soft = desired if hard == resource_module.RLIM_INFINITY else min(desired, hard)
    if next_soft <= soft:
        return
    resource_module.setrlimit(resource_module.RLIMIT_NOFILE, (next_soft, hard))


def _directory_is_usable(directory: Path) -> bool:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".write_probe"
        probe.write_text("", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False
