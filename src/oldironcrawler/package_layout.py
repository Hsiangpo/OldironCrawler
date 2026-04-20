from __future__ import annotations

import shutil
from pathlib import Path

_PACKAGED_ALLOWED_KEYS = {
    "LLM_BASE_URL",
    "LLM_MODEL",
    "LLM_REASONING_EFFORT",
    "LLM_API_STYLE",
    "LLM_CONCURRENCY",
    "SITE_CONCURRENCY",
    "PAGE_CONCURRENCY",
    "PAGE_WORKER_COUNT",
    "PAGE_HOST_LIMIT",
    "REP_PAGE_LIMIT",
    "EMAIL_PAGE_SOFT_LIMIT",
    "EMAIL_PAGE_HARD_LIMIT",
    "PAGE_TOTAL_HARD_LIMIT",
    "EMAIL_STOP_SAME_DOMAIN_COUNT",
    "REQUEST_TIMEOUT_SECONDS",
    "TOTAL_WAIT_SECONDS",
}
_PACKAGED_OVERRIDES = {
    "LLM_CONCURRENCY": "32",
    "SITE_CONCURRENCY": "32",
    "PAGE_CONCURRENCY": "32",
    "PAGE_WORKER_COUNT": "32",
    "PAGE_HOST_LIMIT": "32",
}


def build_portable_dist_folder(*, repo_root: Path, built_exe_path: Path) -> Path:
    dist_root = repo_root / "dist"
    package_root = dist_root / "OldIronCrawler"
    package_root.mkdir(parents=True, exist_ok=True)

    shutil.copy2(built_exe_path, package_root / "OldIronCrawler.exe")
    _write_packaged_env(repo_root=repo_root, package_root=package_root)
    _ensure_placeholder(package_root / "websites", "把 Excel 网站表放到这里.md", "请把网站表放到这个文件夹。\n")
    _ensure_placeholder(package_root / "output", "结果会输出到这里.txt", "程序输出的结果会出现在这个文件夹。\n")
    _ensure_placeholder(package_root / "output" / "runtime", "运行缓存会自动生成.txt", "程序运行缓存会自动生成在这里。\n")
    return package_root


def _write_packaged_env(*, repo_root: Path, package_root: Path) -> None:
    source_env = repo_root / ".env"
    lines = source_env.read_text(encoding="utf-8").splitlines() if source_env.exists() else []
    cleaned: list[str] = []
    seen_key = False
    seen_api_key = False
    seen_allowed: set[str] = set()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            cleaned.append(line)
            continue
        key, _separator, _value = stripped.partition("=")
        key = key.strip()
        if stripped.startswith("LLM_KEY="):
            cleaned.append("LLM_KEY=")
            seen_key = True
            continue
        if stripped.startswith("LLM_API_KEY="):
            cleaned.append("LLM_API_KEY=")
            seen_api_key = True
            continue
        if key not in _PACKAGED_ALLOWED_KEYS:
            continue
        seen_allowed.add(key)
        if key in _PACKAGED_OVERRIDES:
            cleaned.append(f"{key}={_PACKAGED_OVERRIDES[key]}")
            continue
        cleaned.append(line)
    if not seen_key:
        cleaned.append("LLM_KEY=")
    if not seen_api_key:
        cleaned.append("LLM_API_KEY=")
    for name, value in _PACKAGED_OVERRIDES.items():
        if name in seen_allowed:
            continue
        cleaned.append(f"{name}={value}")
    (package_root / ".env").write_text("\n".join(cleaned).rstrip() + "\n", encoding="utf-8")


def _ensure_placeholder(directory: Path, filename: str, content: str) -> None:
    _ensure_directory_ready(directory)
    placeholder = directory / filename
    if not placeholder.exists():
        placeholder.write_text(content, encoding="utf-8")


def _ensure_directory_ready(directory: Path) -> None:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".write_probe"
        probe.write_text("", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except OSError as exc:
        raise RuntimeError(f"打包目录不可写：{directory}") from exc
