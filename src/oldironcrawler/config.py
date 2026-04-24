from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


def _config_str(values: Mapping[str, str], name: str, default: str = "") -> str:
    raw = values.get(name, default)
    return str(raw or "").strip()


def _config_int(values: Mapping[str, str], name: str, default: int) -> int:
    raw = _config_str(values, name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _config_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw = _config_str(values, name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _local_proxy_is_ready(proxy_url: str) -> bool:
    text = str(proxy_url or "").strip().lower()
    if not text:
        return False
    if "127.0.0.1:" not in text and "localhost:" not in text:
        return True
    host = "127.0.0.1" if "127.0.0.1:" in text else "localhost"
    try:
        port = int(text.rsplit(":", 1)[-1].split("/", 1)[0])
    except ValueError:
        return False
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _resolve_proxy_url(values: Mapping[str, str]) -> str:
    explicit = _config_str(values, "PROXY_URL")
    if explicit:
        return explicit if _local_proxy_is_ready(explicit) else ""
    return ""


def _load_config_values(project_root: Path) -> dict[str, str]:
    file_values = dotenv_values(project_root / ".env")
    values = {
        str(name): str(value)
        for name, value in file_values.items()
        if name and value is not None
    }
    for name, value in os.environ.items():
        key = str(name or "").strip()
        if not key or value is None:
            continue
        values[key] = str(value)
    return values


def resolve_websites_dir(project_root: Path) -> Path:
    preferred = project_root / "websites"
    if _directory_is_usable(preferred):
        return preferred
    return project_root / "websites_runtime"


def _directory_is_usable(directory: Path) -> bool:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".write_probe"
        probe.write_text("", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def read_saved_llm_key(project_root: Path) -> str:
    values = _load_config_values(project_root)
    return _config_str(values, "LLM_KEY") or _config_str(values, "LLM_API_KEY")


def persist_llm_key(project_root: Path, llm_key: str) -> None:
    env_path = project_root / ".env"
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    saved_key = _sanitize_env_value(llm_key)
    updated: list[str] = []
    seen_key = False
    seen_api_key = False
    for line in lines:
        stripped = str(line or "").strip()
        if stripped.startswith("LLM_KEY="):
            updated.append(f"LLM_KEY={saved_key}")
            seen_key = True
            continue
        if stripped.startswith("LLM_API_KEY="):
            updated.append(f"LLM_API_KEY={saved_key}")
            seen_api_key = True
            continue
        updated.append(line)
    if not seen_key:
        updated.append(f"LLM_KEY={saved_key}")
    if not seen_api_key:
        updated.append(f"LLM_API_KEY={saved_key}")
    env_path.write_text("\n".join(updated).rstrip() + "\n", encoding="utf-8")


def _sanitize_env_value(value: object) -> str:
    return str(value or "").replace("\r", "").replace("\n", "").strip()


@dataclass
class AppConfig:
    project_root: Path
    websites_dir: Path
    runtime_dir: Path
    delivery_dir: Path
    llm_base_url: str
    llm_key: str
    llm_model: str
    llm_reasoning_effort: str
    llm_api_style: str
    llm_concurrency: int
    capsolver_api_key: str
    capsolver_api_base_url: str
    capsolver_proxy: str
    capsolver_poll_seconds: float
    capsolver_max_wait_seconds: float
    cloudflare_proxy_url: str
    proxy_url: str
    site_concurrency: int
    page_concurrency: int
    page_worker_count: int
    page_host_limit: int
    rep_page_limit: int
    email_page_soft_limit: int
    email_page_hard_limit: int
    page_total_hard_limit: int
    email_stop_same_domain_count: int
    request_timeout_seconds: float
    total_wait_seconds: float

    @classmethod
    def load(cls, project_root: Path, llm_key_override: str | None = None) -> "AppConfig":
        values = _load_config_values(project_root)
        resolved_llm_key = str(llm_key_override or "").strip() or _config_str(values, "LLM_KEY") or _config_str(
            values,
            "LLM_API_KEY",
        )
        return cls(
            project_root=project_root,
            websites_dir=resolve_websites_dir(project_root),
            runtime_dir=project_root / "output" / "runtime",
            delivery_dir=project_root / "output",
            llm_base_url=_config_str(values, "LLM_BASE_URL"),
            llm_key=resolved_llm_key,
            llm_model=_config_str(values, "LLM_MODEL", "gpt-5.4-mini"),
            llm_reasoning_effort=_config_str(values, "LLM_REASONING_EFFORT", "low"),
            llm_api_style=_config_str(values, "LLM_API_STYLE", "responses").lower(),
            llm_concurrency=max(_config_int(values, "LLM_CONCURRENCY", 32), 1),
            capsolver_api_key=_config_str(values, "CAPSOLVER_API_KEY"),
            capsolver_api_base_url=_config_str(values, "CAPSOLVER_API_BASE_URL", "https://api.capsolver.com"),
            capsolver_proxy=_config_str(values, "CAPSOLVER_PROXY"),
            capsolver_poll_seconds=max(_config_float(values, "CAPSOLVER_POLL_SECONDS", 3.0), 1.0),
            capsolver_max_wait_seconds=max(_config_float(values, "CAPSOLVER_MAX_WAIT_SECONDS", 40.0), 5.0),
            cloudflare_proxy_url=_config_str(values, "CLOUDFLARE_PROXY_URL"),
            proxy_url=_resolve_proxy_url(values),
            site_concurrency=max(_config_int(values, "SITE_CONCURRENCY", 32), 1),
            page_concurrency=max(_config_int(values, "PAGE_CONCURRENCY", 32), 1),
            page_worker_count=max(_config_int(values, "PAGE_WORKER_COUNT", 32), 1),
            page_host_limit=max(_config_int(values, "PAGE_HOST_LIMIT", 32), 1),
            rep_page_limit=max(_config_int(values, "REP_PAGE_LIMIT", 5), 1),
            email_page_soft_limit=max(_config_int(values, "EMAIL_PAGE_SOFT_LIMIT", 8), 0),
            email_page_hard_limit=max(_config_int(values, "EMAIL_PAGE_HARD_LIMIT", 16), 0),
            page_total_hard_limit=max(_config_int(values, "PAGE_TOTAL_HARD_LIMIT", 20), 1),
            email_stop_same_domain_count=max(_config_int(values, "EMAIL_STOP_SAME_DOMAIN_COUNT", 2), 1),
            request_timeout_seconds=max(_config_float(values, "REQUEST_TIMEOUT_SECONDS", 10.0), 1.0),
            total_wait_seconds=max(_config_float(values, "TOTAL_WAIT_SECONDS", 180.0), 30.0),
        )

    def ensure_directories(self) -> None:
        self.websites_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.delivery_dir.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        if not self.llm_base_url:
            raise RuntimeError("缺少 LLM_BASE_URL，请检查 .env。")
        if not self.llm_key:
            raise RuntimeError("缺少 LLM_KEY，请检查 .env。")
        if not self.llm_model:
            raise RuntimeError("缺少 LLM_MODEL，请检查 .env。")
