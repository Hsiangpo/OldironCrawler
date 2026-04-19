from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
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
    port = int(text.rsplit(":", 1)[-1].split("/", 1)[0])
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _resolve_proxy_url() -> str:
    explicit = str(os.getenv("PROXY_URL", "") or "").strip()
    if explicit:
        return explicit if _local_proxy_is_ready(explicit) else ""
    env_proxy = str(
        os.getenv("HTTPS_PROXY")
        or os.getenv("https_proxy")
        or os.getenv("HTTP_PROXY")
        or os.getenv("http_proxy")
        or ""
    ).strip()
    if env_proxy:
        return env_proxy if _local_proxy_is_ready(env_proxy) else ""
    fallback = "http://127.0.0.1:7897"
    return fallback if _local_proxy_is_ready(fallback) else ""


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
    def load(cls, project_root: Path) -> "AppConfig":
        load_dotenv(project_root / ".env")
        return cls(
            project_root=project_root,
            websites_dir=project_root / "websites",
            runtime_dir=project_root / "output" / "runtime",
            delivery_dir=project_root / "output" / "delivery",
            llm_base_url=str(os.getenv("LLM_BASE_URL", "") or "").strip(),
            llm_key=str(os.getenv("LLM_KEY") or os.getenv("LLM_API_KEY") or "").strip(),
            llm_model=str(os.getenv("LLM_MODEL", "gpt-5.4-mini") or "").strip(),
            llm_reasoning_effort=str(os.getenv("LLM_REASONING_EFFORT", "low") or "").strip(),
            llm_api_style=str(os.getenv("LLM_API_STYLE", "responses") or "").strip().lower(),
            llm_concurrency=max(_env_int("LLM_CONCURRENCY", 52), 1),
            capsolver_api_key=str(os.getenv("CAPSOLVER_API_KEY", "") or "").strip(),
            capsolver_api_base_url=str(os.getenv("CAPSOLVER_API_BASE_URL", "https://api.capsolver.com") or "").strip(),
            capsolver_proxy=str(os.getenv("CAPSOLVER_PROXY", "") or "").strip(),
            capsolver_poll_seconds=max(_env_float("CAPSOLVER_POLL_SECONDS", 3.0), 1.0),
            capsolver_max_wait_seconds=max(_env_float("CAPSOLVER_MAX_WAIT_SECONDS", 40.0), 5.0),
            cloudflare_proxy_url=str(os.getenv("CLOUDFLARE_PROXY_URL", "") or "").strip(),
            proxy_url=_resolve_proxy_url(),
            site_concurrency=max(_env_int("SITE_CONCURRENCY", 52), 1),
            page_concurrency=max(_env_int("PAGE_CONCURRENCY", 52), 1),
            page_worker_count=max(_env_int("PAGE_WORKER_COUNT", 52), 1),
            page_host_limit=max(_env_int("PAGE_HOST_LIMIT", 52), 1),
            rep_page_limit=max(_env_int("REP_PAGE_LIMIT", 5), 1),
            email_page_soft_limit=max(_env_int("EMAIL_PAGE_SOFT_LIMIT", 8), 0),
            email_page_hard_limit=max(_env_int("EMAIL_PAGE_HARD_LIMIT", 16), 0),
            page_total_hard_limit=max(_env_int("PAGE_TOTAL_HARD_LIMIT", 20), 1),
            email_stop_same_domain_count=max(_env_int("EMAIL_STOP_SAME_DOMAIN_COUNT", 2), 1),
            request_timeout_seconds=max(_env_float("REQUEST_TIMEOUT_SECONDS", 10.0), 1.0),
            total_wait_seconds=max(_env_float("TOTAL_WAIT_SECONDS", 180.0), 30.0),
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
