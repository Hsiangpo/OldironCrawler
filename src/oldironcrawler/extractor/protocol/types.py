from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class HtmlPage:
    url: str
    html: str


@dataclass
class DiscoveryStageResult:
    urls: list[str]
    homepage_html: str


@dataclass
class SiteProtocolConfig:
    timeout_seconds: float = 10.0
    max_retries: int = 2
    proxy_url: str = ""
    capsolver_api_key: str = ""
    capsolver_api_base_url: str = "https://api.capsolver.com"
    capsolver_proxy: str = ""
    capsolver_poll_seconds: float = 3.0
    capsolver_max_wait_seconds: float = 40.0
    cloudflare_proxy_url: str = ""
    impersonate: str = "chrome110"
    max_html_chars: int = 250_000
    page_batch_timeout_seconds: float = 45.0
    deadline_monotonic: float | None = None
    common_probe_target: int = 8
    common_probe_concurrency: int = 8
    probe_worker_count: int = 16
    common_probe_patience_batches: int = 2
    common_probe_min_hits_after_patience: int = 2
    related_seed_limit: int = 2
    request_slot_limit: int = 8
    default_headers: dict[str, str] = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
