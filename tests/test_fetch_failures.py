from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol_client import ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig


def test_page_fetch_pool_raises_when_all_fetches_fail() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=4, per_host_limit=2))

    def fetch_one(_url: str):
        raise ProtocolTemporaryError("temporary_request: https://example.com")

    try:
        pool.fetch_pages(
            urls=["https://example.com/a", "https://example.com/b"],
            fetch_one=fetch_one,
            deadline_monotonic=time.monotonic() + 1.0,
        )
        raise AssertionError("expected ProtocolTemporaryError")
    except ProtocolTemporaryError as exc:
        assert "temporary_request" in str(exc)
    finally:
        pool.close()


def test_page_fetch_pool_keeps_partial_success() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=4, per_host_limit=2))

    def fetch_one(url: str):
        if url.endswith("/bad"):
            raise ProtocolTemporaryError("temporary_request: https://example.com/bad")
        return url

    try:
        pages = pool.fetch_pages(
            urls=["https://example.com/good", "https://example.com/bad"],
            fetch_one=fetch_one,
            deadline_monotonic=time.monotonic() + 1.0,
        )
    finally:
        pool.close()

    assert pages == ["https://example.com/good"]


def test_protocol_client_fetch_pages_raises_when_all_optional_fetches_fail() -> None:
    class FailingClient(SiteProtocolClient):
        def _fetch_page_optional(self, url: str, *, timeout_seconds: float | None = None):
            raise ProtocolTemporaryError(f"temporary_request: {url}")

    client = FailingClient(SiteProtocolConfig(page_batch_timeout_seconds=0.2))

    try:
        client.fetch_pages(["https://example.com/a", "https://example.com/b"], max_workers=2)
        raise AssertionError("expected ProtocolTemporaryError")
    except ProtocolTemporaryError as exc:
        assert "temporary_request" in str(exc)
    finally:
        client.close()


def test_page_fetch_pool_raises_timeout_when_deadline_expires_without_pages() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=8, per_host_limit=2))

    def fetch_one(_url: str):
        time.sleep(0.2)
        return "late-page"

    try:
        pool.fetch_pages(
            urls=[f"https://example.com/{index}" for index in range(6)],
            fetch_one=fetch_one,
            deadline_monotonic=time.monotonic() + 0.05,
        )
        raise AssertionError("expected TimeoutError")
    except TimeoutError as exc:
        assert "page_batch_timeout" in str(exc)
    finally:
        pool.close()
