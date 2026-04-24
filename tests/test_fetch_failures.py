from __future__ import annotations

import sys
import time
import threading
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


def test_page_fetch_pool_keeps_later_batch_progress_under_slot_contention() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=4, per_host_limit=4))
    outcomes: list[tuple[str, object]] = []
    lock = threading.Lock()

    def slow_fetch(_url: str):
        time.sleep(0.35)
        return "slow-page"

    def fast_fetch(url: str):
        return url

    def run_batch(label: str, deadline_seconds: float, fetch_one) -> None:
        try:
            result = pool.fetch_pages(
                urls=[f"https://example.com/{label}/{index}" for index in range(4)],
                fetch_one=fetch_one,
                deadline_monotonic=time.monotonic() + deadline_seconds,
            )
            outcome: object = ("ok", result)
        except Exception as exc:  # noqa: BLE001
            outcome = exc
        with lock:
            outcomes.append((label, outcome))

    slow_thread = threading.Thread(target=run_batch, args=("slow", 1.0, slow_fetch))
    fast_thread = threading.Thread(target=run_batch, args=("fast", 0.5, fast_fetch))

    slow_thread.start()
    time.sleep(0.03)
    fast_thread.start()
    slow_thread.join()
    fast_thread.join()
    pool.close()

    fast_outcome = next(outcome for label, outcome in outcomes if label == "fast")

    assert fast_outcome == (
        "ok",
        [f"https://example.com/fast/{index}" for index in range(4)],
    )


def test_page_fetch_pool_does_not_occupy_workers_while_waiting_for_same_host_limit() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=2, per_host_limit=1))
    lock = threading.Lock()
    completed: list[str] = []

    def fetch_one(url: str):
        if "host-a" in url:
            time.sleep(0.25)
        with lock:
            completed.append(url)
        return url

    def run_batch(urls: list[str]) -> list[str]:
        return pool.fetch_pages(
            urls=urls,
            fetch_one=fetch_one,
            deadline_monotonic=time.monotonic() + 1.0,
        )

    host_a_thread = threading.Thread(
        target=run_batch,
        args=([f"https://host-a.example.com/{index}" for index in range(2)],),
    )
    host_b_result: list[str] = []

    def run_host_b() -> None:
        host_b_result.extend(
            run_batch(["https://host-b.example.com/fast"])
        )

    host_b_thread = threading.Thread(target=run_host_b)

    started = time.monotonic()
    host_a_thread.start()
    time.sleep(0.03)
    host_b_thread.start()
    host_b_thread.join()
    elapsed = time.monotonic() - started
    host_a_thread.join()
    pool.close()

    assert host_b_result == ["https://host-b.example.com/fast"]
    assert elapsed < 0.2
