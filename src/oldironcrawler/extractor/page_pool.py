from __future__ import annotations

import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class PageFetchPoolConfig:
    worker_count: int
    per_host_limit: int


class PageFetchPool:
    def __init__(self, config: PageFetchPoolConfig) -> None:
        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=max(config.worker_count, 1))
        self._lock = threading.Lock()
        self._host_limits: dict[str, threading.BoundedSemaphore] = {}
        self._submit_slots = threading.BoundedSemaphore(max(config.worker_count, 1))

    def fetch_pages(
        self,
        *,
        urls: list[str],
        fetch_one,
        deadline_monotonic: float,
    ) -> list:
        futures: dict[Future, str] = {}
        pages_by_url: dict[str, object] = {}
        last_error: Exception | None = None
        timed_out = False
        inflight_limit = self._inflight_limit(urls)
        url_index = 0
        try:
            url_index = self._fill_futures(
                futures=futures,
                urls=urls,
                fetch_one=fetch_one,
                deadline_monotonic=deadline_monotonic,
                inflight_limit=inflight_limit,
                start_index=url_index,
            )
            while futures:
                remaining = deadline_monotonic - _now_monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                done, _ = wait(futures.keys(), timeout=remaining, return_when=FIRST_COMPLETED)
                if not done:
                    timed_out = True
                    break
                for future in done:
                    url = futures.pop(future, "")
                    try:
                        page = future.result()
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        continue
                    if page is not None and url:
                        pages_by_url[url] = page
                url_index = self._fill_futures(
                    futures=futures,
                    urls=urls,
                    fetch_one=fetch_one,
                    deadline_monotonic=deadline_monotonic,
                    inflight_limit=inflight_limit,
                    start_index=url_index,
                )
        finally:
            for future in futures:
                if not future.done():
                    future.cancel()
        if not pages_by_url and last_error is not None:
            raise last_error
        if not pages_by_url and timed_out:
            raise TimeoutError("page_batch_timeout")
        return [pages_by_url[url] for url in urls if url in pages_by_url]

    def _inflight_limit(self, urls: list[str]) -> int:
        if not urls:
            return 0
        base_limit = max(min(self._config.worker_count, 24), max(self._config.per_host_limit, 1))
        unique_hosts = {self._host_key(url) for url in urls}
        host_cap = max(len(unique_hosts), 1) * max(self._config.per_host_limit, 1)
        return min(len(urls), base_limit, host_cap)

    def _fill_futures(
        self,
        *,
        futures: dict[Future, str],
        urls: list[str],
        fetch_one,
        deadline_monotonic: float,
        inflight_limit: int,
        start_index: int,
    ) -> int:
        index = start_index
        while index < len(urls) and len(futures) < inflight_limit:
            remaining = deadline_monotonic - _now_monotonic()
            if remaining <= 0:
                break
            if not self._submit_slots.acquire(timeout=remaining):
                break
            url = urls[index]
            try:
                future = self._executor.submit(self._run_fetch, fetch_one, url)
            except Exception:  # noqa: BLE001
                self._submit_slots.release()
                break
            future.add_done_callback(lambda _future: self._submit_slots.release())
            futures[future] = url
            index += 1
        return index

    def _run_fetch(self, fetch_one, url: str):
        host_semaphore = self._get_host_semaphore(url)
        with host_semaphore:
            return fetch_one(url)

    def _get_host_semaphore(self, url: str) -> threading.BoundedSemaphore:
        key = self._host_key(url)
        with self._lock:
            semaphore = self._host_limits.get(key)
            if semaphore is None:
                semaphore = threading.BoundedSemaphore(max(self._config.per_host_limit, 1))
                self._host_limits[key] = semaphore
            return semaphore

    def _host_key(self, url: str) -> str:
        host = (urlparse(url).netloc or "").strip().lower()
        return host or "__unknown__"

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


def _now_monotonic() -> float:
    import time

    return time.monotonic()
