from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urlparse

_DISPATCH_QUANTUM_SECONDS = 0.01


@dataclass
class PageFetchPoolConfig:
    worker_count: int
    per_host_limit: int


@dataclass
class _FetchBatch:
    urls: list[str]
    fetch_one: object
    deadline_monotonic: float
    inflight_limit: int
    pending_urls: deque[str] = field(default_factory=deque)
    pages_by_url: dict[str, object] = field(default_factory=dict)
    inflight_count: int = 0
    last_error: Exception | None = None
    closed: bool = False


class PageFetchPool:
    def __init__(self, config: PageFetchPoolConfig) -> None:
        self._config = config
        self._executor = ThreadPoolExecutor(max_workers=max(config.worker_count, 1))
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._host_limits: dict[str, threading.BoundedSemaphore] = {}
        self._active_batches: list[_FetchBatch] = []
        self._available_slots = max(config.worker_count, 1)
        self._dispatch_cursor = 0

    def fetch_pages(
        self,
        *,
        urls: list[str],
        fetch_one,
        deadline_monotonic: float,
    ) -> list:
        inflight_limit = self._inflight_limit(urls)
        if inflight_limit <= 0:
            return []
        batch = _FetchBatch(
            urls=list(urls),
            fetch_one=fetch_one,
            deadline_monotonic=deadline_monotonic,
            inflight_limit=inflight_limit,
            pending_urls=deque(urls),
        )
        timed_out = False
        with self._condition:
            self._active_batches.append(batch)
            try:
                while True:
                    self._dispatch_locked()
                    if self._is_batch_complete_locked(batch):
                        break
                    remaining = batch.deadline_monotonic - _now_monotonic()
                    if remaining <= 0:
                        timed_out = True
                        batch.closed = True
                        break
                    self._condition.wait(timeout=min(remaining, _DISPATCH_QUANTUM_SECONDS))
            finally:
                batch.closed = True
                self._remove_batch_locked(batch)
                self._dispatch_locked()
                self._condition.notify_all()
        if not batch.pages_by_url and batch.last_error is not None:
            raise batch.last_error
        if not batch.pages_by_url and timed_out:
            raise TimeoutError("page_batch_timeout")
        return [batch.pages_by_url[url] for url in batch.urls if url in batch.pages_by_url]

    def _dispatch_locked(self) -> None:
        if self._available_slots <= 0 or not self._active_batches:
            return
        while self._available_slots > 0:
            submitted = False
            total = len(self._active_batches)
            for _ in range(total):
                batch = self._pick_next_batch_locked()
                if batch is None:
                    break
                dispatch_item = self._take_dispatch_item_locked(batch)
                if dispatch_item is None:
                    continue
                url, host_semaphore = dispatch_item
                batch.inflight_count += 1
                self._available_slots -= 1
                try:
                    future = self._executor.submit(batch.fetch_one, url)
                except Exception:  # noqa: BLE001
                    host_semaphore.release()
                    batch.inflight_count -= 1
                    self._available_slots += 1
                    batch.pending_urls.appendleft(url)
                    break
                future.add_done_callback(
                    lambda completed, batch=batch, url=url, host_semaphore=host_semaphore: self._handle_future_completion(
                        batch,
                        url,
                        host_semaphore,
                        completed,
                    )
                )
                submitted = True
                break
            if not submitted:
                break

    def _pick_next_batch_locked(self) -> _FetchBatch | None:
        total = len(self._active_batches)
        if total <= 0:
            return None
        for offset in range(total):
            index = (self._dispatch_cursor + offset) % total
            batch = self._active_batches[index]
            if self._can_submit_locked(batch):
                self._dispatch_cursor = (index + 1) % max(len(self._active_batches), 1)
                return batch
        return None

    def _can_submit_locked(self, batch: _FetchBatch) -> bool:
        if batch.closed or batch.inflight_count >= batch.inflight_limit:
            return False
        if not batch.pending_urls:
            return False
        return batch.deadline_monotonic > _now_monotonic()

    def _take_dispatch_item_locked(self, batch: _FetchBatch) -> tuple[str, threading.BoundedSemaphore] | None:
        total_urls = len(batch.pending_urls)
        for _ in range(total_urls):
            url = batch.pending_urls[0]
            host_semaphore = self._get_host_semaphore_locked(url)
            if host_semaphore.acquire(blocking=False):
                batch.pending_urls.popleft()
                return url, host_semaphore
            batch.pending_urls.rotate(-1)
        return None

    def _handle_future_completion(
        self,
        batch: _FetchBatch,
        url: str,
        host_semaphore: threading.BoundedSemaphore,
        future: Future,
    ) -> None:
        with self._condition:
            host_semaphore.release()
            batch.inflight_count = max(batch.inflight_count - 1, 0)
            self._available_slots += 1
            try:
                page = future.result()
            except Exception as exc:  # noqa: BLE001
                if not batch.closed:
                    batch.last_error = exc
            else:
                if not batch.closed and page is not None:
                    batch.pages_by_url[url] = page
            self._dispatch_locked()
            self._condition.notify_all()

    def _remove_batch_locked(self, batch: _FetchBatch) -> None:
        try:
            index = self._active_batches.index(batch)
        except ValueError:
            return
        self._active_batches.pop(index)
        if not self._active_batches:
            self._dispatch_cursor = 0
            return
        if index < self._dispatch_cursor:
            self._dispatch_cursor -= 1
        self._dispatch_cursor %= len(self._active_batches)

    def _is_batch_complete_locked(self, batch: _FetchBatch) -> bool:
        return not batch.pending_urls and batch.inflight_count <= 0

    def _inflight_limit(self, urls: list[str]) -> int:
        if not urls:
            return 0
        base_limit = max(min(self._config.worker_count, 24), max(self._config.per_host_limit, 1))
        unique_hosts = {self._host_key(url) for url in urls}
        host_cap = max(len(unique_hosts), 1) * max(self._config.per_host_limit, 1)
        return min(len(urls), base_limit, host_cap)

    def _get_host_semaphore_locked(self, url: str) -> threading.BoundedSemaphore:
        key = self._host_key(url)
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
    return time.monotonic()
