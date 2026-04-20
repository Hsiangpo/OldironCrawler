from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

_DEFAULT_LIMIT = 8
_PROBE_EXECUTOR: ThreadPoolExecutor | None = None
_PROBE_EXECUTOR_LIMIT = 0
_PROBE_EXECUTOR_LOCK = threading.Lock()
_REQUEST_SLOT_SEMAPHORE: threading.BoundedSemaphore | None = None
_REQUEST_SLOT_LIMIT = 0
_REQUEST_SLOT_LOCK = threading.Lock()


def configure_protocol_runtime(*, probe_workers: int, request_slots: int) -> None:
    _set_probe_executor_limit(probe_workers)
    _set_request_slot_limit(request_slots)


def get_probe_executor() -> ThreadPoolExecutor:
    with _PROBE_EXECUTOR_LOCK:
        global _PROBE_EXECUTOR
        global _PROBE_EXECUTOR_LIMIT
        if _PROBE_EXECUTOR is None:
            _PROBE_EXECUTOR = ThreadPoolExecutor(max_workers=_DEFAULT_LIMIT)
            _PROBE_EXECUTOR_LIMIT = _DEFAULT_LIMIT
        return _PROBE_EXECUTOR


@contextmanager
def request_slot(*, timeout_seconds: float | None = None):
    semaphore = _get_request_slot_semaphore()
    wait_timeout = None if timeout_seconds is None else max(timeout_seconds, 0.01)
    acquired = semaphore.acquire(timeout=wait_timeout)
    if not acquired:
        raise RuntimeError("request_slot_timeout")
    try:
        yield
    finally:
        semaphore.release()


def _set_probe_executor_limit(limit: int) -> None:
    bounded = max(int(limit or 0), 1)
    old_executor: ThreadPoolExecutor | None = None
    with _PROBE_EXECUTOR_LOCK:
        global _PROBE_EXECUTOR
        global _PROBE_EXECUTOR_LIMIT
        if _PROBE_EXECUTOR is not None and _PROBE_EXECUTOR_LIMIT == bounded:
            return
        old_executor = _PROBE_EXECUTOR
        _PROBE_EXECUTOR = ThreadPoolExecutor(max_workers=bounded)
        _PROBE_EXECUTOR_LIMIT = bounded
    if old_executor is not None:
        old_executor.shutdown(wait=False, cancel_futures=False)


def _set_request_slot_limit(limit: int) -> None:
    bounded = max(int(limit or 0), 1)
    with _REQUEST_SLOT_LOCK:
        global _REQUEST_SLOT_SEMAPHORE
        global _REQUEST_SLOT_LIMIT
        if _REQUEST_SLOT_SEMAPHORE is not None and _REQUEST_SLOT_LIMIT == bounded:
            return
        _REQUEST_SLOT_SEMAPHORE = threading.BoundedSemaphore(bounded)
        _REQUEST_SLOT_LIMIT = bounded


def _get_request_slot_semaphore() -> threading.BoundedSemaphore:
    with _REQUEST_SLOT_LOCK:
        global _REQUEST_SLOT_SEMAPHORE
        global _REQUEST_SLOT_LIMIT
        if _REQUEST_SLOT_SEMAPHORE is None:
            _REQUEST_SLOT_SEMAPHORE = threading.BoundedSemaphore(_DEFAULT_LIMIT)
            _REQUEST_SLOT_LIMIT = _DEFAULT_LIMIT
        return _REQUEST_SLOT_SEMAPHORE
