from __future__ import annotations

import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class LlmIngressNode:
    id: str
    label: str
    base_url: str
    health_url: str


@dataclass(frozen=True)
class LlmIngressProbe:
    ok: bool
    first_event_ms: float | None = None
    total_ms: float | None = None
    health_ms: float | None = None
    status_code: int = 0
    error: str = ""


@dataclass(frozen=True)
class LlmIngressResult:
    node: LlmIngressNode
    samples: list[LlmIngressProbe]

    @property
    def success_rate(self) -> float:
        if not self.samples:
            return 0.0
        return len([sample for sample in self.samples if sample.ok]) / len(self.samples)

    @property
    def first_event_ms(self) -> float:
        return _median([sample.first_event_ms for sample in self.samples if sample.ok])

    @property
    def total_ms(self) -> float:
        return _median([sample.total_ms for sample in self.samples if sample.ok])

    @property
    def error(self) -> str:
        for sample in self.samples:
            if sample.error:
                return sample.error
        return ""


@dataclass(frozen=True)
class LlmIngressSelection:
    best: LlmIngressResult
    results: list[LlmIngressResult]


@dataclass(frozen=True)
class LlmIngressBenchmarkOptions:
    api_key: str
    model: str
    reasoning_effort: str
    timeout_seconds: float
    proxy_url: str = ""


BenchmarkProbe = Callable[[LlmIngressNode, LlmIngressBenchmarkOptions], LlmIngressProbe]

GPTEAM_INGRESS_NODES = (
    LlmIngressNode("jp-direct", "日本主机直连", "https://api.gpteamservices.com/v1", "https://api.gpteamservices.com/api/health"),
    LlmIngressNode("jp-split", "日本入口", "https://api-jp.gpteamservices.com/v1", "https://api-jp.gpteamservices.com/api/health"),
    LlmIngressNode("hk-split", "香港入口", "https://api-hk.gpteamservices.com/v1", "https://api-hk.gpteamservices.com/api/health"),
    LlmIngressNode("us-split", "美国入口", "https://api-us.gpteamservices.com/v1", "https://api-us.gpteamservices.com/api/health"),
)


def resolve_llm_ingress_nodes(*, primary_base_url: str, extra_base_urls: list[str]) -> list[LlmIngressNode]:
    configured_urls = [_normalize_base_url(primary_base_url), *[_normalize_base_url(url) for url in extra_base_urls]]
    configured_urls = [url for url in configured_urls if url]
    nodes: list[LlmIngressNode] = []
    if any(_is_gpteam_base_url(url) for url in configured_urls):
        nodes.extend(GPTEAM_INGRESS_NODES)
    for index, base_url in enumerate(configured_urls, start=1):
        if not _node_exists(nodes, base_url):
            nodes.append(_custom_node(base_url, index))
    return _dedupe_nodes(nodes)


def select_best_llm_ingress(
    *,
    api_key: str,
    model: str,
    reasoning_effort: str,
    nodes: list[LlmIngressNode],
    rounds: int,
    timeout_seconds: float,
    proxy_url: str = "",
    benchmark_probe: BenchmarkProbe | None = None,
) -> LlmIngressSelection:
    if not nodes:
        raise RuntimeError("LLM API 入口测速失败：没有可用入口")
    options = LlmIngressBenchmarkOptions(
        api_key=api_key,
        model=model,
        reasoning_effort=reasoning_effort,
        timeout_seconds=max(float(timeout_seconds), 1.0),
        proxy_url=str(proxy_url or "").strip(),
    )
    probe = benchmark_probe or benchmark_llm_ingress_once
    bounded_rounds = min(max(int(rounds), 1), 5)
    results = _benchmark_nodes_parallel(nodes, options, bounded_rounds, probe)
    ordered = sorted(results, key=_result_sort_key)
    if ordered and ordered[0].success_rate > 0:
        return LlmIngressSelection(best=ordered[0], results=ordered)
    detail = "；".join(f"{result.node.label}: {result.error or '请求失败'}" for result in ordered)
    raise RuntimeError(f"LLM API 入口测速失败：{detail or '全部入口不可用'}")


def benchmark_llm_ingress_once(node: LlmIngressNode, options: LlmIngressBenchmarkOptions) -> LlmIngressProbe:
    started = time.perf_counter()
    deadline = started + max(float(options.timeout_seconds), 1.0)
    try:
        with httpx.Client(**_build_http_client_kwargs(options)) as client:
            health_started = time.perf_counter()
            health = client.get(
                node.health_url,
                headers={"User-Agent": "OldIronCrawler/ingress-benchmark"},
                timeout=_remaining_timeout(deadline),
            )
            health.read()
            health_ms = _elapsed_ms(health_started)
            if not health.is_success:
                return LlmIngressProbe(False, total_ms=_elapsed_ms(started), health_ms=health_ms, status_code=health.status_code, error=f"health HTTP {health.status_code}")
            models_error = _probe_models(client, node, options, deadline)
            if models_error:
                return LlmIngressProbe(False, total_ms=_elapsed_ms(started), health_ms=health_ms, error=models_error)
            stream = _probe_response_stream(client, node, options, started, deadline)
            return LlmIngressProbe(
                ok=stream.ok,
                first_event_ms=stream.first_event_ms,
                total_ms=_elapsed_ms(started),
                health_ms=health_ms,
                status_code=stream.status_code,
                error=stream.error,
            )
    except Exception as exc:  # noqa: BLE001
        return LlmIngressProbe(False, total_ms=_elapsed_ms(started), error=str(exc)[:240])


def format_ingress_selection(selection: LlmIngressSelection) -> str:
    best = selection.best
    return (
        f"已选择 LLM API 入口：{best.node.label} "
        f"({best.node.base_url})，成功率={best.success_rate:.0%}，"
        f"首包={_format_ms(best.first_event_ms)}，总耗时={_format_ms(best.total_ms)}"
    )


def _benchmark_nodes_parallel(
    nodes: list[LlmIngressNode],
    options: LlmIngressBenchmarkOptions,
    rounds: int,
    probe: BenchmarkProbe,
) -> list[LlmIngressResult]:
    executor = ThreadPoolExecutor(max_workers=max(len(nodes), 1))
    futures = {executor.submit(_benchmark_node_rounds, node, options, rounds, probe): node for node in nodes}
    results: dict[str, LlmIngressResult] = {}
    timed_out = False
    try:
        timeout_seconds = max(options.timeout_seconds * rounds + 2.0, 3.0)
        for future in as_completed(futures, timeout=timeout_seconds):
            node = futures[future]
            try:
                results[node.id] = future.result()
            except Exception as exc:  # noqa: BLE001
                results[node.id] = LlmIngressResult(node=node, samples=[LlmIngressProbe(False, error=str(exc)[:240])])
    except FuturesTimeoutError:
        timed_out = True
    finally:
        executor.shutdown(wait=not timed_out, cancel_futures=True)
    for future, node in futures.items():
        if node.id not in results:
            future.cancel()
            results[node.id] = LlmIngressResult(node=node, samples=[LlmIngressProbe(False, error="入口测速超时")])
    return list(results.values())


def _benchmark_node_rounds(
    node: LlmIngressNode,
    options: LlmIngressBenchmarkOptions,
    rounds: int,
    probe: BenchmarkProbe,
) -> LlmIngressResult:
    samples = [probe(node, options) for _ in range(rounds)]
    return LlmIngressResult(node=node, samples=samples)


def _probe_models(client: httpx.Client, node: LlmIngressNode, options: LlmIngressBenchmarkOptions, deadline: float) -> str:
    response = client.get(
        f"{node.base_url.rstrip('/')}/models",
        headers={"Authorization": f"Bearer {options.api_key}", "User-Agent": "OldIronCrawler/ingress-benchmark"},
        timeout=_remaining_timeout(deadline),
    )
    if response.is_success:
        return ""
    return f"/v1/models HTTP {response.status_code}: {_read_error_detail(response)}"


@dataclass(frozen=True)
class _StreamProbe:
    ok: bool
    first_event_ms: float | None
    status_code: int
    error: str


def _probe_response_stream(
    client: httpx.Client,
    node: LlmIngressNode,
    options: LlmIngressBenchmarkOptions,
    started: float,
    deadline: float,
) -> _StreamProbe:
    payload = {
        "model": options.model,
        "stream": True,
        "input": "请只回复一句话：节点测速完成。",
        "max_output_tokens": 64,
        "metadata": {"oldironcrawler_ingress_probe": "1"},
    }
    if options.reasoning_effort:
        payload["reasoning"] = {"effort": options.reasoning_effort}
    headers = {
        "Authorization": f"Bearer {options.api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "User-Agent": "OldIronCrawler/ingress-benchmark",
    }
    with client.stream(
        "POST",
        f"{node.base_url.rstrip('/')}/responses",
        headers=headers,
        json=payload,
        timeout=_remaining_timeout(deadline),
    ) as response:
        first_event_ms: float | None = None
        body_parts: list[str] = []
        for raw_line in response.iter_lines():
            if time.perf_counter() >= deadline:
                return _StreamProbe(False, first_event_ms, response.status_code, "入口测速超时")
            line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else str(raw_line or "")
            if line.strip():
                body_parts.append(line)
            if first_event_ms is None and line.strip().startswith("data:"):
                first_event_ms = _elapsed_ms(started)
        body_text = "\n".join(body_parts)
        if not response.is_success:
            return _StreamProbe(False, first_event_ms, response.status_code, f"stream HTTP {response.status_code}: {body_text[:240]}")
        event_error = _read_sse_error(body_text)
        if event_error:
            return _StreamProbe(False, first_event_ms, response.status_code, event_error)
        if first_event_ms is None:
            return _StreamProbe(False, None, response.status_code, "stream empty")
        return _StreamProbe(True, first_event_ms, response.status_code, "")


def _build_http_client_kwargs(options: LlmIngressBenchmarkOptions) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "timeout": max(float(options.timeout_seconds), 1.0),
        "follow_redirects": True,
        "trust_env": False,
    }
    if options.proxy_url:
        kwargs["proxy"] = options.proxy_url
    return kwargs


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.perf_counter()
    if remaining <= 0:
        raise TimeoutError("入口测速超时")
    return max(remaining, 0.05)


def _read_sse_error(body_text: str) -> str:
    for line in body_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("data:"):
            continue
        payload_text = stripped[5:].strip()
        if not payload_text or payload_text == "[DONE]":
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        if str(payload.get("type", "") or "").lower() == "error":
            return str(payload.get("error") or payload)[:240]
    return ""


def _read_error_detail(response: httpx.Response) -> str:
    try:
        data = response.json()
    except Exception:  # noqa: BLE001
        return str(response.text or "")[:240]
    error = data.get("error") if isinstance(data, dict) else None
    if isinstance(error, dict):
        return str(error.get("message") or error.get("code") or error)[:240]
    return str(data)[:240]


def _result_sort_key(result: LlmIngressResult) -> tuple[float, float, float, str]:
    first = result.first_event_ms if math.isfinite(result.first_event_ms) else 999_999.0
    total = result.total_ms if math.isfinite(result.total_ms) else 999_999.0
    return (-result.success_rate, first + total, first, result.node.id)


def _normalize_base_url(value: str) -> str:
    text = str(value or "").strip().rstrip("/")
    return text if text else ""


def _is_gpteam_base_url(base_url: str) -> bool:
    host = urlparse(base_url).hostname or ""
    return host.endswith("gpteamservices.com")


def _custom_node(base_url: str, index: int) -> LlmIngressNode:
    root = base_url.removesuffix("/v1").rstrip("/")
    return LlmIngressNode(f"custom-{index}", f"自定义入口 {index}", base_url, f"{root}/api/health")


def _node_exists(nodes: list[LlmIngressNode], base_url: str) -> bool:
    return any(_normalize_base_url(node.base_url) == base_url for node in nodes)


def _dedupe_nodes(nodes: list[LlmIngressNode]) -> list[LlmIngressNode]:
    result: list[LlmIngressNode] = []
    for node in nodes:
        if not _node_exists(result, _normalize_base_url(node.base_url)):
            result.append(node)
    return result


def _median(values: list[float | None]) -> float:
    safe = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not safe:
        return math.nan
    middle = len(safe) // 2
    if len(safe) % 2:
        return safe[middle]
    return (safe[middle - 1] + safe[middle]) / 2


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def _format_ms(value: float) -> str:
    if not math.isfinite(value):
        return "-"
    if value < 1000:
        return f"{round(value)}ms"
    return f"{value / 1000:.2f}s"
