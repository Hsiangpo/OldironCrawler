from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError
from oldironcrawler.extractor.service import SiteProfileService
from oldironcrawler.reporter import print_progress_heartbeat, print_site_result, write_delivery_csv
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore, SiteTask


def run_crawl_session(config: AppConfig, store: RuntimeStore, delivery_path) -> None:
    progress = store.progress()
    total = progress["total"]
    completed_count = _count_completed_sites(progress)
    heartbeat_seconds = 10.0
    llm_client = WebsiteLlmClient(
        api_key=config.llm_key,
        base_url=config.llm_base_url,
        model=config.llm_model,
        api_style=config.llm_api_style,
        reasoning_effort=config.llm_reasoning_effort,
        proxy_url=config.proxy_url,
        timeout_seconds=config.request_timeout_seconds * 3,
        concurrency_limit=config.llm_concurrency,
    )
    page_pool = PageFetchPool(
        PageFetchPoolConfig(
            worker_count=config.page_worker_count,
            per_host_limit=config.page_host_limit,
        )
    )
    learning_store = GlobalLearningStore(config.runtime_dir / "global_learning.sqlite3")
    try:
        with ThreadPoolExecutor(max_workers=config.site_concurrency) as executor:
            futures: dict[Future, SiteTask] = {}
            while True:
                while len(futures) < config.site_concurrency:
                    task = store.claim_next_site()
                    if task is None:
                        break
                    futures[executor.submit(_run_single_site, config, store, learning_store, llm_client, page_pool, task)] = task
                if not futures:
                    break
                done, _ = wait(futures.keys(), timeout=heartbeat_seconds, return_when=FIRST_COMPLETED)
                if not done:
                    progress = store.progress()
                    print_progress_heartbeat(
                        total=total,
                        done=progress["done"],
                        running=progress["running"],
                        dropped=progress["dropped"],
                        pending=progress["pending"] + progress["failed_temp"],
                    )
                    continue
                llm_error: LlmConfigurationError | None = None
                for future in done:
                    task = futures.pop(future)
                    try:
                        completed_count = _handle_future(
                            future,
                            task,
                            total,
                            completed_count,
                            store,
                            learning_store,
                            delivery_path,
                        )
                    except LlmConfigurationError as exc:
                        llm_error = llm_error or exc
                if llm_error is not None:
                    for pending_future in futures:
                        pending_future.cancel()
                    raise llm_error
    finally:
        page_pool.close()
        llm_client.close()
        learning_store.close()
    write_delivery_csv(delivery_path, store.delivery_rows())


def _count_completed_sites(progress: dict[str, int]) -> int:
    done = int(progress.get("done", 0) or 0)
    dropped = int(progress.get("dropped", 0) or 0)
    return done + dropped


def _run_single_site(
    config: AppConfig,
    store: RuntimeStore,
    learning_store: GlobalLearningStore,
    llm_client: WebsiteLlmClient,
    page_pool: PageFetchPool,
    task: SiteTask,
):
    deadline = time.monotonic() + config.total_wait_seconds
    try:
        service = SiteProfileService(config, store, learning_store, llm_client, page_pool)
        return service.process(task.id, task.website, deadline_monotonic=deadline)
    except LlmConfigurationError:
        raise
    except LlmTemporaryError:
        raise
    except ProtocolTemporaryError:
        raise
    except Exception as exc:  # noqa: BLE001
        if not _looks_temporary_error(exc):
            raise
        raise ProtocolTemporaryError(str(exc)) from exc


def _handle_future(
    future: Future,
    task: SiteTask,
    total: int,
    completed_count: int,
    store: RuntimeStore,
    learning_store: GlobalLearningStore,
    delivery_path,
) -> int:
    try:
        processed = future.result()
    except LlmConfigurationError:
        raise
    except LlmTemporaryError as exc:
        status = store.mark_failed(task.id, str(exc))
        if status == "dropped":
            stage_metrics = store.load_stage_metrics(task.id)
            completed_count += 1
            _flush_delivery_snapshot(delivery_path, store)
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    except ProtocolPermanentError as exc:
        store.mark_dropped(task.id, str(exc))
        stage_metrics = store.load_stage_metrics(task.id)
        completed_count += 1
        _flush_delivery_snapshot(delivery_path, store)
        print_site_result(
            completed_index=completed_count,
            total=total,
            website=task.website,
            company_name="",
            representative="",
            emails="",
            reason=_describe_error_reason(str(exc)),
            stage_metrics=stage_metrics,
        )
        return completed_count
    except ProtocolTemporaryError as exc:
        status = store.mark_failed(task.id, str(exc))
        if status == "dropped":
            stage_metrics = store.load_stage_metrics(task.id)
            completed_count += 1
            _flush_delivery_snapshot(delivery_path, store)
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    except Exception as exc:  # noqa: BLE001
        status = store.mark_failed(task.id, str(exc))
        if status == "dropped":
            stage_metrics = store.load_stage_metrics(task.id)
            completed_count += 1
            _flush_delivery_snapshot(delivery_path, store)
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    store.mark_done(task.id, processed.result)
    _apply_learning_feedback(learning_store, processed.learning_feedback)
    completed_count += 1
    _flush_delivery_snapshot(delivery_path, store)
    print_site_result(
        completed_index=completed_count,
        total=total,
        website=task.website,
        company_name=processed.result.company_name,
        representative=processed.result.representative,
        emails=processed.result.emails,
        reason=_describe_missing_reason(processed.result),
        stage_metrics=processed.stage_metrics,
    )
    return completed_count


def _flush_delivery_snapshot(delivery_path, store: RuntimeStore) -> None:
    try:
        write_delivery_csv(delivery_path, store.delivery_rows())
    except OSError as exc:
        print(f"写入交付文件失败：{exc}", flush=True)


def _apply_learning_feedback(learning_store: GlobalLearningStore, feedback) -> None:
    if feedback.rep_positive_tokens:
        learning_store.record_success("representative", feedback.rep_positive_tokens)
    if feedback.rep_negative_tokens:
        learning_store.record_failure("representative", feedback.rep_negative_tokens)
    if feedback.email_positive_tokens:
        learning_store.record_success("email", feedback.email_positive_tokens)
    if feedback.email_negative_tokens:
        learning_store.record_failure("email", feedback.email_negative_tokens)


def _looks_temporary_error(error: Exception) -> bool:
    text = str(error or "").lower()
    return any(
        token in text
        for token in (
            "429",
            "timeout",
            "timed out",
            "connection reset",
            "connection aborted",
            "remoteprotocolerror",
            "connection timed out",
            "resource temporarily unavailable",
            "[errno 35]",
            "unexpected_eof_while_reading",
            "eof occurred in violation of protocol",
            "500",
            "502",
            "503",
            "504",
            "overloaded",
            "capacity",
            "upstream",
        )
    )


def _describe_missing_reason(result) -> str:
    reasons: list[str] = []
    if not str(result.company_name or "").strip():
        reasons.append("官网页面里未识别到明确公司名")
    if not str(result.representative or "").strip():
        reasons.append("官网页面里未识别到负责人姓名")
    if not str(result.emails or "").strip():
        reasons.append("价值页里未命中有效邮箱")
    return "；".join(reasons)


def _describe_error_reason(error_text: str) -> str:
    text = str(error_text or "").strip()
    lowered = text.lower()
    if "http_401" in lowered:
        return "站点返回 HTTP 401，页面拒绝访问"
    if "http_404" in lowered:
        return "站点返回 HTTP 404，页面不存在"
    if "http_403" in lowered:
        return "站点返回 HTTP 403，页面禁止访问"
    if "cloudflare_challenge" in lowered:
        return "站点被 Cloudflare 风控挑战页拦截，协议抓取当前拿不到真实正文"
    if "imperva_challenge" in lowered:
        return "站点被 Imperva/Incapsula 风控挑战页拦截，协议抓取当前拿不到真实正文"
    if "http_500" in lowered:
        return "站点返回 HTTP 500，服务器内部错误"
    if "http_502" in lowered:
        return "站点返回 HTTP 502，网关错误"
    if "http_503" in lowered:
        return "站点返回 HTTP 503，服务暂时不可用"
    if "http_504" in lowered:
        return "站点返回 HTTP 504，网关超时"
    if "certificate has expired" in lowered:
        return "站点 HTTPS 证书已过期"
    if "certificate subject name" in lowered or "no alternative certificate subject name matches" in lowered:
        return "站点 HTTPS 证书域名不匹配"
    if "getaddrinfo() thread failed to start" in lowered:
        return "本地高并发 DNS 解析资源不足，当前请求未成功"
    if "resource temporarily unavailable" in lowered or "[errno 35]" in lowered:
        return "本地高并发网络资源暂时不足，当前请求未成功"
    if "request_slot_timeout" in lowered:
        return "本地协议请求并发已打满，当前请求未成功"
    if any(token in lowered for token in ("failed to connect", "couldn't connect to server", "connection refused", "no route to host", "network is unreachable", "host is down")):
        return "站点当前明显无法连通，已直接停止重试"
    if "timed out" in lowered or "timeout" in lowered:
        return "请求超时"
    if "service_temporarily_unavailable" in lowered or "llm 服务暂时不可用" in lowered:
        return "LLM 服务暂时不可用"
    if "tls connect error" in lowered or "tlsv1_alert" in lowered:
        return "站点 TLS 握手失败"
    if "site_deadline_exceeded" in lowered:
        return "单站已达到 180 秒时间上限，当前直接停止，不再重试"
    if "empty reply from server" in lowered:
        return "站点返回空响应"
    if "temporary_request" in lowered:
        return "协议请求临时失败"
    return text or "未知错误"
