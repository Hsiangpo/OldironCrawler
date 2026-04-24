from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError
from oldironcrawler.extractor.service import SiteProfileService
from oldironcrawler.reporter import print_progress_heartbeat, print_site_result, write_delivery_csv
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore, SiteTask

_DELIVERY_FLUSH_EVERY_SITES = 25
_DELIVERY_FLUSH_EVERY_SECONDS = 5.0


def run_crawl_session(config: AppConfig, store: RuntimeStore, delivery_path) -> None:
    progress = store.progress()
    total = progress["total"]
    completed_count = _count_completed_sites(progress)
    heartbeat_seconds = 10.0
    delivery_writer = _DeliverySnapshotWriter(Path(delivery_path), store)
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
    executor = ThreadPoolExecutor(max_workers=config.site_concurrency)
    wait_for_executor = True
    try:
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
                delivery_writer.flush_if_due()
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
            completed_count, llm_error = _process_done_futures(
                done=done,
                futures=futures,
                total=total,
                completed_count=completed_count,
                store=store,
                learning_store=learning_store,
                delivery_writer=delivery_writer,
            )
            if llm_error is not None:
                completed_count, _ = _process_done_futures(
                    done=_collect_ready_futures(futures),
                    futures=futures,
                    total=total,
                    completed_count=completed_count,
                    store=store,
                    learning_store=learning_store,
                    delivery_writer=delivery_writer,
                )
                for pending_future in list(futures):
                    pending_future.cancel()
                wait_for_executor = False
                page_pool.close()
                llm_client.close()
                raise llm_error
    finally:
        shutdown = getattr(executor, "shutdown", None)
        if callable(shutdown):
            shutdown(wait=wait_for_executor, cancel_futures=True)
        page_pool.close()
        llm_client.close()
        learning_store.close()
        delivery_writer.force_flush()


def _collect_ready_futures(futures: dict[Future, SiteTask]) -> set[Future]:
    if not futures:
        return set()
    done, _ = wait(futures.keys(), timeout=0, return_when=FIRST_COMPLETED)
    return set(done)


def _process_done_futures(
    *,
    done,
    futures: dict[Future, SiteTask],
    total: int,
    completed_count: int,
    store: RuntimeStore,
    learning_store: GlobalLearningStore,
    delivery_writer,
) -> tuple[int, LlmConfigurationError | None]:
    llm_error: LlmConfigurationError | None = None
    for future in done:
        task = futures.pop(future, None)
        if task is None:
            continue
        try:
            completed_count = _handle_future(
                future,
                task,
                total,
                completed_count,
                store,
                learning_store,
                delivery_writer,
            )
        except LlmConfigurationError as exc:
            llm_error = llm_error or exc
    return completed_count, llm_error


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
        if _is_site_deadline_error(exc):
            raise ProtocolPermanentError("site_deadline_exceeded") from exc
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
    delivery_writer,
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
            delivery_writer.note_completion()
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                phones="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    except ProtocolPermanentError as exc:
        stage_metrics = store.load_stage_metrics(task.id)
        if _should_retry_protocol_deadline(exc, stage_metrics):
            status = store.mark_failed(task.id, str(exc))
            if status != "dropped":
                return completed_count
        else:
            store.mark_dropped(task.id, str(exc))
        completed_count += 1
        delivery_writer.note_completion()
        print_site_result(
            completed_index=completed_count,
            total=total,
            website=task.website,
            company_name="",
            representative="",
            emails="",
            phones="",
            reason=_describe_error_reason(str(exc)),
            stage_metrics=stage_metrics,
        )
        return completed_count
    except ProtocolTemporaryError as exc:
        status = store.mark_failed(task.id, str(exc))
        if status == "dropped":
            stage_metrics = store.load_stage_metrics(task.id)
            completed_count += 1
            delivery_writer.note_completion()
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                phones="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    except Exception as exc:  # noqa: BLE001
        status = store.mark_failed(task.id, str(exc))
        if status == "dropped":
            stage_metrics = store.load_stage_metrics(task.id)
            completed_count += 1
            delivery_writer.note_completion()
            print_site_result(
                completed_index=completed_count,
                total=total,
                website=task.website,
                company_name="",
                representative="",
                emails="",
                phones="",
                reason=_describe_error_reason(str(exc)),
                stage_metrics=stage_metrics,
            )
        return completed_count
    store.mark_done(task.id, processed.result)
    _apply_learning_feedback(learning_store, processed.learning_feedback)
    completed_count += 1
    delivery_writer.note_completion()
    print_site_result(
        completed_index=completed_count,
        total=total,
        website=task.website,
        company_name=processed.result.company_name,
        representative=processed.result.representative,
        emails=processed.result.emails,
        phones=processed.result.phones,
        reason=_describe_missing_reason(processed.result),
        stage_metrics=processed.stage_metrics,
    )
    return completed_count


class _DeliverySnapshotWriter:
    def __init__(self, delivery_path: Path, store: RuntimeStore) -> None:
        self._delivery_path = delivery_path
        self._store = store
        self._dirty = False
        self._completed_since_flush = 0
        self._last_flush_monotonic = time.monotonic()

    def note_completion(self) -> None:
        self._dirty = True
        self._completed_since_flush += 1
        self.flush_if_due()

    def flush_if_due(self) -> None:
        if not self._dirty:
            return
        now = time.monotonic()
        if (
            self._completed_since_flush < _DELIVERY_FLUSH_EVERY_SITES
            and now - self._last_flush_monotonic < _DELIVERY_FLUSH_EVERY_SECONDS
        ):
            return
        self._flush(now)

    def force_flush(self) -> None:
        if self._dirty:
            self._flush(time.monotonic())

    def _flush(self, now_monotonic: float) -> None:
        if not _flush_delivery_snapshot(self._delivery_path, self._store):
            return
        self._dirty = False
        self._completed_since_flush = 0
        self._last_flush_monotonic = now_monotonic


def _flush_delivery_snapshot(delivery_path, store: RuntimeStore) -> bool:
    try:
        write_delivery_csv(delivery_path, store.delivery_rows())
        return True
    except OSError as exc:
        print(f"写入交付文件失败：{exc}", flush=True)
        return False


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
            "llm_queue_timeout",
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


def _is_site_deadline_error(error: Exception) -> bool:
    return "site_deadline_exceeded" in str(error or "").lower()


def _should_retry_protocol_deadline(error: Exception, stage_metrics) -> bool:
    if not _is_site_deadline_error(error):
        return False
    return int(getattr(stage_metrics, "fetched_page_count", 0) or 0) <= 0


def _describe_missing_reason(result) -> str:
    reasons: list[str] = []
    if not str(result.company_name or "").strip():
        reasons.append("官网页面里未识别到明确公司名")
    if not str(result.representative or "").strip():
        reasons.append("官网页面里未识别到负责人姓名")
    if not str(result.emails or "").strip():
        reasons.append("价值页里未命中有效邮箱")
    if not str(result.phones or "").strip():
        reasons.append("价值页里未命中有效电话")
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
    if "sgcaptcha_challenge" in lowered:
        return "站点被安全验证页拦截，当前抓到的是验证码/挑战页，不是真实正文"
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
        return "单站已达到时间上限，本轮没有拿到足够结果"
    if "page_batch_timeout" in lowered:
        return "目标页批量抓取超时，这一轮子页在时限内没有拿到可用正文"
    if "empty_page_batch" in lowered:
        return "目标页请求后没有拿到任何可用正文，可能被风控拦截、返回错误页，或站点只回空白页"
    if "empty reply from server" in lowered:
        return "站点返回空响应"
    if "temporary_request" in lowered:
        return "协议请求临时失败"
    return text or "未知错误"
