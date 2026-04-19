from __future__ import annotations

from dataclasses import dataclass
import time

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.company_rules import extract_company_name_fallback
from oldironcrawler.extractor.email_rules import collect_emails_for_pages, join_emails
from oldironcrawler.extractor.llm_client import LlmExtractionResult, WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.value_rules import (
    build_candidates,
    count_selected_families,
    extract_learning_tokens,
    merge_representative_urls,
    select_email_urls,
    select_representative_urls,
)
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore, SiteResult, SiteStageMetrics

_DISCOVERY_PRIMARY_LIMIT = 80
_DISCOVERY_SITEMAP_LIMIT = 80
_DISCOVERY_RELATED_LIMIT = 40
_DISCOVERY_FINAL_LIMIT = 160
_DISCOVERY_EMAIL_FAMILY_TARGET = 6


@dataclass
class SiteProcessingResult:
    result: SiteResult
    learning_feedback: "LearningFeedback"
    stage_metrics: SiteStageMetrics


@dataclass
class DiscoverySnapshot:
    urls: list[str]
    candidates: list
    rep_urls: list[str]
    teacher_pool: list[str]
    email_urls: list[str]


@dataclass
class LearningFeedback:
    rep_positive_tokens: list[str]
    rep_negative_tokens: list[str]
    email_positive_tokens: list[str]
    email_negative_tokens: list[str]


class SiteProfileService:
    def __init__(
        self,
        config: AppConfig,
        store: RuntimeStore,
        learning_store: GlobalLearningStore,
        llm_client: WebsiteLlmClient,
        page_pool: PageFetchPool,
    ) -> None:
        self._config = config
        self._store = store
        self._learning_store = learning_store
        self._llm = llm_client
        self._page_pool = page_pool

    def process(self, site_id: int, website: str, *, deadline_monotonic: float | None = None) -> SiteProcessingResult:
        metrics = SiteStageMetrics()
        rep_learned = self._learning_store.load_scores("representative")
        email_learned = self._learning_store.load_scores("email")
        protocol = SiteProtocolClient(
            SiteProtocolConfig(
                timeout_seconds=self._config.request_timeout_seconds,
                proxy_url=self._config.proxy_url,
                capsolver_api_key=self._config.capsolver_api_key,
                capsolver_api_base_url=self._config.capsolver_api_base_url,
                capsolver_proxy=self._config.capsolver_proxy,
                capsolver_poll_seconds=self._config.capsolver_poll_seconds,
                capsolver_max_wait_seconds=self._config.capsolver_max_wait_seconds,
                cloudflare_proxy_url=self._config.cloudflare_proxy_url,
                deadline_monotonic=deadline_monotonic,
                page_batch_timeout_seconds=max(self._config.request_timeout_seconds * 2, 45.0),
            )
        )
        try:
            discovery = self._time_call(
                metrics,
                "discover_ms",
                lambda: _discover_value_snapshot(protocol, website, rep_learned, email_learned),
            )
            discovered_urls = discovery.urls
            candidates = discovery.candidates
            rep_urls = discovery.rep_urls
            teacher_pool = discovery.teacher_pool
            email_urls = discovery.email_urls
            metrics.discovered_url_count = len(discovered_urls)
            metrics.rep_url_count = len(rep_urls)
            metrics.email_url_count = len(email_urls)
            metrics.target_url_count = len(_merge_page_targets(rep_urls, email_urls))
            self._store.update_stage_metrics(site_id, metrics)
            if len(rep_urls) < 5 and teacher_pool:
                extra_urls = self._time_call(
                    metrics,
                    "llm_pick_ms",
                    lambda: self._llm.pick_representative_urls(
                        homepage=website,
                        candidate_urls=teacher_pool,
                        target_count=5 - len(rep_urls),
                        deadline_monotonic=deadline_monotonic,
                    ),
                )
                rep_urls = merge_representative_urls(rep_urls, extra_urls, limit=5)
                metrics.rep_url_count = len(rep_urls)
            target_urls = _merge_page_targets(rep_urls, email_urls)
            metrics.target_url_count = len(target_urls)
            self._store.update_stage_metrics(site_id, metrics)
            fetched_pages = self._time_call(
                metrics,
                "fetch_pages_ms",
                lambda: protocol.fetch_pages(
                    target_urls,
                    max_workers=self._config.page_concurrency,
                    page_pool=self._page_pool,
                ),
            )
            metrics.fetched_page_count = len(fetched_pages)
            self._store.update_stage_metrics(site_id, metrics)
            page_map = {page.url: page for page in fetched_pages}
            rep_pages = [page_map[url] for url in rep_urls if url in page_map]
            email_pages = [page_map[url] for url in email_urls if url in page_map]
            llm_result = self._time_call(metrics, "llm_extract_ms", lambda: _extract_with_llm_or_empty(
                llm_client=self._llm,
                homepage=website,
                rep_pages=rep_pages,
                deadline_monotonic=deadline_monotonic,
            ))
            llm_result = _normalize_llm_result(llm_result, rep_pages)
            company_name = str(llm_result.company_name or "").strip()
            if not company_name:
                company_name = self._time_call(
                    metrics,
                    "company_rule_ms",
                    lambda: extract_company_name_fallback(
                        website,
                        [(page.url, page.html) for page in fetched_pages],
                ),
            )
            emails, email_sources = self._time_call(
                metrics,
                "email_rule_ms",
                lambda: collect_emails_for_pages(website, [(page.url, page.html) for page in email_pages]),
            )
            self._store.update_stage_metrics(site_id, metrics)
            learning_feedback = build_learning_feedback(
                representative=llm_result.representative,
                evidence_url=llm_result.evidence_url,
                rep_urls=rep_urls,
                rep_fetched_urls=[page.url for page in rep_pages],
                emails=join_emails(emails),
                email_sources=email_sources,
                email_urls=email_urls,
                email_fetched_urls=[page.url for page in email_pages],
            )
            return SiteProcessingResult(
                result=SiteResult(
                    company_name=company_name,
                    representative=llm_result.representative,
                    emails=join_emails(emails),
                    website=website,
                    evidence_url=llm_result.evidence_url,
                    evidence_quote=llm_result.evidence_quote,
                ),
                learning_feedback=learning_feedback,
                stage_metrics=metrics,
            )
        except Exception:
            self._store.update_stage_metrics(site_id, metrics)
            raise
        finally:
            protocol.close()

    def _time_call(self, metrics: SiteStageMetrics, field_name: str, func):
        started = time.monotonic()
        try:
            return func()
        finally:
            elapsed_ms = int(round((time.monotonic() - started) * 1000))
            setattr(metrics, field_name, elapsed_ms)


def _merge_page_targets(rep_urls: list[str], email_urls: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*rep_urls, *email_urls]:
        if url and url not in seen:
            seen.add(url)
            result.append(url)
    return result


def _discover_value_snapshot(
    protocol: SiteProtocolClient,
    website: str,
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
) -> DiscoverySnapshot:
    primary = protocol.discover_primary_urls(website, limit=_DISCOVERY_PRIMARY_LIMIT)
    snapshot = _build_discovery_snapshot(website, primary.urls, rep_learned, email_learned)
    if _has_enough_discovery_coverage(snapshot):
        return snapshot
    sitemap_urls = protocol.discover_sitemap_urls(website, limit=_DISCOVERY_SITEMAP_LIMIT)
    merged = _merge_unique_urls(snapshot.urls, sitemap_urls, limit=_DISCOVERY_FINAL_LIMIT)
    snapshot = _build_discovery_snapshot(website, merged, rep_learned, email_learned)
    if _has_enough_discovery_coverage(snapshot):
        return snapshot
    related_urls = protocol.discover_related_subdomain_urls(
        website,
        homepage_html=primary.homepage_html,
        direct_urls=merged,
        limit=_DISCOVERY_RELATED_LIMIT,
    )
    merged = _merge_unique_urls(merged, related_urls, limit=_DISCOVERY_FINAL_LIMIT)
    return _build_discovery_snapshot(website, merged, rep_learned, email_learned)


def _build_discovery_snapshot(
    website: str,
    discovered_urls: list[str],
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
) -> DiscoverySnapshot:
    candidates = build_candidates(website, discovered_urls, rep_learned, email_learned)
    rep_urls, teacher_pool = select_representative_urls(candidates, target_count=5)
    email_urls = select_email_urls(candidates)
    return DiscoverySnapshot(
        urls=discovered_urls,
        candidates=candidates,
        rep_urls=rep_urls,
        teacher_pool=teacher_pool,
        email_urls=email_urls,
    )


def _has_enough_discovery_coverage(snapshot: DiscoverySnapshot) -> bool:
    if len(snapshot.rep_urls) < 5:
        return False
    return count_selected_families(snapshot.candidates, snapshot.email_urls) >= _DISCOVERY_EMAIL_FAMILY_TARGET


def _merge_unique_urls(left: list[str], right: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for url in [*left, *right]:
        value = str(url or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= limit:
            break
    return result


def build_learning_feedback(
    *,
    representative: str,
    evidence_url: str,
    rep_urls: list[str],
    rep_fetched_urls: list[str],
    emails: str,
    email_sources: list[str],
    email_urls: list[str],
    email_fetched_urls: list[str],
) -> LearningFeedback:
    rep_positive_tokens = _collect_positive_rep_tokens(representative, evidence_url, rep_fetched_urls)
    rep_negative_tokens = _collect_contrastive_negative_tokens(rep_positive_tokens, rep_fetched_urls)
    email_positive_tokens = _collect_positive_email_tokens(emails, email_sources, email_fetched_urls)
    email_negative_tokens = _collect_contrastive_negative_tokens(email_positive_tokens, email_fetched_urls)
    rep_negative_tokens = [token for token in rep_negative_tokens if token not in rep_positive_tokens]
    email_negative_tokens = [token for token in email_negative_tokens if token not in email_positive_tokens]
    return LearningFeedback(
        rep_positive_tokens=rep_positive_tokens,
        rep_negative_tokens=rep_negative_tokens,
        email_positive_tokens=email_positive_tokens,
        email_negative_tokens=email_negative_tokens,
    )


def _merge_learning_tokens(urls: list[str]) -> list[str]:
    tokens: list[str] = []
    for url in urls:
        for token in extract_learning_tokens(url):
            if token not in tokens:
                tokens.append(token)
    return tokens


def _collect_contrastive_negative_tokens(positive_tokens: list[str], candidate_urls: list[str]) -> list[str]:
    if not positive_tokens:
        return []
    negative_tokens = _merge_learning_tokens(candidate_urls)
    return [token for token in negative_tokens if token not in positive_tokens]


def _collect_positive_rep_tokens(representative: str, evidence_url: str, rep_fetched_urls: list[str]) -> list[str]:
    if not representative or not evidence_url:
        return []
    if evidence_url not in rep_fetched_urls:
        return []
    return extract_learning_tokens(evidence_url)


def _collect_positive_email_tokens(emails: str, email_sources: list[str], email_fetched_urls: list[str]) -> list[str]:
    if not emails:
        return []
    kept_sources = [url for url in email_sources if url in email_fetched_urls]
    return _merge_learning_tokens(kept_sources)


def _extract_with_llm_or_empty(
    *,
    llm_client: WebsiteLlmClient,
    homepage: str,
    rep_pages: list,
    deadline_monotonic: float | None,
) -> LlmExtractionResult:
    if not rep_pages:
        return LlmExtractionResult(company_name="", representative="", evidence_url="", evidence_quote="")
    return llm_client.extract_company_and_representative(
        homepage=homepage,
        pages=[{"url": page.url, "html": page.html} for page in rep_pages],
        deadline_monotonic=deadline_monotonic,
    )


def _normalize_llm_result(llm_result: LlmExtractionResult, rep_pages: list) -> LlmExtractionResult:
    available_urls = {page.url for page in rep_pages}
    evidence_url = str(llm_result.evidence_url or "").strip()
    if evidence_url and evidence_url not in available_urls:
        evidence_url = ""
    representative = str(llm_result.representative or "").strip() if evidence_url else ""
    evidence_quote = str(llm_result.evidence_quote or "").strip() if representative else ""
    return LlmExtractionResult(
        company_name=str(llm_result.company_name or "").strip() if rep_pages else "",
        representative=representative,
        evidence_url=evidence_url,
        evidence_quote=evidence_quote,
    )
