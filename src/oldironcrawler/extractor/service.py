from __future__ import annotations

from dataclasses import dataclass
import time

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.company_rules import clean_company_name_candidate, extract_company_name_fallback
from oldironcrawler.extractor.email_rules import analyze_email_set, collect_emails_for_pages, join_emails
from oldironcrawler.extractor.llm_client import LlmExtractionResult, WebsiteLlmClient
from oldironcrawler.extractor.page_pool import PageFetchPool
from oldironcrawler.extractor.protocol_client import HtmlPage, ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.value_rules import (
    build_fetch_plan,
    build_candidates,
    canonicalize_target_url,
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
_DISCOVERY_REP_STRONG_TOKENS = {
    "board",
    "chair",
    "chairman",
    "chief",
    "director",
    "executive",
    "founder",
    "governance",
    "impressum",
    "imprint",
    "leadership",
    "management",
    "officers",
    "owner",
    "partner",
    "partners",
    "president",
    "principal",
    "solicitor",
    "team",
}


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
    homepage_html: str = ""


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
        protocol = SiteProtocolClient(_build_site_protocol_config(self._config, deadline_monotonic))
        try:
            discovery = self._time_call(
                metrics,
                "discover_ms",
                lambda: _discover_value_snapshot(
                    protocol,
                    website,
                    rep_learned,
                    email_learned,
                    rep_target_count=_get_rep_page_limit(self._config),
                ),
            )
            rep_urls = self._resolve_representative_urls(discovery, website, metrics, deadline_monotonic)
            fetch_plan = _plan_fetch_targets(self._config, website, rep_urls, discovery.email_urls)
            metrics.discovered_url_count = len(discovery.urls)
            metrics.rep_url_count = len(fetch_plan["rep_urls"])
            metrics.email_url_count = len(fetch_plan["email_primary_urls"]) + len(fetch_plan["email_overflow_urls"])
            metrics.target_url_count = len(fetch_plan["all_primary_urls"]) + len(fetch_plan["email_overflow_urls"])
            self._store.update_stage_metrics(site_id, metrics)
            page_map, rep_pages, llm_result = self._collect_budgeted_pages(
                protocol,
                website,
                fetch_plan,
                discovery.homepage_html,
                metrics,
                deadline_monotonic,
            )
            metrics.fetched_page_count = len(page_map)
            self._store.update_stage_metrics(site_id, metrics)
            fetched_pages = list(page_map.values())
            company_name = clean_company_name_candidate(str(llm_result.company_name or "").strip())
            if not company_name:
                company_name = clean_company_name_candidate(self._time_call(
                    metrics,
                    "company_rule_ms",
                    lambda: extract_company_name_fallback(
                        website,
                        [(page.url, page.html) for page in fetched_pages],
                ),
            ))
            email_rule_pages = _collect_email_rule_pages(page_map, fetch_plan)
            emails, email_sources = self._time_call(
                metrics,
                "email_rule_ms",
                lambda: collect_emails_for_pages(website, email_rule_pages),
            )
            self._store.update_stage_metrics(site_id, metrics)
            learning_feedback = build_learning_feedback(
                representative=llm_result.representative,
                evidence_url=llm_result.evidence_url,
                rep_urls=fetch_plan["rep_urls"],
                rep_fetched_urls=[page.url for page in rep_pages],
                emails=join_emails(emails),
                email_sources=list(email_sources.keys()),
                email_urls=[*fetch_plan["email_primary_urls"], *fetch_plan["email_overflow_urls"]],
                email_fetched_urls=[url for url, _html in email_rule_pages],
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

    def _resolve_representative_urls(
        self,
        discovery: DiscoverySnapshot,
        website: str,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> list[str]:
        rep_urls = list(discovery.rep_urls)
        missing_count = max(_get_rep_page_limit(self._config) - len(rep_urls), 0)
        if missing_count <= 0 or not discovery.teacher_pool:
            return rep_urls
        extra_urls = self._time_call(
            metrics,
            "llm_pick_ms",
            lambda: self._llm.pick_representative_urls(
                homepage=website,
                candidate_urls=discovery.teacher_pool,
                target_count=missing_count,
                deadline_monotonic=deadline_monotonic,
            ),
        )
        return merge_representative_urls(rep_urls, extra_urls, limit=_get_rep_page_limit(self._config))

    def _collect_budgeted_pages(
        self,
        protocol: SiteProtocolClient,
        website: str,
        fetch_plan: dict[str, list[str]],
        homepage_html: str,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> tuple[dict[str, object], list, LlmExtractionResult]:
        primary_fetch_ms = 0
        overflow_fetch_ms = 0
        page_map: dict[str, object] = {}
        try:
            reused_primary_pages = _build_reused_primary_pages(website, fetch_plan, homepage_html)
            _merge_pages_into_map(page_map, reused_primary_pages)
            primary_pages, primary_fetch_ms = _fetch_primary_pages(
                protocol,
                _filter_network_primary_urls(fetch_plan["all_primary_urls"], reused_primary_pages),
                page_concurrency=self._config.page_concurrency,
                page_pool=self._page_pool,
            )
            _merge_pages_into_map(page_map, primary_pages)
            rep_pages = _select_pages_from_map(page_map, fetch_plan["rep_urls"])
            llm_result = self._extract_primary_representative(
                website,
                rep_pages,
                metrics,
                deadline_monotonic,
            )
            overflow_pages, overflow_fetch_ms = self._fetch_email_overflow_pages_if_needed(
                protocol,
                website,
                fetch_plan,
                page_map,
                llm_result,
            )
            _merge_pages_into_map(page_map, overflow_pages)
            return page_map, rep_pages, llm_result
        finally:
            metrics.fetch_pages_ms = primary_fetch_ms + overflow_fetch_ms

    def _extract_primary_representative(
        self,
        website: str,
        rep_pages: list,
        metrics: SiteStageMetrics,
        deadline_monotonic: float | None,
    ) -> LlmExtractionResult:
        llm_result = self._time_call(
            metrics,
            "llm_extract_ms",
            lambda: _extract_with_llm_or_empty(
                llm_client=self._llm,
                homepage=website,
                rep_pages=rep_pages,
                deadline_monotonic=deadline_monotonic,
            ),
        )
        return _normalize_llm_result(llm_result, rep_pages)

    def _fetch_email_overflow_pages_if_needed(
        self,
        protocol: SiteProtocolClient,
        website: str,
        fetch_plan: dict[str, list[str]],
        page_map: dict[str, object],
        llm_result: LlmExtractionResult,
    ) -> tuple[list, int]:
        if not _should_fetch_email_overflow_after_primary_fetch(
            website,
            llm_result,
            _collect_primary_email_rule_pages(page_map, fetch_plan),
            fetch_plan["email_overflow_urls"],
            email_stop_same_domain_count=_get_email_stop_same_domain_count(self._config),
        ):
            return [], 0
        return _fetch_email_overflow_pages(
            protocol,
            fetch_plan,
            page_concurrency=self._config.page_concurrency,
            page_pool=self._page_pool,
        )

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


def _build_site_protocol_config(config: AppConfig, deadline_monotonic: float | None) -> SiteProtocolConfig:
    return SiteProtocolConfig(
        timeout_seconds=config.request_timeout_seconds,
        proxy_url=config.proxy_url,
        capsolver_api_key=config.capsolver_api_key,
        capsolver_api_base_url=config.capsolver_api_base_url,
        capsolver_proxy=config.capsolver_proxy,
        capsolver_poll_seconds=config.capsolver_poll_seconds,
        capsolver_max_wait_seconds=config.capsolver_max_wait_seconds,
        cloudflare_proxy_url=config.cloudflare_proxy_url,
        deadline_monotonic=deadline_monotonic,
        page_batch_timeout_seconds=max(
            getattr(config, "total_wait_seconds", config.request_timeout_seconds * 2),
            config.request_timeout_seconds * 2,
        ),
        common_probe_concurrency=max(int(config.page_concurrency or 1), 1),
        request_slot_limit=max(int(config.page_concurrency or 1), 1),
    )


def _discover_value_snapshot(
    protocol: SiteProtocolClient,
    website: str,
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
    *,
    rep_target_count: int = 5,
) -> DiscoverySnapshot:
    primary = protocol.discover_primary_urls(website, limit=_DISCOVERY_PRIMARY_LIMIT)
    snapshot = _build_discovery_snapshot(
        website,
        primary.urls,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
    )
    if _has_enough_discovery_coverage(snapshot, rep_target_count=rep_target_count):
        return snapshot
    sitemap_urls = protocol.discover_sitemap_urls(website, limit=_DISCOVERY_SITEMAP_LIMIT)
    merged = _merge_unique_urls(snapshot.urls, sitemap_urls, limit=_DISCOVERY_FINAL_LIMIT)
    snapshot = _build_discovery_snapshot(
        website,
        merged,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
    )
    if _has_enough_discovery_coverage(snapshot, rep_target_count=rep_target_count):
        return snapshot
    related_urls = protocol.discover_related_subdomain_urls(
        website,
        homepage_html=primary.homepage_html,
        direct_urls=merged,
        limit=_DISCOVERY_RELATED_LIMIT,
    )
    merged = _merge_unique_urls(merged, related_urls, limit=_DISCOVERY_FINAL_LIMIT)
    return _build_discovery_snapshot(
        website,
        merged,
        rep_learned,
        email_learned,
        rep_target_count=rep_target_count,
        homepage_html=primary.homepage_html,
    )


def _build_discovery_snapshot(
    website: str,
    discovered_urls: list[str],
    rep_learned: dict[str, int],
    email_learned: dict[str, int],
    *,
    rep_target_count: int = 5,
    homepage_html: str = "",
) -> DiscoverySnapshot:
    candidates = build_candidates(website, discovered_urls, rep_learned, email_learned)
    rep_urls, teacher_pool = select_representative_urls(candidates, target_count=rep_target_count)
    email_urls = select_email_urls(candidates)
    return DiscoverySnapshot(
        urls=discovered_urls,
        candidates=candidates,
        rep_urls=rep_urls,
        teacher_pool=teacher_pool,
        email_urls=email_urls,
        homepage_html=homepage_html,
    )


def _has_enough_discovery_coverage(snapshot: DiscoverySnapshot, *, rep_target_count: int = 5) -> bool:
    if len(snapshot.rep_urls) < rep_target_count:
        return False
    if count_selected_families(snapshot.candidates, snapshot.email_urls) < _DISCOVERY_EMAIL_FAMILY_TARGET:
        return False
    return _has_high_confidence_representative_coverage(snapshot)


def _has_high_confidence_representative_coverage(snapshot: DiscoverySnapshot) -> bool:
    candidate_map = {candidate.url: candidate for candidate in snapshot.candidates}
    for url in snapshot.rep_urls:
        candidate = candidate_map.get(url)
        if candidate is None:
            continue
        if candidate.is_person_detail_page:
            return True
        if any(token in _DISCOVERY_REP_STRONG_TOKENS for token in candidate.tokens):
            return True
    return False


def _plan_fetch_targets(config: AppConfig, website: str, rep_urls: list[str], email_urls: list[str]) -> dict[str, list[str]]:
    return build_fetch_plan(
        website,
        rep_urls,
        email_urls,
        rep_limit=_get_rep_page_limit(config),
        email_soft_limit=_get_email_page_soft_limit(config),
        email_hard_limit=_get_email_page_hard_limit(config),
        total_hard_limit=_get_page_total_hard_limit(config),
    )


def _fetch_primary_pages(
    protocol: SiteProtocolClient,
    primary_urls: list[str],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
) -> tuple[list, int]:
    if not primary_urls:
        return [], 0
    return _fetch_pages_with_elapsed(
        protocol.fetch_pages,
        primary_urls,
        page_concurrency=page_concurrency,
        page_pool=page_pool,
    )


def _fetch_email_overflow_pages(
    protocol: SiteProtocolClient,
    fetch_plan: dict[str, list[str]],
    *,
    page_concurrency: int,
    page_pool: PageFetchPool | None,
) -> tuple[list, int]:
    if not fetch_plan["email_overflow_urls"]:
        return [], 0
    return _fetch_pages_with_elapsed(
        protocol.fetch_pages,
        fetch_plan["email_overflow_urls"],
        page_concurrency=page_concurrency,
        page_pool=page_pool,
    )


def _should_fetch_email_overflow_after_primary_fetch(
    website: str,
    llm_result: LlmExtractionResult,
    primary_email_rule_pages: list[tuple[str, str]],
    email_overflow_urls: list[str],
    *,
    email_stop_same_domain_count: int,
) -> bool:
    if not email_overflow_urls:
        return False
    if not str(llm_result.representative or "").strip():
        return True
    emails, _page_hits = collect_emails_for_pages(website, primary_email_rule_pages)
    same_domain_count = len(analyze_email_set(website, emails).same_domain_emails)
    return same_domain_count < email_stop_same_domain_count


def _merge_pages_into_map(page_map: dict[str, object], pages: list) -> None:
    for page in pages:
        page_map[page.url] = page


def _select_pages_from_map(page_map: dict[str, object], urls: list[str]) -> list:
    return [page_map[url] for url in urls if url in page_map]


def _collect_email_rule_pages(page_map: dict[str, object], fetch_plan: dict[str, list[str]]) -> list[tuple[str, str]]:
    email_pages = _select_pages_from_map(
        page_map,
        [*fetch_plan["email_primary_urls"], *fetch_plan["email_overflow_urls"]],
    )
    rep_pages = _select_pages_from_map(page_map, fetch_plan["rep_urls"])
    return _merge_email_rule_pages(email_pages, rep_pages)


def _collect_primary_email_rule_pages(page_map: dict[str, object], fetch_plan: dict[str, list[str]]) -> list[tuple[str, str]]:
    email_primary_pages = _select_pages_from_map(page_map, fetch_plan["email_primary_urls"])
    rep_pages = _select_pages_from_map(page_map, fetch_plan["rep_urls"])
    return _merge_email_rule_pages(email_primary_pages, rep_pages)


def _merge_email_rule_pages(*page_groups: list) -> list[tuple[str, str]]:
    merged_pages: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    for pages in page_groups:
        for page in pages:
            if page.url in seen_urls:
                continue
            seen_urls.add(page.url)
            merged_pages.append((page.url, page.html))
    return merged_pages


def _build_reused_primary_pages(website: str, fetch_plan: dict[str, list[str]], homepage_html: str) -> list[HtmlPage]:
    if not homepage_html:
        return []
    if website not in fetch_plan["all_primary_urls"]:
        return []
    return [HtmlPage(url=website, html=homepage_html)]


def _filter_network_primary_urls(primary_urls: list[str], reused_pages: list[HtmlPage]) -> list[str]:
    reused_urls = {page.url for page in reused_pages}
    if not reused_urls:
        return list(primary_urls)
    return [url for url in primary_urls if url not in reused_urls]


def _fetch_pages_with_elapsed(fetch_func, urls: list[str], *, page_concurrency: int, page_pool: PageFetchPool | None) -> tuple[list, int]:
    started = time.monotonic()
    pages = fetch_func(
        urls,
        max_workers=page_concurrency,
        page_pool=page_pool,
    )
    elapsed_ms = int(round((time.monotonic() - started) * 1000))
    return pages, elapsed_ms


def _get_rep_page_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "rep_page_limit", 5) or 5), 1)


def _get_email_page_soft_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "email_page_soft_limit", 8) or 8), 0)


def _get_email_page_hard_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "email_page_hard_limit", 16) or 16), 0)


def _get_page_total_hard_limit(config: AppConfig) -> int:
    return max(int(getattr(config, "page_total_hard_limit", 20) or 20), 1)


def _get_email_stop_same_domain_count(config: AppConfig) -> int:
    return max(int(getattr(config, "email_stop_same_domain_count", 2) or 2), 1)


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
    rep_negative_tokens = _collect_failed_rep_negative_tokens(
        representative,
        evidence_url,
        rep_positive_tokens,
        rep_fetched_urls,
    )
    email_positive_tokens = _collect_positive_email_tokens(emails, email_sources, email_fetched_urls)
    email_negative_tokens = _collect_failed_email_negative_tokens(
        emails,
        email_positive_tokens,
        email_fetched_urls,
    )
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


def _collect_failed_rep_negative_tokens(
    representative: str,
    evidence_url: str,
    positive_tokens: list[str],
    rep_fetched_urls: list[str],
) -> list[str]:
    return []


def _collect_failed_email_negative_tokens(
    emails: str,
    positive_tokens: list[str],
    email_fetched_urls: list[str],
) -> list[str]:
    return []


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
    available_urls = {
        canonicalize_target_url(page.url): page.url
        for page in rep_pages
        if str(page.url or "").strip()
    }
    raw_evidence_url = str(llm_result.evidence_url or "").strip()
    evidence_url = available_urls.get(canonicalize_target_url(raw_evidence_url), "")
    representative = str(llm_result.representative or "").strip() if evidence_url else ""
    evidence_quote = str(llm_result.evidence_quote or "").strip() if representative else ""
    return LlmExtractionResult(
        company_name=str(llm_result.company_name or "").strip() if rep_pages else "",
        representative=representative,
        evidence_url=evidence_url,
        evidence_quote=evidence_quote,
    )
