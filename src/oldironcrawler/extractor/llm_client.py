from __future__ import annotations

import json
import importlib.util
import os
import random
import re
import time
import threading
from dataclasses import dataclass
from typing import Any

import httpx
from bs4 import BeautifulSoup
from markdownify import MarkdownConverter
from openai import OpenAI

from oldironcrawler.llm_errors import LlmIntervention, classify_llm_exception


_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
_DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_META_HEAD_SIGNAL_NAMES = {
    "description",
    "og:description",
    "og:title",
    "twitter:description",
    "twitter:title",
}
_LLM_SEMAPHORE: threading.Semaphore | None = None
_LLM_SEMAPHORE_LOCK = threading.Lock()
_LLM_SEMAPHORE_LIMIT = 0
_SITE_DEADLINE_SAFETY_SECONDS = 8.0
_REPRESENTATIVE_CONTENT_HINTS = (
    "about", "accountant", "bio", "board", "chief", "co-founder", "contact",
    "director", "executive", "founder", "general partner", "impressum", "imprint",
    "leadership", "management", "officer", "our story", "owner", "partner",
    "people", "president", "principal", "profile", "referral", "solicitor",
    "team", "vice-chancellor", "who we are",
)
_REPRESENTATIVE_DROP_HINTS = (
    "analytics purposes",
    "accept all cookies",
    "already working at",
    "candidate connect login",
    "career site by teamtailor",
    "cookie policy",
    "cookie preferences",
    "decline all non-necessary cookies",
    "employee login",
    "log in as employee",
    "log in to connect",
    "manage cookies",
    "select which cookies you accept",
    "strictly necessary",
    "this website uses cookies",
    "withdraw and manage your consent",
)
_REPRESENTATIVE_REJECT_EXACT = {
    "advisor", "advisors", "ceo", "chair", "chairman", "cofounder", "co-founder",
    "complaints", "complaintsmanager", "contact", "coo", "coordinator", "cto",
    "customer", "customerservice", "customersupport", "department", "director",
    "employee", "employees", "executive", "executives", "finance", "financeadvisors",
    "financeadvisor", "founder", "head", "leadership", "manager", "managers",
    "marketing", "media", "mediacoordinator", "member", "members", "office",
    "officer", "officers", "owner", "owners", "partner", "partners", "person",
    "people", "president", "representative", "sales", "salesteam", "service",
    "services", "staff", "support", "team", "teams",
}
_REPRESENTATIVE_PAGE_BUDGET = 180_000
_REPRESENTATIVE_KEEP_FULL_PAGES = 2
_REPRESENTATIVE_HEAD_LINE_LIMIT = 24
_REPRESENTATIVE_TAIL_LINE_LIMIT = 16


@dataclass
class LlmExtractionResult:
    company_name: str
    representative: str
    evidence_url: str
    evidence_quote: str


class LlmConfigurationError(RuntimeError):
    def __init__(self, message: str, *, failure: LlmIntervention | None = None) -> None:
        super().__init__(message)
        self.failure = failure


class LlmTemporaryError(RuntimeError):
    def __init__(self, message: str, *, failure: LlmIntervention | None = None) -> None:
        super().__init__(message)
        self.failure = failure


class WebsiteLlmClient:
    _MAX_PAGE_CHARS = 80_000
    _MAX_PROMPT_CHARS = 250_000

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        model: str,
        api_style: str,
        reasoning_effort: str,
        proxy_url: str,
        timeout_seconds: float,
        concurrency_limit: int,
    ) -> None:
        client_kwargs: dict[str, Any] = {
            "timeout": timeout_seconds,
            "follow_redirects": True,
            "http2": _http2_is_available(),
            "headers": dict(_DEFAULT_HEADERS),
            "limits": httpx.Limits(
                max_connections=max(concurrency_limit * 2, 128),
                max_keepalive_connections=max(concurrency_limit, 64),
                keepalive_expiry=30.0,
            ),
            "trust_env": False,
        }
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        verify_mode = str(os.getenv("LLM_TLS_VERIFY", "auto") or "auto").strip().lower()
        if verify_mode in {"0", "false", "no", "off"}:
            client_kwargs["verify"] = False
        self._http_client = httpx.Client(**client_kwargs)
        self._client = OpenAI(
            api_key=api_key,
            base_url=base_url or None,
            timeout=timeout_seconds,
            http_client=self._http_client,
        )
        self._api_key = api_key
        self._base_url = str(base_url or "").strip()
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._api_style = str(api_style or "responses").strip().lower()
        self._reasoning_effort = reasoning_effort
        self._set_global_concurrency_limit(concurrency_limit)

    def close(self) -> None:
        try:
            self._http_client.close()
        except Exception:  # noqa: BLE001
            return None

    def ping(self) -> None:
        payload = self._call_json('只返回 JSON：{"ok":true}')
        if payload.get("ok") is not True:
            raise RuntimeError("llm_ping_invalid_response")

    def pick_website_column(
        self,
        *,
        source_name: str,
        columns: list[dict[str, Any]],
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        prompt = (
            "你是表格网站列识别器。\n"
            "目标：在一个网站导入表里，找出真正代表公司官网主站的那一列。\n"
            "强规则：\n"
            "1. 只能选择一个列 index。\n"
            "2. 优先公司官网主站、主页、domain、company website、official website。\n"
            "3. 明确排除 linkedin/facebook/instagram/twitter/youtube/tiktok 等社媒列。\n"
            "4. 明确排除 email、phone、address、notes、comments 这类列。\n"
            "5. 如果有多个 URL 类列，优先公司官网，不要选社媒、客服工单、招聘平台、地图页。\n"
            '返回 JSON：{"selected_index":0,"confidence":"high|medium|low","reason":""}\n\n'
            f"文件名: {source_name}\n"
            f"列摘要(JSON): {json.dumps(columns, ensure_ascii=False)}"
        )
        return self._call_json(prompt, deadline_monotonic=deadline_monotonic)

    def pick_representative_urls(
        self,
        *,
        homepage: str,
        candidate_urls: list[str],
        target_count: int,
        deadline_monotonic: float | None = None,
    ) -> list[str]:
        prompt = (
            "你是企业官网页面选择器。\n"
            "目标：只为了抽取公司名和最高负责人，选择最有价值的网页链接。\n"
            "优先页面：about, company, team, leadership, management, board, governance, officers, executive, people。\n"
            "强规则：如果候选里同时存在 team / leadership / management / board / governance / about 这类静态栏目页，绝对优先这些页面。\n"
            "不要选择新闻稿、博客文章、采访、活动页、奖项页、公告页、长标题文章页作为负责人来源页，除非完全没有静态领导页。\n"
            "新闻页中的引语、Chair 发言、Founder 被提及，不等于最高负责人来源页。\n"
            "不要考虑邮箱。\n"
            "只能从给定链接里选，不能编造。\n"
            '返回 JSON：{"selected_urls":["..."]}\n\n'
            f"首页: {homepage}\n"
            f"最多返回: {max(int(target_count), 1)}\n"
            f"候选链接(JSON): {json.dumps(candidate_urls, ensure_ascii=False)}"
        )
        data = self._call_json(prompt, deadline_monotonic=deadline_monotonic)
        selected = data.get("selected_urls")
        if not isinstance(selected, list):
            return []
        allowed = set(candidate_urls)
        result: list[str] = []
        for item in selected:
            url = str(item or "").strip()
            if url and url in allowed and url not in result:
                result.append(url)
        return result[: max(int(target_count), 1)]

    def extract_company_and_representative(
        self,
        *,
        homepage: str,
        pages: list[dict[str, str]],
        deadline_monotonic: float | None = None,
    ) -> LlmExtractionResult:
        safe_pages = _prepare_representative_pages(self._convert_pages_to_markdown(pages))
        prompt = (
            "你是企业官网联系人抽取器。\n"
            "目标：从给定网页内容中抽取公司名和公司最高负责人单人。\n\n"
            "规则：\n"
            "1. company_name：如果官网明确显示法定名称就用法定名称，否则用品牌名。若 imprint/impressum/legal notice 页面写了法定主体，则优先采用该法定主体，不要被首页品牌名带偏。\n"
            "2. representative：只返回一个最高负责人，只接受真人姓名，不接受部门、团队名、品牌名、职位名本身。\n"
            "3. 优先级从高到低参考：CEO > Managing Director > President > Chief Executive > Vice-Chancellor > Managing Partner > Director。\n"
            "4. 如果静态领导页里没有 CEO / Managing Director / President / Chief Executive，但明确列出了 Founder / Co-Founder / Owner，可以返回 Founder / Co-Founder / Owner 中最核心的一人。\n"
            "5. 如果给定页面里同时有静态领导页和新闻/奖项/报道/活动页，只采信静态领导页，不要被新闻引语误导。\n"
            "6. 新闻稿或报道里的 Chair、Founder、发言人、受访者，默认不算最高负责人，除非静态领导页也能佐证。\n"
            "7. 对 cookie、登录、招聘公告、页脚法务、隐私条款这类噪音内容一律忽略，不要把它们当成负责人证据。\n"
            "8. 不能猜名字，名字必须在网页正文里明确出现。\n"
            "9. 优先返回完整人名；如果官网只公开了单名，并且这个单名在静态 about/team/leadership/founder 语境里明确指向某一个自然人，也允许返回单名。\n"
            "10. 不能把职位词、团队词、角色词当成人名。比如 Founder、Director、Sales Team、Complaints Manager 这类都返回空。\n"
            "11. 如果官网明确写出多个并列最高负责人，例如 general partners、co-founders、founding directors、joint owners、兄弟共同经营者，也不要留空。你必须从中挑一个你判断更高、或在页面上更核心的人名返回；如果完全分不出高低，就返回最先被正式列出的那个人。\n"
            "12. 如果没有正式 CEO/Director 头衔，但官网在静态 about/contact/profile/referrals/team/imprint 页面里清楚地把某个自然人作为主要对外联系人、创始人、principal solicitor、founding director、核心合伙人、核心服务负责人，也可以返回该自然人。\n"
            "13. 如果页面里只有普通员工、顾问、项目联系人、销售联系人、门店联系人，没有明确最高负责人或核心代表人，就返回空。\n"
            "14. evidence_url 必须尽量指向 about/team/leadership/management/board/governance/profile/contact/referrals/imprint/impressum 这类静态页面；如果只给新闻页而存在静态领导页，视为错误。\n"
            "15. 对律师事务所、会计师事务所、咨询公司这类专业服务站点，principal solicitor、founding director、named partner 这类角色可以视为最高负责人或核心代表人。\n"
            "16. 如果站点没有正式管理层页，但存在单独的人物详情页，并且该人物与公司主体、同域名邮箱、对外服务描述直接关联，也可以返回该自然人。\n"
            "17. 页面标题、OG 标题、meta description 里的姓名和头衔也算官网明确内容；如果这些头部信号与静态人物页/联系页一致，可以作为有效证据。\n"
            "18. 如果是明确以人物命名的静态页面，例如 Andy Maggs referrals、David Esfandi | Chief Executive Officer 这类，也可以把该人物视为核心代表人证据。\n"
            "19. evidence_quote 必须包含代表人姓名原文，并且要能支撑这个人确实出现在页面中。\n"
            "20. 找不到时可以留空，不要编造。\n\n"
            '返回 JSON：{"company_name":"","representative":"","evidence_url":"","evidence_quote":""}\n\n'
            f"首页: {homepage}\n"
            f"页面(JSON): {json.dumps(safe_pages, ensure_ascii=False)}"
        )
        data = self._call_json(prompt, deadline_monotonic=deadline_monotonic)
        representative = _normalize_representative_name(str(data.get("representative", "") or "").strip())
        evidence_quote = str(data.get("evidence_quote", "") or "").strip()
        if representative and not _quote_contains_name(evidence_quote, representative):
            representative = ""
            evidence_quote = ""
        return LlmExtractionResult(
            company_name=str(data.get("company_name", "") or "").strip(),
            representative=representative,
            evidence_url=str(data.get("evidence_url", "") or "").strip(),
            evidence_quote=evidence_quote,
        )

    def _convert_pages_to_markdown(self, pages: list[dict[str, str]]) -> list[dict[str, str]]:
        remove_tags = ["script", "style", "img", "svg", "video", "audio", "canvas", "iframe", "noscript"]
        result: list[dict[str, str]] = []
        for page in pages:
            html_text = str(page.get("html", "") or "")
            url = str(page.get("url", "") or "")
            if not html_text.strip():
                result.append({"url": url, "content": ""})
                continue
            soup = BeautifulSoup(html_text, "lxml")
            head_signals = _extract_head_signal_lines(soup)
            for tag in soup.find_all(remove_tags):
                tag.decompose()
            content = MarkdownConverter().convert_soup(soup)
            content = re.sub(r"\n{3,}", "\n\n", content).strip()
            if head_signals:
                content = "\n".join(["--- 页面头部信号 ---", *head_signals, "", content]).strip()
            if len(content) > self._MAX_PAGE_CHARS:
                half = self._MAX_PAGE_CHARS // 2
                content = content[:half] + "\n\n...（内容过长已截断）...\n\n" + content[-half:]
            result.append({"url": url, "content": content})
        return result

    def _call_json(self, prompt: str, *, deadline_monotonic: float | None = None) -> dict[str, Any]:
        text = prompt[: self._MAX_PROMPT_CHARS]
        kwargs: dict[str, Any] = {
            "model": self._model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": text}]}],
        }
        if self._reasoning_effort:
            kwargs["reasoning"] = {"effort": self._reasoning_effort}
        semaphore = _get_llm_semaphore()
        acquire_timeout = _remaining_deadline_seconds(deadline_monotonic)
        if acquire_timeout is not None and acquire_timeout <= 0:
            raise TimeoutError("site_deadline_exceeded")
        acquired = semaphore.acquire(timeout=acquire_timeout)
        if not acquired:
            raise TimeoutError("llm_queue_timeout")
        try:
            output = self._call_with_retry(kwargs, deadline_monotonic=deadline_monotonic)
        finally:
            semaphore.release()
        return _parse_json_text(output)

    def _set_global_concurrency_limit(self, limit: int) -> None:
        global _LLM_SEMAPHORE
        global _LLM_SEMAPHORE_LIMIT
        bounded = max(limit, 1)
        with _LLM_SEMAPHORE_LOCK:
            current = _LLM_SEMAPHORE
            if current is None or _LLM_SEMAPHORE_LIMIT != bounded:
                _LLM_SEMAPHORE = threading.Semaphore(bounded)
                _LLM_SEMAPHORE_LIMIT = bounded

    def _call_with_retry(
        self,
        kwargs: dict[str, Any],
        *,
        deadline_monotonic: float | None = None,
        max_retries: int = 2,
    ) -> str:
        transient_attempt = 0
        while True:
            _raise_if_deadline_exceeded(deadline_monotonic)
            try:
                if self._api_style == "chat":
                    return self._call_chat_with_retry(
                        kwargs,
                        deadline_monotonic=deadline_monotonic,
                        max_retries=max_retries,
                    )
                response_text = self._call_responses_streaming_api(kwargs, deadline_monotonic=deadline_monotonic)
                if response_text.strip():
                    return response_text
                return self._call_chat_with_retry(
                    kwargs,
                    deadline_monotonic=deadline_monotonic,
                    max_retries=max_retries,
                )
            except Exception as exc:  # noqa: BLE001
                failure = classify_llm_exception(exc)
                if failure is not None:
                    if failure.prompt_mode == "new_key":
                        raise LlmConfigurationError(failure.user_message, failure=failure) from exc
                    transient_attempt += 1
                    if transient_attempt >= max_retries:
                        raise LlmTemporaryError(failure.user_message, failure=failure) from exc
                    _sleep_for_llm_failure(failure, transient_attempt, deadline_monotonic=deadline_monotonic)
                    continue
                error_text = str(exc)
                if any(
                    token in error_text.lower()
                    for token in (
                        "timeout",
                        "timed out",
                        "connection",
                        "remoteprotocolerror",
                        "server disconnected",
                        "unexpected_eof_while_reading",
                        "eof occurred in violation of protocol",
                    )
                ):
                    transient_attempt += 1
                    if transient_attempt >= max_retries:
                        raise
                    _sleep_with_jitter(min(2 ** transient_attempt, 8), 1.0, deadline_monotonic=deadline_monotonic)
                    continue
                raise

    def _call_chat_with_retry(
        self,
        kwargs: dict[str, Any],
        *,
        deadline_monotonic: float | None = None,
        max_retries: int = 2,
    ) -> str:
        transient_attempt = 0
        prompt = self._extract_prompt_from_kwargs(kwargs)
        chat_kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }
        while True:
            request_timeout = _bounded_deadline_timeout(self._timeout_seconds, deadline_monotonic)
            try:
                response = self._client.chat.completions.create(**chat_kwargs, timeout=request_timeout)
                return _extract_chat_text(response)
            except Exception as exc:  # noqa: BLE001
                failure = classify_llm_exception(exc)
                if failure is not None:
                    if failure.prompt_mode == "new_key":
                        raise LlmConfigurationError(failure.user_message, failure=failure) from exc
                    transient_attempt += 1
                    if transient_attempt >= max_retries:
                        raise LlmTemporaryError(failure.user_message, failure=failure) from exc
                    _sleep_for_llm_failure(failure, transient_attempt, deadline_monotonic=deadline_monotonic)
                    continue
                error_text = str(exc).lower()
                transient_attempt += 1
                if transient_attempt >= max_retries:
                    raise
                if any(
                    token in error_text
                    for token in (
                        "timeout",
                        "timed out",
                        "connection",
                        "remoteprotocolerror",
                        "server disconnected",
                        "unexpected_eof_while_reading",
                        "eof occurred in violation of protocol",
                    )
                ):
                    _sleep_with_jitter(min(2 ** transient_attempt, 8), 1.0, deadline_monotonic=deadline_monotonic)
                    continue
                raise

    def _extract_prompt_from_kwargs(self, kwargs: dict[str, Any]) -> str:
        input_items = kwargs.get("input", [])
        if not isinstance(input_items, list):
            return ""
        parts: list[str] = []
        for item in input_items:
            if not isinstance(item, dict):
                continue
            content = item.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                text = str(block.get("text", "") or "").strip()
                if text:
                    parts.append(text)
        return "\n".join(parts)

    def _call_responses_streaming_api(self, kwargs: dict[str, Any], *, deadline_monotonic: float | None = None) -> str:
        payload = dict(kwargs)
        payload["stream"] = True
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "User-Agent": _DEFAULT_HEADERS["User-Agent"],
        }
        stream_url = self._base_url.rstrip("/") + "/responses"
        chunks: list[str] = []
        request_timeout = _bounded_deadline_timeout(self._timeout_seconds, deadline_monotonic)
        with self._http_client.stream("POST", stream_url, headers=headers, json=payload, timeout=request_timeout) as response:
            response.raise_for_status()
            for raw_line in response.iter_lines():
                line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else str(raw_line or "")
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                payload_text = line[5:].strip()
                if not payload_text or payload_text == "[DONE]":
                    continue
                try:
                    event = json.loads(payload_text)
                except json.JSONDecodeError:
                    continue
                event_type = str(event.get("type", "") or "")
                if event_type == "response.output_text.delta":
                    delta = str(event.get("delta", "") or "")
                    if delta:
                        chunks.append(delta)
                    continue
                if event_type == "response.output_text.done":
                    text = str(event.get("text", "") or "")
                    if text and not chunks:
                        chunks.append(text)
        return "".join(chunks)


def _get_llm_semaphore() -> threading.Semaphore:
    global _LLM_SEMAPHORE
    global _LLM_SEMAPHORE_LIMIT
    with _LLM_SEMAPHORE_LOCK:
        if _LLM_SEMAPHORE is None:
            _LLM_SEMAPHORE = threading.Semaphore(52)
            _LLM_SEMAPHORE_LIMIT = 52
        return _LLM_SEMAPHORE


def _http2_is_available() -> bool:
    return importlib.util.find_spec("h2") is not None


def _sleep_with_jitter(
    base_seconds: float,
    jitter_seconds: float,
    *,
    deadline_monotonic: float | None = None,
) -> None:
    floor = max(base_seconds - jitter_seconds, 0.0)
    ceiling = max(base_seconds + jitter_seconds, floor)
    sleep_seconds = random.uniform(floor, ceiling)
    remaining = _remaining_deadline_seconds(deadline_monotonic)
    if remaining is not None:
        sleep_seconds = min(sleep_seconds, max(remaining, 0.0))
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)


def _sleep_for_llm_failure(
    failure: LlmIntervention,
    transient_attempt: int,
    *,
    deadline_monotonic: float | None = None,
) -> None:
    if failure.retry_after_seconds is not None:
        _sleep_with_jitter(
            float(failure.retry_after_seconds),
            0.5,
            deadline_monotonic=deadline_monotonic,
        )
        return
    if failure.status_code == 429:
        _sleep_with_jitter(5.0, 1.5, deadline_monotonic=deadline_monotonic)
        return
    _sleep_with_jitter(
        min(30 + transient_attempt * 5, 60),
        4.0,
        deadline_monotonic=deadline_monotonic,
    )


def _bounded_deadline_timeout(base_timeout: float, deadline_monotonic: float | None) -> float:
    remaining = _remaining_deadline_seconds(deadline_monotonic)
    if remaining is None:
        return max(base_timeout, 0.05)
    if remaining <= 0:
        raise TimeoutError("site_deadline_exceeded")
    return max(min(base_timeout, remaining), 0.05)


def _remaining_deadline_seconds(deadline_monotonic: float | None) -> float | None:
    if deadline_monotonic is None:
        return None
    return deadline_monotonic - time.monotonic() - _SITE_DEADLINE_SAFETY_SECONDS


def _raise_if_deadline_exceeded(deadline_monotonic: float | None) -> None:
    remaining = _remaining_deadline_seconds(deadline_monotonic)
    if remaining is not None and remaining <= 0:
        raise TimeoutError("site_deadline_exceeded")


def _extract_response_text(response: Any) -> str:
    text = str(getattr(response, "output_text", "") or "")
    if text.strip():
        return text
    output = getattr(response, "output", None) or []
    chunks: list[str] = []
    for item in output:
        content = getattr(item, "content", None)
        if content is None and isinstance(item, dict):
            content = item.get("content", [])
        for part in content or []:
            part_type = getattr(part, "type", None)
            if part_type is None and isinstance(part, dict):
                part_type = part.get("type", "")
            if part_type not in {"output_text", "text"}:
                continue
            part_text = getattr(part, "text", None)
            if part_text is None and isinstance(part, dict):
                part_text = part.get("text", "")
            if isinstance(part_text, str) and part_text:
                chunks.append(part_text)
    return "".join(chunks)


def _extract_chat_text(response: Any) -> str:
    choices = getattr(response, "choices", None) or []
    if not choices:
        return ""
    message = getattr(choices[0], "message", None)
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            text = getattr(item, "text", None)
            if text is None and isinstance(item, dict):
                text = item.get("text", "")
            if isinstance(text, str) and text:
                parts.append(text)
        return "".join(parts)
    return str(content or "")


def _parse_json_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass
    match = _JSON_BLOCK_RE.search(text)
    if match is None:
        return {}
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _prepare_representative_pages(pages: list[dict[str, str]]) -> list[dict[str, str]]:
    ranked = sorted(
        pages,
        key=lambda page: _representative_page_priority(page.get("url", ""), page.get("content", "")),
        reverse=True,
    )
    prepared = [
        {
            "url": str(page.get("url", "") or "").strip(),
            "content": _prioritize_representative_content(str(page.get("content", "") or "").strip()),
        }
        for page in ranked
    ]
    return _fit_representative_pages_to_budget(prepared, budget=_REPRESENTATIVE_PAGE_BUDGET)


def _extract_head_signal_lines(soup: BeautifulSoup) -> list[str]:
    lines: list[str] = []
    if soup.title and soup.title.string:
        title = re.sub(r"\s+", " ", str(soup.title.string or "")).strip()
        if title:
            lines.append(title)
    for tag in soup.find_all("meta"):
        key = str(tag.get("property") or tag.get("name") or "").strip().lower()
        if key not in _META_HEAD_SIGNAL_NAMES:
            continue
        content = re.sub(r"\s+", " ", str(tag.get("content") or "")).strip()
        if content and content not in lines:
            lines.append(content)
    return lines[:6]


def _representative_page_priority(url: str, content: str) -> int:
    lowered_url = str(url or "").lower()
    lowered_content = str(content or "").lower()
    score = 0
    for hint in _REPRESENTATIVE_CONTENT_HINTS:
        if hint in lowered_url:
            score += 6
        if hint in lowered_content:
            score += 2
    for noise in _REPRESENTATIVE_DROP_HINTS:
        if noise in lowered_url:
            score -= 4
    return score


def _prioritize_representative_content(content: str) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    filtered_lines = _strip_representative_noise_lines(text)
    if not filtered_lines:
        return text
    windows = _collect_representative_windows(filtered_lines)
    if not windows:
        return "\n".join(filtered_lines)
    prioritized_lines: list[str] = []
    seen: set[str] = set()
    prioritized_lines.append("--- 重点片段 ---")
    for start, end in windows:
        _append_unique_lines(prioritized_lines, filtered_lines[start:end], seen)
        prioritized_lines.append("---")
    if prioritized_lines and prioritized_lines[-1] == "---":
        prioritized_lines.pop()
    if filtered_lines:
        prioritized_lines.append("--- 页面开头 ---")
        _append_unique_lines(prioritized_lines, filtered_lines[:_REPRESENTATIVE_HEAD_LINE_LIMIT], seen)
    if seen:
        prioritized_lines.append("--- 完整正文 ---")
    _append_unique_lines(prioritized_lines, filtered_lines, seen)
    return "\n".join(prioritized_lines).strip()


def _fit_representative_pages_to_budget(pages: list[dict[str, str]], *, budget: int) -> list[dict[str, str]]:
    if _total_page_chars(pages) <= budget:
        return pages
    fitted = [{"url": page["url"], "content": page["content"]} for page in pages]
    degrade_order = list(range(len(fitted) - 1, _REPRESENTATIVE_KEEP_FULL_PAGES - 1, -1))
    degrade_order.extend(range(min(_REPRESENTATIVE_KEEP_FULL_PAGES - 1, len(fitted) - 1), -1, -1))
    for index in degrade_order:
        original = fitted[index]["content"]
        compacted = _abbreviate_representative_content(original)
        if len(compacted) >= len(original):
            continue
        fitted[index]["content"] = compacted
        if _total_page_chars(fitted) <= budget:
            break
    return fitted


def _total_page_chars(pages: list[dict[str, str]]) -> int:
    return sum(len(str(page.get("content", "") or "")) for page in pages)


def _abbreviate_representative_content(content: str) -> str:
    lines = _strip_representative_noise_lines(content)
    if not lines:
        return str(content or "").strip()
    windows = _collect_representative_windows(lines)
    abbreviated: list[str] = []
    seen: set[str] = set()
    if windows:
        abbreviated.append("--- 重点片段 ---")
        for start, end in windows:
            _append_unique_lines(abbreviated, lines[start:end], seen)
            abbreviated.append("---")
        if abbreviated and abbreviated[-1] == "---":
            abbreviated.pop()
    abbreviated.append("--- 页面开头 ---")
    _append_unique_lines(abbreviated, lines[:_REPRESENTATIVE_HEAD_LINE_LIMIT], seen)
    if len(lines) > _REPRESENTATIVE_TAIL_LINE_LIMIT:
        abbreviated.append("--- 末尾上下文 ---")
        _append_unique_lines(abbreviated, lines[-_REPRESENTATIVE_TAIL_LINE_LIMIT:], seen)
    return "\n".join(abbreviated).strip()


def _strip_representative_noise_lines(content: str) -> list[str]:
    text = str(content or "").strip()
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
    return [
        line
        for line in raw_lines
        if not any(noise in line.lower() for noise in _REPRESENTATIVE_DROP_HINTS)
    ]


def _append_unique_lines(target: list[str], source: list[str], seen: set[str]) -> None:
    for line in source:
        if line in seen:
            continue
        seen.add(line)
        target.append(line)


def _collect_representative_windows(lines: list[str]) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for index, line in enumerate(lines):
        lowered = line.lower()
        if any(hint in lowered for hint in _REPRESENTATIVE_CONTENT_HINTS):
            windows.append((max(0, index - 2), min(len(lines), index + 8)))
    if not windows:
        return []
    merged: list[tuple[int, int]] = []
    start, end = windows[0]
    for next_start, next_end in windows[1:]:
        if next_start <= end + 2:
            end = max(end, next_end)
            continue
        merged.append((start, end))
        start, end = next_start, next_end
    merged.append((start, end))
    return merged[:8]


def _quote_contains_name(quote: str, name: str) -> bool:
    if not quote or not name:
        return False
    parts = [part for part in name.split() if part]
    if not parts:
        return False
    matches = sum(1 for part in parts if part.lower() in quote.lower())
    return matches >= max(1, len(parts) // 2)


def _normalize_representative_name(name: str) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    text = re.sub(r"^(mr|mrs|ms|miss|dr|sir|madam|mx)\.?\s+", "", text, flags=re.IGNORECASE).strip()
    parts = [part for part in text.split() if part]
    if not parts:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", "", text.lower())
    if not normalized or normalized in _REPRESENTATIVE_REJECT_EXACT:
        return ""
    return " ".join(parts)
