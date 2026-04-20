from __future__ import annotations

import email.utils
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from openai import APIConnectionError, APIStatusError, APITimeoutError, AuthenticationError, RateLimitError


_RATE_LIMIT_CODES = {
    "rate_limited",
    "rate_limit_exceeded",
    "service_temporarily_unavailable",
    "too_many_inflight_requests",
    "upstream_account_concurrency_exceeded",
    "upstream_account_pool_exhausted",
    "upstream_account_rate_limited",
    "upstream_account_unavailable",
    "upstream_token_unavailable",
}
_ACCESS_RESTRICTED_CODES = {"ip_not_allowed", "max_ip_exceeded"}
_QUOTA_CODES = {"budget_exhausted", "insufficient_quota", "quota_exceeded"}


@dataclass(frozen=True)
class LlmIntervention:
    category: str
    prompt_mode: str
    user_message: str
    status_code: int | None
    error_code: str
    error_type: str
    raw_message: str
    retry_after_seconds: int | None = None


def classify_llm_exception(exc: BaseException | str) -> LlmIntervention | None:
    existing = getattr(exc, "failure", None)
    if isinstance(existing, LlmIntervention):
        return existing
    status_code = _extract_status_code(exc)
    error_code = _extract_error_code(exc)
    error_type = _extract_error_type(exc)
    raw_message = _extract_error_message(exc)
    if status_code is None:
        status_code = _find_numeric_status(raw_message)
    retry_after_seconds = _extract_retry_after_seconds(exc)
    text = " ".join(part for part in (error_code, error_type, raw_message) if part).lower()

    if (
        isinstance(exc, AuthenticationError)
        or status_code == 401
        or error_code == "invalid_api_key"
        or _contains_any(text, ("invalid_api_key", "incorrect api key", "missing api key"))
    ):
        return _build_intervention(
            category="invalid_key",
            prompt_mode="new_key",
            status_code=status_code,
            error_code=error_code or "invalid_api_key",
            error_type=error_type or "invalid_request_error",
            raw_message=raw_message or "Incorrect API key provided.",
            message="LLM 认证失败：Key 不正确或缺失，请重新输入。",
        )
    if status_code == 403 and error_code in _ACCESS_RESTRICTED_CODES:
        return _build_intervention(
            category="access_restricted",
            prompt_mode="new_key",
            status_code=status_code,
            error_code=error_code,
            error_type=error_type or "invalid_request_error",
            raw_message=raw_message,
            message="LLM 访问受限：当前 Key 或 IP 被限制，请更换 Key 或检查白名单。",
        )
    if status_code == 403 and (
        error_code in _QUOTA_CODES
        or error_type == "insufficient_quota"
        or _contains_any(text, ("budget_exhausted", "insufficient_quota", "quota", "额度", "预算已用尽"))
    ):
        return _build_intervention(
            category="quota_exhausted",
            prompt_mode="new_key",
            status_code=status_code,
            error_code=error_code or "budget_exhausted",
            error_type=error_type or "insufficient_quota",
            raw_message=raw_message,
            message="LLM 额度不足：当前 Key 已无可用额度，请重新输入新的 Key。",
        )
    if (
        isinstance(exc, RateLimitError)
        or status_code == 429
        or error_code in _RATE_LIMIT_CODES
        or error_type == "rate_limit_error"
        or _contains_any(
            text,
            (
                "rate limit",
                "rate_limited",
                "quota",
                "usage limit",
                "temporarily unavailable",
                "service_temporarily_unavailable",
                "暂时不可用",
            ),
        )
    ):
        return _build_intervention(
            category="temporary_unavailable",
            prompt_mode="retry",
            status_code=status_code,
            error_code=error_code or "service_temporarily_unavailable",
            error_type=error_type or "api_connection_error",
            raw_message=raw_message,
            retry_after_seconds=retry_after_seconds,
            message=_temporary_message(retry_after_seconds),
        )
    if status_code == 403:
        return _build_intervention(
            category="request_rejected",
            prompt_mode="retry",
            status_code=status_code,
            error_code=error_code or "request_rejected",
            error_type=error_type or "api_connection_error",
            raw_message=raw_message,
            message="LLM 服务返回 403，请求被拒绝，请稍后重试。",
        )
    if (
        isinstance(exc, (APIConnectionError, APITimeoutError, httpx.TimeoutException, httpx.NetworkError))
        or isinstance(exc, APIStatusError) and (status_code or 0) >= 500
        or _contains_any(text, ("timeout", "timed out", "connection", "remoteprotocolerror", "server disconnected", "upstream"))
    ):
        return _build_intervention(
            category="temporary_unavailable",
            prompt_mode="retry",
            status_code=status_code,
            error_code=error_code or "service_temporarily_unavailable",
            error_type=error_type or "api_connection_error",
            raw_message=raw_message,
            retry_after_seconds=retry_after_seconds,
            message=_temporary_message(retry_after_seconds),
        )
    return None


def _build_intervention(
    *,
    category: str,
    prompt_mode: str,
    status_code: int | None,
    error_code: str,
    error_type: str,
    raw_message: str,
    message: str,
    retry_after_seconds: int | None = None,
) -> LlmIntervention:
    suffix = []
    if status_code is not None:
        suffix.append(str(status_code))
    if error_code:
        suffix.append(error_code)
    if suffix:
        message = f"{message}（{' '.join(suffix)}）"
    return LlmIntervention(
        category=category,
        prompt_mode=prompt_mode,
        user_message=message,
        status_code=status_code,
        error_code=error_code,
        error_type=error_type,
        raw_message=raw_message,
        retry_after_seconds=retry_after_seconds,
    )


def _temporary_message(retry_after_seconds: int | None) -> str:
    if retry_after_seconds is None:
        return "LLM 服务暂时不可用，请稍后重试。"
    return f"LLM 服务暂时不可用，建议等待 {retry_after_seconds} 秒后重试。"


def _extract_status_code(exc: BaseException | str) -> int | None:
    if isinstance(exc, str):
        return _find_numeric_status(exc)
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code
    response = getattr(exc, "response", None)
    if response is None:
        return None
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _extract_error_code(exc: BaseException | str) -> str:
    body = getattr(exc, "body", None) if not isinstance(exc, str) else None
    return _read_error_field(body, "code")


def _extract_error_type(exc: BaseException | str) -> str:
    body = getattr(exc, "body", None) if not isinstance(exc, str) else None
    return _read_error_field(body, "type")


def _extract_error_message(exc: BaseException | str) -> str:
    if isinstance(exc, str):
        return exc.strip()
    body = getattr(exc, "body", None)
    body_message = _read_error_field(body, "message")
    if body_message:
        return body_message
    return str(exc).strip()


def _extract_retry_after_seconds(exc: BaseException | str) -> int | None:
    if isinstance(exc, str):
        return None
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    raw_value = str(headers.get("Retry-After", "") or "").strip()
    if not raw_value:
        return None
    if raw_value.isdigit():
        return max(int(raw_value), 1)
    parsed_time = email.utils.parsedate_to_datetime(raw_value)
    if parsed_time is None:
        return None
    if parsed_time.tzinfo is None:
        parsed_time = parsed_time.replace(tzinfo=timezone.utc)
    remaining = int((parsed_time - datetime.now(timezone.utc)).total_seconds())
    return max(remaining, 1)


def _read_error_field(body: Any, field: str) -> str:
    if not isinstance(body, dict):
        return ""
    value = body.get(field)
    if isinstance(value, str) and value.strip():
        return value.strip()
    error_body = body.get("error")
    if isinstance(error_body, dict):
        nested = error_body.get(field)
        if isinstance(nested, str):
            return nested.strip()
    return ""


def _find_numeric_status(text: str) -> int | None:
    for token in ("401", "403", "429", "500", "502", "503", "504"):
        if token in str(text or ""):
            return int(token)
    return None


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)
