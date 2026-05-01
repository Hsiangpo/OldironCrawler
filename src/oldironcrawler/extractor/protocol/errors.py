from __future__ import annotations


class ProtocolTemporaryError(RuntimeError):
    pass


class ProtocolPermanentError(RuntimeError):
    pass


TEMP_ERROR_HINTS = (
    "timeout",
    "timed out",
    "429",
    "503",
    "504",
    "500",
    "502",
    "ssl",
    "tls",
    "eof",
    "getaddrinfo() thread failed to start",
    "thread failed to start",
    "couldn't create thread",
    "failed to create thread",
    "resource temporarily unavailable",
    "[errno 35]",
    "request_slot_timeout",
    "challenge_refetch_failed",
)

PERMANENT_ERROR_HINTS = (
    "could not resolve host",
    "name or service not known",
    "nodename nor servname",
    "certificate has expired",
    "ssl certificate problem",
    "no alternative certificate subject name matches",
    "certificate subject name",
    "failed to connect",
    "couldn't connect to server",
    "connection refused",
    "no route to host",
    "network is unreachable",
    "host is down",
    "could not connect to server",
)

FAST_FAIL_TLS_HANDSHAKE_HINTS = (
    "tls connect error",
    "tlsv1_alert",
    "sslv3_alert_handshake_failure",
    "handshake failure",
    "openssl_internal:invalid library",
)


def is_fast_fail_tls_handshake_error(lowered_error: str) -> bool:
    return any(token in lowered_error for token in FAST_FAIL_TLS_HANDSHAKE_HINTS)


def should_abort_common_probe_after_homepage_error(error: Exception) -> bool:
    if isinstance(error, ProtocolTemporaryError):
        return _is_homepage_fast_abort_temporary_error(error)
    if not isinstance(error, ProtocolPermanentError):
        return False
    lowered = str(error or "").lower()
    if any(token in lowered for token in ("http_403", "cloudflare_challenge", "sgcaptcha_challenge", "imperva_challenge")):
        return False
    if is_fast_fail_tls_handshake_error(lowered):
        return True
    return any(
        token in lowered
        for token in (
            "could not resolve host",
            "certificate has expired",
            "ssl certificate problem",
            "no alternative certificate subject name matches",
            "certificate subject name",
            "failed to connect",
            "couldn't connect to server",
            "connection refused",
            "no route to host",
            "network is unreachable",
            "host is down",
            "could not connect to server",
            "site_open_timeout",
        )
    )


def normalize_homepage_open_error(start_url: str, error: Exception) -> Exception:
    if isinstance(error, ProtocolTemporaryError) and is_site_open_timeout_error(error):
        return ProtocolPermanentError(f"site_open_timeout: {start_url}")
    return error


def _is_homepage_fast_abort_temporary_error(error: Exception) -> bool:
    lowered = str(error or "").lower()
    return is_site_open_timeout_error(error) or any(
        token in lowered
        for token in (
            "request_slot_timeout",
            "resource temporarily unavailable",
            "thread failed to start",
            "couldn't create thread",
            "failed to create thread",
            "site_deadline_exceeded",
        )
    )


def is_site_open_timeout_error(error: Exception) -> bool:
    lowered = str(error or "").lower()
    if any(token in lowered for token in ("request_slot_timeout", "resource temporarily unavailable", "thread failed to start")):
        return False
    return any(token in lowered for token in ("operation timed out", "timed out", "timeout"))
