from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
from openai import AuthenticationError, InternalServerError, PermissionDeniedError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler import app as app_module
from oldironcrawler import console as console_module
from oldironcrawler import dashboard as dashboard_module
from oldironcrawler.extractor import llm_client as llm_module
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.importer import ImportedWebsite
from oldironcrawler.runtime import llm_ingress
from oldironcrawler.runtime.store import RuntimeStore


def _build_status_error(status_code: int, payload: dict[str, object]):
    request = httpx.Request("POST", "https://example.com/v1/responses")
    response = httpx.Response(status_code, request=request, json=payload)
    if status_code == 401:
        return AuthenticationError("auth failed", response=response, body=payload)
    if status_code == 403:
        return PermissionDeniedError("permission denied", response=response, body=payload)
    return InternalServerError("server error", response=response, body=payload)


class _FakeRuntimeStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.prepare_calls = 0
        self.reset_running_calls = 0
        self.closed = False

    def prepare_job(self, *, input_name: str, fingerprint: str, rows: list[ImportedWebsite]) -> None:
        self.prepare_calls += 1

    def reset_running_tasks(self) -> None:
        self.reset_running_calls += 1

    def reset_completed_job_for_rerun(self) -> bool:
        return False

    def progress(self) -> dict[str, int]:
        return {"total": 1}

    def close(self) -> None:
        self.closed = True


def test_gpteam_base_url_expands_to_all_production_ingress_nodes() -> None:
    nodes = llm_ingress.resolve_llm_ingress_nodes(
        primary_base_url="https://api.gpteamservices.com/v1",
        extra_base_urls=[],
    )

    assert [node.id for node in nodes] == ["jp-direct", "jp-split", "hk-split", "us-split"]


def test_select_best_llm_ingress_prefers_full_success_then_latency() -> None:
    nodes = [
        llm_ingress.LlmIngressNode("slow", "慢入口", "https://slow.example/v1", "https://slow.example/api/health"),
        llm_ingress.LlmIngressNode("lossy", "丢包入口", "https://lossy.example/v1", "https://lossy.example/api/health"),
        llm_ingress.LlmIngressNode("fast", "快入口", "https://fast.example/v1", "https://fast.example/api/health"),
    ]
    call_order: list[str] = []

    def fake_probe(node, _options):
        call_order.append(node.id)
        if node.id == "lossy" and call_order.count("lossy") == 2:
            return llm_ingress.LlmIngressProbe(ok=False, first_event_ms=None, total_ms=90.0, error="stream timeout")
        if node.id == "slow":
            return llm_ingress.LlmIngressProbe(ok=True, first_event_ms=120.0, total_ms=220.0)
        return llm_ingress.LlmIngressProbe(ok=True, first_event_ms=40.0, total_ms=90.0)

    selection = llm_ingress.select_best_llm_ingress(
        api_key="test-key",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        nodes=nodes,
        rounds=2,
        timeout_seconds=1.0,
        benchmark_probe=fake_probe,
    )

    assert selection.best.node.id == "fast"
    assert selection.best.success_rate == 1.0
    assert len(call_order) == 6


def test_benchmark_llm_ingress_uses_configured_proxy(monkeypatch) -> None:
    seen_client_kwargs: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        is_success = True
        text = ""

        def read(self) -> bytes:
            return b""

        def json(self) -> dict[str, object]:
            return {}

    class FakeStreamResponse:
        status_code = 200
        is_success = True

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb) -> None:
            return None

        def iter_lines(self):
            yield 'data: {"type":"response.output_text.delta"}'
            yield "data: [DONE]"

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            seen_client_kwargs.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb) -> None:
            return None

        def get(self, _url: str, **_kwargs):
            return FakeResponse()

        def stream(self, *_args, **_kwargs):
            return FakeStreamResponse()

    node = llm_ingress.LlmIngressNode("fast", "快入口", "https://fast.example/v1", "https://fast.example/api/health")
    options = llm_ingress.LlmIngressBenchmarkOptions(
        api_key="test-key",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        timeout_seconds=3.0,
        proxy_url="http://127.0.0.1:7897",
    )
    monkeypatch.setattr(llm_ingress.httpx, "Client", FakeClient)

    probe = llm_ingress.benchmark_llm_ingress_once(node, options)

    assert probe.ok is True
    assert seen_client_kwargs["proxy"] == "http://127.0.0.1:7897"
    assert seen_client_kwargs["trust_env"] is False


def test_ingress_stream_probe_stops_at_total_deadline(monkeypatch) -> None:
    class FakeStreamResponse:
        status_code = 200
        is_success = True

        def __enter__(self):
            return self

        def __exit__(self, _exc_type, _exc, _tb) -> None:
            return None

        def iter_lines(self):
            yield ": keepalive"

    class FakeClient:
        def stream(self, *_args, **_kwargs):
            return FakeStreamResponse()

    node = llm_ingress.LlmIngressNode("slow", "慢入口", "https://slow.example/v1", "https://slow.example/api/health")
    options = llm_ingress.LlmIngressBenchmarkOptions(
        api_key="test-key",
        model="gpt-5.4-mini",
        reasoning_effort="low",
        timeout_seconds=3.0,
    )
    times = iter([0.0, 2.0])
    monkeypatch.setattr(llm_ingress.time, "perf_counter", lambda: next(times))

    result = llm_ingress._probe_response_stream(FakeClient(), node, options, started=0.0, deadline=1.0)

    assert result.ok is False
    assert result.error == "入口测速超时"


def test_frozen_llm_client_ignores_tls_verify_env(monkeypatch) -> None:
    seen_client_kwargs: dict[str, object] = {}

    class FakeHttpClient:
        def __init__(self, **kwargs) -> None:
            seen_client_kwargs.update(kwargs)

        def close(self) -> None:
            return None

    class FakeOpenAI:
        def __init__(self, **_kwargs) -> None:
            return None

    monkeypatch.setenv("LLM_TLS_VERIFY", "0")
    monkeypatch.setattr(llm_module.sys, "frozen", True, raising=False)
    monkeypatch.setattr(llm_module.httpx, "Client", FakeHttpClient)
    monkeypatch.setattr(llm_module, "OpenAI", FakeOpenAI)

    client = WebsiteLlmClient(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="gpt-5.4-mini",
        api_style="responses",
        reasoning_effort="low",
        proxy_url="",
        timeout_seconds=3.0,
        concurrency_limit=1,
    )
    client.close()

    assert "verify" not in seen_client_kwargs


def test_validate_llm_runtime_applies_selected_ingress(tmp_path: Path, monkeypatch, capsys) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://api.gpteamservices.com/v1",
                "LLM_BASE_URLS=https://custom.example/v1",
                "LLM_KEY=good-key",
                "LLM_MODEL=gpt-5.4-mini",
                "PROXY_URL=http://127.0.0.1:7897",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("oldironcrawler.config._local_proxy_is_ready", lambda _proxy_url: True)
    config = app_module._load_runtime_config(tmp_path, "good-key")
    selected_node = llm_ingress.LlmIngressNode(
        "fast",
        "快入口",
        "https://fast.example/v1",
        "https://fast.example/api/health",
    )
    selected = llm_ingress.LlmIngressSelection(
        best=llm_ingress.LlmIngressResult(
            node=selected_node,
            samples=[llm_ingress.LlmIngressProbe(ok=True, first_event_ms=30.0, total_ms=70.0)],
        ),
        results=[],
    )
    seen: dict[str, object] = {}

    def fake_select(**kwargs):
        seen.update(kwargs)
        return selected

    monkeypatch.setattr(app_module, "select_best_llm_ingress", fake_select)

    app_module._validate_llm_runtime(config)

    assert config.llm_base_url == "https://fast.example/v1"
    assert seen["api_key"] == "good-key"
    assert seen["proxy_url"] == "http://127.0.0.1:7897"
    assert any(node.base_url == "https://custom.example/v1" for node in seen["nodes"])
    assert "已选择 LLM API 入口：快入口" in capsys.readouterr().out


def test_run_selected_input_uses_selected_ingress_for_rows_and_crawl(tmp_path: Path, monkeypatch) -> None:
    workbook = tmp_path / "sites.xlsx"
    workbook.write_text("", encoding="utf-8")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://api.gpteamservices.com/v1",
                "LLM_KEY=good-key",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    rows = [ImportedWebsite(input_index=1, raw_website="acme.com", website="https://acme.com", dedupe_key="acme.com")]
    seen: list[str] = []

    def fake_load_rows(config, _input_path: Path):
        seen.append(f"load:{config.llm_base_url}")
        return rows

    def fake_run(config, _store, _delivery_path) -> None:
        seen.append(f"run:{config.llm_base_url}")

    monkeypatch.setattr(app_module, "_load_input_rows", fake_load_rows)
    monkeypatch.setattr(app_module, "RuntimeStore", _FakeRuntimeStore)
    monkeypatch.setattr(app_module, "run_crawl_session", fake_run)

    result = app_module.run_selected_input(
        tmp_path,
        "good-key",
        workbook,
        llm_base_url="https://fast.example/v1",
    )

    assert result.exit_code == 0
    assert result.llm_base_url == "https://fast.example/v1"
    assert seen == ["load:https://fast.example/v1", "run:https://fast.example/v1"]


def test_classify_invalid_api_key_requires_new_key() -> None:
    classifier = getattr(llm_module, "classify_llm_exception", None)

    assert classifier is not None
    details = classifier(
        _build_status_error(
            401,
            {"error": {"message": "Incorrect API key provided.", "code": "invalid_api_key", "type": "invalid_request_error"}},
        )
    )

    assert details is not None
    assert details.prompt_mode == "new_key"
    assert details.category == "invalid_key"
    assert "Key" in details.user_message


def test_classify_budget_exhausted_requires_new_key() -> None:
    classifier = getattr(llm_module, "classify_llm_exception", None)

    assert classifier is not None
    details = classifier(
        _build_status_error(
            403,
            {"error": {"message": "额度不足（预算已用尽）。", "code": "budget_exhausted", "type": "insufficient_quota"}},
        )
    )

    assert details is not None
    assert details.prompt_mode == "new_key"
    assert details.category == "quota_exhausted"
    assert "额度" in details.user_message


def test_classify_service_temporarily_unavailable_pauses_current_key() -> None:
    classifier = getattr(llm_module, "classify_llm_exception", None)

    assert classifier is not None
    details = classifier(
        _build_status_error(
            503,
            {
                "error": {
                    "message": "请求暂时不可用，请稍后重试。",
                    "code": "service_temporarily_unavailable",
                    "type": "api_connection_error",
                }
            },
        )
    )

    assert details is not None
    assert details.prompt_mode == "retry"
    assert details.category == "temporary_unavailable"
    assert "暂时不可用" in details.user_message


def test_run_interactive_reprompts_new_key_without_reselecting_workbook(tmp_path: Path, monkeypatch) -> None:
    workbook = tmp_path / "sites.xlsx"
    workbook.write_text("", encoding="utf-8")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    rows = [ImportedWebsite(input_index=1, raw_website="acme.com", website="https://acme.com", dedupe_key="acme.com")]
    key_prompts: list[str] = []
    selected_files: list[Path] = []
    load_rows_calls: list[str] = []
    validate_calls: list[str] = []
    run_calls: list[str] = []

    monkeypatch.setattr(app_module, "choose_input_file", lambda _path: selected_files.append(workbook) or workbook)

    def fake_validate(config) -> None:
        validate_calls.append(config.llm_key)
        if config.llm_key == "bad-key":
            raise LlmConfigurationError("invalid_api_key")

    monkeypatch.setattr(app_module, "_validate_llm_runtime", fake_validate)

    def fake_load_rows(config, input_path: Path):
        load_rows_calls.append(config.llm_key)
        assert input_path == workbook
        return rows

    monkeypatch.setattr(app_module, "_load_input_rows", fake_load_rows)
    monkeypatch.setattr(app_module, "RuntimeStore", _FakeRuntimeStore)
    monkeypatch.setattr(app_module, "run_crawl_session", lambda config, store, delivery_path: run_calls.append(config.llm_key))
    monkeypatch.setattr(console_module, "prompt_runtime_llm_key", lambda: key_prompts.append("good-key") or "good-key")

    try:
        result = app_module.run_interactive(tmp_path, llm_key_override="bad-key")
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"run_interactive should recover by asking for a new key, got: {exc}")

    assert result == 0
    assert selected_files == [workbook]
    assert validate_calls == ["bad-key", "good-key"]
    assert load_rows_calls == ["good-key"]
    assert run_calls == ["good-key"]
    assert key_prompts == ["good-key"]


def test_run_interactive_validates_key_before_showing_file_list(tmp_path: Path, monkeypatch) -> None:
    workbook = tmp_path / "sites.xlsx"
    workbook.write_text("", encoding="utf-8")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    rows = [ImportedWebsite(input_index=1, raw_website="acme.com", website="https://acme.com", dedupe_key="acme.com")]
    event_log: list[str] = []

    def fake_validate(config) -> None:
        event_log.append(f"validate:{config.llm_key}")
        if config.llm_key == "bad-key":
            raise LlmConfigurationError("invalid_api_key")

    monkeypatch.setattr(app_module, "_validate_llm_runtime", fake_validate)
    monkeypatch.setattr(console_module, "prompt_runtime_llm_key", lambda: event_log.append("prompt") or "good-key")
    monkeypatch.setattr(app_module, "choose_input_file", lambda _path: event_log.append("choose") or workbook)
    monkeypatch.setattr(app_module, "_load_input_rows", lambda config, _input_path: event_log.append(f"load:{config.llm_key}") or rows)
    monkeypatch.setattr(app_module, "RuntimeStore", _FakeRuntimeStore)
    monkeypatch.setattr(app_module, "run_crawl_session", lambda config, store, delivery_path: event_log.append(f"run:{config.llm_key}"))

    result = app_module.run_interactive(tmp_path, llm_key_override="bad-key")

    assert result == 0
    assert event_log == [
        "validate:bad-key",
        "prompt",
        "validate:good-key",
        "choose",
        "load:good-key",
        "run:good-key",
    ]


def test_run_interactive_auto_retries_same_key_after_temporary_llm_outage(tmp_path: Path, monkeypatch, capsys) -> None:
    workbook = tmp_path / "sites.xlsx"
    workbook.write_text("", encoding="utf-8")
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    rows = [ImportedWebsite(input_index=1, raw_website="acme.com", website="https://acme.com", dedupe_key="acme.com")]
    selected_files: list[Path] = []
    load_rows_calls: list[str] = []
    run_calls: list[str] = []

    monkeypatch.setattr(app_module, "choose_input_file", lambda _path: selected_files.append(workbook) or workbook)
    monkeypatch.setattr(app_module, "_validate_llm_runtime", lambda _config: None)

    def fake_load_rows(config, input_path: Path):
        load_rows_calls.append(config.llm_key)
        assert input_path == workbook
        return rows

    monkeypatch.setattr(app_module, "_load_input_rows", fake_load_rows)
    monkeypatch.setattr(app_module, "RuntimeStore", _FakeRuntimeStore)
    sleep_calls: list[int] = []
    monkeypatch.setattr(app_module.time, "sleep", lambda seconds: sleep_calls.append(int(seconds)))

    def fake_run(config, store, delivery_path) -> None:
        run_calls.append(config.llm_key)
        if len(run_calls) == 1:
            raise LlmConfigurationError("503 service_temporarily_unavailable")

    monkeypatch.setattr(app_module, "run_crawl_session", fake_run)
    monkeypatch.setattr(
        console_module,
        "wait_for_llm_retry_confirmation",
        lambda message=None: pytest.fail("temporary LLM outage should auto-retry without waiting for user input"),
        raising=False,
    )

    try:
        result = app_module.run_interactive(tmp_path, llm_key_override="steady-key")
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"run_interactive should pause and retry the same key, got: {exc}")

    assert result == 0
    assert selected_files == [workbook]
    assert load_rows_calls == ["steady-key"]
    assert run_calls == ["steady-key", "steady-key"]
    assert sleep_calls == [3]
    assert "程序将自动重试" in capsys.readouterr().out


def test_run_dashboard_validates_key_before_showing_panel(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    event_log: list[str] = []

    def fake_validate(config) -> None:
        event_log.append(f"validate:{config.llm_key}")
        if config.llm_key == "bad-key":
            raise LlmConfigurationError("invalid_api_key")

    answers = iter(["5"])

    monkeypatch.setattr(app_module, "_validate_llm_runtime", fake_validate)
    monkeypatch.setattr(console_module, "prompt_runtime_llm_key", lambda: event_log.append("prompt") or "good-key")
    monkeypatch.setattr(dashboard_module, "_render_panel", lambda title, lines: event_log.append(f"panel:{title}"))
    monkeypatch.setattr(dashboard_module, "_clear_screen", lambda: None)
    monkeypatch.setattr(dashboard_module, "wait_for_enter", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    result = dashboard_module.run_dashboard(tmp_path, "bad-key")

    assert result == 0
    assert event_log[:4] == [
        "validate:bad-key",
        "prompt",
        "validate:good-key",
        "panel:OldIronCrawler 控制面板",
    ]


def test_run_dashboard_retries_temporary_validation_error_without_crashing(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    event_log: list[str] = []
    answers = iter(["5"])

    def fake_validate(config) -> None:
        event_log.append(f"validate:{config.llm_key}")
        if len(event_log) == 1:
            raise LlmTemporaryError("503 service_temporarily_unavailable")

    monkeypatch.setattr(app_module, "_validate_llm_runtime", fake_validate)
    sleep_calls: list[int] = []
    monkeypatch.setattr(app_module.time, "sleep", lambda seconds: sleep_calls.append(int(seconds)))
    monkeypatch.setattr(dashboard_module, "_render_panel", lambda title, lines: event_log.append(f"panel:{title}"))
    monkeypatch.setattr(dashboard_module, "_clear_screen", lambda: None)
    monkeypatch.setattr(dashboard_module, "wait_for_enter", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    result = dashboard_module.run_dashboard(tmp_path, "steady-key")

    assert result == 0
    assert event_log[:3] == [
        "validate:steady-key",
        "validate:steady-key",
        "panel:OldIronCrawler 控制面板",
    ]
    assert sleep_calls == [3]


def test_reset_running_tasks_recovers_real_sqlite_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.sqlite3"
    store = RuntimeStore(db_path)
    rows = [ImportedWebsite(input_index=1, raw_website="acme.com", website="https://acme.com", dedupe_key="acme.com")]
    store.prepare_job(input_name="sites.xlsx", fingerprint="fp-1", rows=rows)

    claimed = store.claim_next_site()

    assert claimed is not None
    assert store.progress()["running"] == 1
    store.close()

    reopened = RuntimeStore(db_path)
    reopened.reset_running_tasks()
    progress = reopened.progress()
    reclaimed = reopened.claim_next_site()
    reopened.close()

    assert progress["running"] == 0
    assert progress["pending"] == 1
    assert reclaimed is not None
    assert reclaimed.id == claimed.id


def test_run_dashboard_persists_validated_key_to_env(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
                "LLM_KEY=",
                "LLM_API_KEY=",
            ]
        ),
        encoding="utf-8",
    )
    answers = iter(["5"])

    monkeypatch.setattr(app_module, "_validate_llm_runtime", lambda _config: None)
    monkeypatch.setattr(dashboard_module, "_clear_screen", lambda: None)
    monkeypatch.setattr(dashboard_module, "wait_for_enter", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    result = dashboard_module.run_dashboard(tmp_path, "saved-key")
    env_text = (tmp_path / ".env").read_text(encoding="utf-8")

    assert result == 0
    assert "LLM_KEY=saved-key" in env_text
    assert "LLM_API_KEY=saved-key" in env_text


def test_llm_ping_rejects_invalid_response_payload(monkeypatch) -> None:
    client = WebsiteLlmClient(
        api_key="test-key",
        base_url="https://example.com/v1",
        model="gpt-5.4-mini",
        api_style="responses",
        reasoning_effort="low",
        proxy_url="",
        timeout_seconds=5.0,
        concurrency_limit=1,
    )
    monkeypatch.setattr(client, "_call_json", lambda *_args, **_kwargs: {})

    with pytest.raises(RuntimeError, match="llm_ping_invalid_response"):
        client.ping()

    client.close()


def test_classify_invalid_ping_response_as_temporary_issue() -> None:
    details = llm_module.classify_llm_exception("llm_ping_invalid_response")

    assert details is not None
    assert details.prompt_mode == "retry"
    assert details.category == "temporary_unavailable"


def test_run_dashboard_retries_invalid_file_choice_inside_panel(tmp_path: Path, monkeypatch) -> None:
    websites_dir = tmp_path / "websites"
    websites_dir.mkdir(parents=True)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    selected_file = websites_dir / "sites.xlsx"
    selected_file.write_text("", encoding="utf-8")
    event_log: list[str] = []
    answers = iter(["1", "9", "1", "5"])

    monkeypatch.setattr(app_module, "_validate_llm_runtime", lambda _config: None)
    monkeypatch.setattr(
        app_module,
        "run_selected_input",
        lambda project_root, current_key, input_path, **kwargs: event_log.append(f"run:{input_path.name}") or app_module.CrawlRunResult(
            exit_code=0,
            delivery_path=tmp_path / "output" / "sites.csv",
            effective_key=current_key,
        ),
    )
    monkeypatch.setattr(dashboard_module, "_clear_screen", lambda: None)
    monkeypatch.setattr(dashboard_module, "wait_for_enter", lambda *args, **kwargs: None)
    monkeypatch.setattr(dashboard_module, "_open_folder", lambda _path: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    result = dashboard_module.run_dashboard(tmp_path, "good-key")

    assert result == 0
    assert event_log == ["run:sites.xlsx"]


def test_run_dashboard_delete_key_exits_system(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )
    answers = iter(["4", "1", "2"])

    monkeypatch.setattr(app_module, "_validate_llm_runtime", lambda _config: None)
    monkeypatch.setattr(dashboard_module, "_clear_screen", lambda: None)
    monkeypatch.setattr(dashboard_module, "wait_for_enter", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.input", lambda _prompt="": next(answers))

    result = dashboard_module.run_dashboard(tmp_path, "good-key")

    assert result == 0


def test_dashboard_wrap_and_pad_handle_wide_characters() -> None:
    wrapped = dashboard_module._wrap_line("系统配置 Key 状态：********g666", 12)
    padded = [dashboard_module._pad_panel_text(item, 12) for item in wrapped]

    assert wrapped
    assert all(dashboard_module._display_width(item) <= 12 for item in wrapped)
    assert all(dashboard_module._display_width(item) == 12 for item in padded)


def test_dashboard_uses_numeric_back_options() -> None:
    assert "0. 返回主菜单" in dashboard_module._build_file_select_lines([], None)
    assert "4. 返回主菜单" in dashboard_module._build_system_config_lines(
        key_status="已设置",
        concurrency=32,
        site_timeout_seconds=180,
    )
    assert "3. 返回系统配置" in dashboard_module._build_key_settings_lines("已设置")
