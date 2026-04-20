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
from oldironcrawler.extractor.llm_client import LlmConfigurationError
from oldironcrawler.importer import ImportedWebsite


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
