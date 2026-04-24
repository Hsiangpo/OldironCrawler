from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.bootstrap import raise_nofile_soft_limit, resolve_runtime_root
from oldironcrawler.package_layout import build_portable_dist_folder
from oldironcrawler.config import AppConfig, persist_llm_key
from oldironcrawler.console import prompt_runtime_llm_key, wait_for_llm_retry_confirmation
from oldironcrawler.app import _apply_runtime_preferences, _build_artifact_stem, _derive_runtime_concurrency_budget
import oldironcrawler.package_layout as package_layout


def test_run_bootstrap_returns_source_project_root() -> None:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    import run

    root = run._bootstrap()

    assert root == PROJECT_ROOT


def test_packaging_assets_exist() -> None:
    assert (PROJECT_ROOT / "packaging" / "build_exe.ps1").exists()
    assert (PROJECT_ROOT / "packaging" / "OldIronCrawler.spec").exists()


def test_packaging_icon_asset_exists() -> None:
    assert (PROJECT_ROOT / "packaging" / "OldIronCrawler.ico").exists()


def test_packaging_spec_references_custom_icon() -> None:
    spec_text = (PROJECT_ROOT / "packaging" / "OldIronCrawler.spec").read_text(encoding="utf-8")

    assert "icon=str(project_root / \"packaging\" / \"OldIronCrawler.ico\")" in spec_text


def test_build_script_uses_custom_icon() -> None:
    script_text = (PROJECT_ROOT / "packaging" / "build_exe.ps1").read_text(encoding="utf-8")

    assert "--icon" in script_text
    assert "OldIronCrawler.ico" in script_text


def test_build_script_cleans_existing_portable_dir() -> None:
    script_text = (PROJECT_ROOT / "packaging" / "build_exe.ps1").read_text(encoding="utf-8")

    assert "Remove-PathWithRetry -LiteralPath $portableDir" in script_text


def test_packaging_targets_portable_folder_layout(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    dist_root = repo_root / "dist"
    dist_root.mkdir(parents=True)
    (repo_root / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_KEY=env-secret",
                "LLM_API_KEY=env-api-secret",
                "LLM_MODEL=gpt-5.4-mini",
                "PROXY_URL=http://127.0.0.1:7897",
                "CAPSOLVER_API_KEY=capsolver-secret",
                "CAPSOLVER_PROXY=http://secret-proxy.local:9000",
                "CLOUDFLARE_PROXY_URL=http://cloudflare-proxy.local:7000",
                "LLM_CONCURRENCY=4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    built_exe = dist_root / "OldIronCrawler.exe"
    built_exe.write_text("exe", encoding="utf-8")

    package_root = build_portable_dist_folder(repo_root=repo_root, built_exe_path=built_exe)

    assert package_root == dist_root / "OldIronCrawler"
    assert (package_root / "OldIronCrawler.exe").exists()
    assert (package_root / ".env").exists()
    packaged_env = (package_root / ".env").read_text(encoding="utf-8")
    assert "LLM_KEY=" in packaged_env
    assert "LLM_API_KEY=" in packaged_env
    assert "env-secret" not in packaged_env
    assert "env-api-secret" not in packaged_env
    assert "PROXY_URL=http://127.0.0.1:7897" in packaged_env
    assert "CAPSOLVER_API_KEY=capsolver-secret" in packaged_env
    assert "CAPSOLVER_PROXY=http://secret-proxy.local:9000" in packaged_env
    assert "CLOUDFLARE_PROXY_URL=http://cloudflare-proxy.local:7000" in packaged_env
    assert "LLM_CONCURRENCY=4" in packaged_env
    assert "PAGE_CONCURRENCY=32" not in packaged_env
    assert "PAGE_WORKER_COUNT=32" not in packaged_env
    assert "PAGE_HOST_LIMIT=32" not in packaged_env
    assert (package_root / "websites").is_dir()
    assert (package_root / "websites" / "把 Excel 网站表放到这里.md").exists()
    assert (package_root / "output").is_dir()
    assert (package_root / "output" / "runtime").is_dir()


def test_derive_runtime_concurrency_budget_keeps_site_concurrency_high() -> None:
    budget = _derive_runtime_concurrency_budget(64)

    assert budget.site_concurrency == 64
    assert budget.llm_concurrency == 12
    assert budget.page_concurrency == 12
    assert budget.page_worker_count == 32
    assert budget.page_host_limit == 4


def test_apply_runtime_preferences_shapes_internal_limits_without_lowering_site_concurrency() -> None:
    config = SimpleNamespace(
        llm_concurrency=1,
        site_concurrency=1,
        page_concurrency=1,
        page_worker_count=1,
        page_host_limit=1,
        total_wait_seconds=180.0,
    )

    _apply_runtime_preferences(config, concurrency=64, site_timeout_seconds=180)

    assert config.site_concurrency == 64
    assert config.llm_concurrency == 12
    assert config.page_worker_count == 32
    assert config.page_concurrency == 12
    assert config.page_host_limit == 4


def test_packaging_raises_when_existing_directory_is_not_writable(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    dist_root = repo_root / "dist"
    websites_dir = dist_root / "OldIronCrawler" / "websites"
    websites_dir.mkdir(parents=True)
    (repo_root / ".env").write_text("LLM_BASE_URL=https://example.com/v1\n", encoding="utf-8")
    built_exe = dist_root / "OldIronCrawler.exe"
    built_exe.write_text("exe", encoding="utf-8")

    real_ensure_directory_ready = package_layout._ensure_directory_ready

    def fake_ensure_directory_ready(directory: Path) -> None:
        if directory == websites_dir:
            raise RuntimeError(f"打包目录不可写：{directory}")
        real_ensure_directory_ready(directory)

    monkeypatch.setattr(package_layout, "_ensure_directory_ready", fake_ensure_directory_ready)

    with pytest.raises(RuntimeError, match="websites"):
        build_portable_dist_folder(repo_root=repo_root, built_exe_path=built_exe)


def test_prompt_runtime_llm_key_masks_and_retries() -> None:
    output = io.StringIO()
    chars = iter([" ", "\r", "a", "b", "c", "1", "2", "3", "\r"])

    key = prompt_runtime_llm_key(
        reader=lambda: next(chars),
        writer=output,
    )

    text = output.getvalue()
    assert key == "abc123"
    assert text.count("请输入 Crawler 公司官网系统爬虫密钥") == 2
    assert "Key 不能为空" in text
    assert "******" in text


def test_prompt_runtime_llm_key_supports_backspace() -> None:
    output = io.StringIO()
    chars = iter(["a", "b", "\b", "c", "\r"])

    key = prompt_runtime_llm_key(
        reader=lambda: next(chars),
        writer=output,
    )

    text = output.getvalue()
    assert key == "ac"
    assert "\b \b" in text


def test_prompt_runtime_llm_key_raises_keyboard_interrupt_on_eof() -> None:
    with pytest.raises(KeyboardInterrupt):
        prompt_runtime_llm_key(
            reader=lambda: "",
            writer=io.StringIO(),
        )


def test_wait_for_llm_retry_confirmation_raises_keyboard_interrupt_on_eof() -> None:
    with pytest.raises(KeyboardInterrupt):
        wait_for_llm_retry_confirmation(
            "LLM 服务暂时不可用，请稍后重试。",
            line_reader=lambda: "",
            writer=io.StringIO(),
        )


def test_app_config_prefers_runtime_llm_key_override(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://example.com/v1",
                "LLM_KEY=env-key",
                "LLM_MODEL=gpt-5.4-mini",
            ]
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(tmp_path, llm_key_override="runtime-key")

    assert config.llm_key == "runtime-key"


def test_app_config_prefers_process_environment_over_dotenv_values(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://dotenv.example/v1",
                "LLM_KEY=dotenv-key",
                "LLM_MODEL=dotenv-model",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("LLM_BASE_URL", "https://env.example/v1")
    monkeypatch.setenv("LLM_KEY", "env-key")
    monkeypatch.setenv("LLM_MODEL", "env-model")

    config = AppConfig.load(tmp_path)

    assert config.llm_base_url == "https://env.example/v1"
    assert config.llm_key == "env-key"
    assert config.llm_model == "env-model"


def test_app_config_does_not_auto_enable_local_proxy_when_proxy_url_is_missing(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://dotenv.example/v1",
                "LLM_KEY=dotenv-key",
                "LLM_MODEL=dotenv-model",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("oldironcrawler.config._local_proxy_is_ready", lambda _proxy_url: True)

    config = AppConfig.load(tmp_path)

    assert config.proxy_url == ""


def test_app_config_keeps_explicit_proxy_url_when_local_proxy_is_ready(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://dotenv.example/v1",
                "LLM_KEY=dotenv-key",
                "LLM_MODEL=dotenv-model",
                "PROXY_URL=http://127.0.0.1:7897",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("oldironcrawler.config._local_proxy_is_ready", lambda _proxy_url: True)

    config = AppConfig.load(tmp_path)

    assert config.proxy_url == "http://127.0.0.1:7897"


def test_app_config_discards_invalid_local_proxy_port(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://dotenv.example/v1",
                "LLM_KEY=dotenv-key",
                "LLM_MODEL=dotenv-model",
                "PROXY_URL=http://127.0.0.1:not-a-port",
            ]
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(tmp_path)

    assert config.proxy_url == ""


def test_persist_llm_key_strips_newlines(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("LLM_KEY=\nLLM_API_KEY=\n", encoding="utf-8")

    persist_llm_key(tmp_path, "good-key\nPROXY_URL=http://evil")
    env_text = env_path.read_text(encoding="utf-8")

    assert "LLM_KEY=good-keyPROXY_URL=http://evil" in env_text
    assert "\nPROXY_URL=http://evil\n" not in env_text


def test_build_artifact_stem_separates_suffixes() -> None:
    assert _build_artifact_stem(Path("clients.csv")) == "clients-csv"
    assert _build_artifact_stem(Path("clients.xlsx")) == "clients-xlsx"


def test_app_config_falls_back_when_default_websites_dir_is_denied(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LLM_BASE_URL=https://dotenv.example/v1",
                "LLM_KEY=dotenv-key",
                "LLM_MODEL=dotenv-model",
            ]
        ),
        encoding="utf-8",
    )
    real_mkdir = Path.mkdir

    def fake_mkdir(path_obj: Path, *args, **kwargs):
        if path_obj == tmp_path / "websites":
            raise PermissionError("denied")
        return real_mkdir(path_obj, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fake_mkdir)

    config = AppConfig.load(tmp_path)
    config.ensure_directories()

    assert config.websites_dir == tmp_path / "websites_runtime"
    assert config.websites_dir.exists()


def test_run_main_returns_clean_cancel_when_saved_key_load_is_interrupted(monkeypatch, capsys) -> None:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    import run

    monkeypatch.setattr(run, "_bootstrap", lambda: PROJECT_ROOT)

    def _raise_keyboard_interrupt(project_root: Path) -> str:
        raise KeyboardInterrupt

    monkeypatch.setattr(run, "_load_initial_llm_key", _raise_keyboard_interrupt)

    result = run.main()
    captured = capsys.readouterr()

    assert result == 130
    assert "已取消运行" in captured.out


def test_run_main_passes_saved_llm_key_to_app(monkeypatch) -> None:
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    import run
    import oldironcrawler.dashboard as dashboard_module

    seen: dict[str, object] = {}
    monkeypatch.setattr(run, "_bootstrap", lambda: PROJECT_ROOT)
    monkeypatch.setattr(run, "_load_initial_llm_key", lambda project_root: "runtime-key")

    def _fake_run_dashboard(project_root: Path, initial_key: str) -> int:
        seen["project_root"] = project_root
        seen["initial_key"] = initial_key
        return 0

    monkeypatch.setattr(dashboard_module, "run_dashboard", _fake_run_dashboard)

    result = run.main()

    assert result == 0
    assert seen["project_root"] == PROJECT_ROOT
    assert seen["initial_key"] == "runtime-key"


def test_resolve_runtime_root_uses_source_parent_when_not_frozen() -> None:
    entry = PROJECT_ROOT / "run.py"

    root = resolve_runtime_root(entry_file=entry, frozen=False, executable_path=None)

    assert root == PROJECT_ROOT


def test_resolve_runtime_root_uses_exe_parent_when_frozen(tmp_path: Path) -> None:
    entry = PROJECT_ROOT / "run.py"
    exe_path = tmp_path / "bundle" / "OldIronCrawler.exe"
    exe_path.parent.mkdir()
    exe_path.write_text("", encoding="utf-8")

    root = resolve_runtime_root(entry_file=entry, frozen=True, executable_path=exe_path)

    assert root == exe_path.parent


def test_raise_nofile_soft_limit_skips_when_resource_missing() -> None:
    raise_nofile_soft_limit(resource_module=None)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows runtime regression")
def test_importing_app_module_does_not_require_resource() -> None:
    script = "\n".join(
        [
            "import sys",
            f"sys.path.insert(0, r'{SRC_DIR}')",
            "import oldironcrawler.app",
            "print('ok')",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=False,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0
    assert "ModuleNotFoundError: No module named 'resource'" not in stderr
    assert "ok" in stdout


@pytest.mark.skipif(sys.platform != "win32", reason="Windows runtime regression")
def test_run_bootstrap_configures_utf8_output_under_cp1252() -> None:
    script = "\n".join(
        [
            "import run",
            "run._bootstrap()",
            "print('工作簿1.xlsx')",
        ]
    )
    env = os.environ.copy()
    env["PYTHONUTF8"] = "0"
    env["PYTHONIOENCODING"] = "cp1252"

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=False,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0
    assert "UnicodeEncodeError" not in stderr
    assert "工作簿1.xlsx" in stdout


@pytest.mark.skipif(sys.platform != "win32", reason="Windows runtime regression")
def test_choose_input_file_handles_chinese_output_under_cp1252(tmp_path: Path) -> None:
    target = tmp_path / "工作簿1.xlsx"
    target.write_text("", encoding="utf-8")
    (tmp_path / "smoke.txt").write_text("example.com", encoding="utf-8")

    script = "\n".join(
        [
            "import builtins",
            "import sys",
            "from pathlib import Path",
            f"sys.path.insert(0, r'{SRC_DIR}')",
            "from oldironcrawler.bootstrap import configure_stdio_utf8",
            "from oldironcrawler.importer import choose_input_file",
            "configure_stdio_utf8()",
            "builtins.input = lambda _prompt='': '工作簿1.xlsx'",
            f"chosen = choose_input_file(Path(r'{tmp_path}'))",
            "print(chosen.name)",
        ]
    )
    env = os.environ.copy()
    env["PYTHONUTF8"] = "0"
    env["PYTHONIOENCODING"] = "cp1252"

    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=False,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")

    assert result.returncode == 0
    assert "UnicodeEncodeError" not in stderr
    assert "工作簿1.xlsx" in stdout
