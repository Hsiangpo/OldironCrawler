from __future__ import annotations

import os
import shutil
import subprocess
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from oldironcrawler import app as app_module
from oldironcrawler.config import resolve_websites_dir
from oldironcrawler.console import prompt_runtime_llm_key, wait_for_enter
from oldironcrawler.extractor.llm_client import LlmConfigurationError
from oldironcrawler.importer import list_input_files


@dataclass
class DashboardSession:
    project_root: Path
    current_key: str
    selected_input: Path | None = None
    concurrency: int = 32
    site_timeout_seconds: int = 180
    last_delivery_path: Path | None = None


def run_dashboard(project_root: Path, initial_key: str) -> int:
    session = DashboardSession(
        project_root=project_root.resolve(),
        current_key=str(initial_key or "").strip(),
    )
    _ensure_key_before_panel(session)
    while True:
        _render_panel(
            "OldIronCrawler 控制面板",
            [
                f"Key 状态：{_display_key_status(session)}",
                f"当前文件：{session.selected_input.name if session.selected_input else '未选择'}",
                f"可爬表数量：{len(list_input_files(_get_websites_dir(session.project_root)))}",
                f"结果文件数量：{len(_list_output_results(session.project_root / 'output'))}",
                f"并发设置：{session.concurrency}",
                f"单站等待上限：{session.site_timeout_seconds} 秒",
                "",
                "1. 开始抓取",
                "2. 打开 websites 文件夹",
                "3. 打开 output 文件夹",
                "4. 系统配置",
                "5. 退出程序",
            ],
        )
        choice = input("请输入菜单序号: ").strip().upper()
        if choice == "1":
            _handle_start_crawl(session)
            continue
        if choice == "2":
            _handle_open_websites(session)
            continue
        if choice == "3":
            _handle_open_output(session)
            continue
        if choice == "4":
            if _handle_system_config(session) == "exit":
                return 0
            continue
        if choice == "5":
            return 0
        _show_message("输入无效，请重新选择。")


def _ensure_key_before_panel(session: DashboardSession) -> None:
    while True:
        if not session.current_key:
            session.current_key = prompt_runtime_llm_key()
        try:
            config = app_module._load_runtime_config(session.project_root, session.current_key)
            app_module._apply_runtime_preferences(
                config,
                concurrency=session.concurrency,
                site_timeout_seconds=session.site_timeout_seconds,
            )
            app_module._validate_llm_runtime(config)
            app_module._persist_runtime_llm_key(session.project_root, session.current_key)
            return
        except LlmConfigurationError as exc:
            session.current_key = app_module._recover_runtime_llm_key(session.current_key, exc)


def _handle_start_crawl(session: DashboardSession) -> None:
    selected = _select_input_file(session)
    if selected is None:
        return
    session.selected_input = selected
    try:
        result = app_module.run_selected_input(
            session.project_root,
            session.current_key,
            selected,
            concurrency=session.concurrency,
            site_timeout_seconds=session.site_timeout_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        _show_message(f"抓取过程中出现未处理错误：{exc}")
        return
    session.current_key = result.effective_key
    session.last_delivery_path = result.delivery_path
    wait_for_enter(f"任务完成：{result.delivery_path}\n按回车返回主菜单。")


def _select_input_file(session: DashboardSession) -> Path | None:
    while True:
        files = list_input_files(_get_websites_dir(session.project_root))
        _render_panel("选择爬取表", _build_file_select_lines(files, session.selected_input))
        raw = input("你的选择: ").strip()
        if raw == "0":
            return None
        if not files:
            _show_message("还没有可抓取表。")
            continue
        matched = _match_file_choice(files, raw)
        if matched is not None:
            return matched
        _show_message("文件序号或文件名无效，请重新选择。")


def _handle_open_websites(session: DashboardSession) -> None:
    websites_dir = _get_websites_dir(session.project_root)
    files = list_input_files(websites_dir)
    lines = [f"当前共 {len(files)} 个可抓取表：", ""]
    if files:
        lines.extend(f"  {index}. {path.name}" for index, path in enumerate(files, start=1))
    else:
        lines.append("当前没有可抓取表。")
    lines.extend(["", f"文件夹路径：{websites_dir}"])
    _render_panel("websites 文件夹", lines)
    _open_folder(websites_dir)
    wait_for_enter("已尝试打开 websites 文件夹，按回车返回主菜单。")


def _handle_open_output(session: DashboardSession) -> None:
    results = _list_output_results(session.project_root / "output")
    lines = [f"当前共 {len(results)} 个结果文件：", ""]
    if results:
        lines.extend(f"  {index}. {path.name}" for index, path in enumerate(results, start=1))
    else:
        lines.append("当前还没有结果文件。")
    if session.last_delivery_path is not None:
        lines.extend(["", f"最近结果：{session.last_delivery_path.name}"])
    lines.extend(["", f"文件夹路径：{session.project_root / 'output'}"])
    _render_panel("output 文件夹", lines)
    _open_folder(session.project_root / "output")
    wait_for_enter("已尝试打开 output 文件夹，按回车返回主菜单。")


def _handle_system_config(session: DashboardSession) -> str | None:
    while True:
        _render_panel(
            "系统配置",
            _build_system_config_lines(
                key_status=_display_key_status(session),
                concurrency=session.concurrency,
                site_timeout_seconds=session.site_timeout_seconds,
            ),
        )
        choice = input("请输入菜单序号: ").strip().upper()
        if choice == "1":
            if _handle_key_settings(session) == "exit":
                return "exit"
            continue
        if choice == "2":
            _handle_numeric_setting(
                title="并发设置",
                current_value=session.concurrency,
                min_value=1,
                max_value=64,
                apply_value=lambda value: setattr(session, "concurrency", value),
                description="请输入新的并发值，范围 1-64。",
            )
            continue
        if choice == "3":
            _handle_numeric_setting(
                title="单站等待上限",
                current_value=session.site_timeout_seconds,
                min_value=60,
                max_value=600,
                apply_value=lambda value: setattr(session, "site_timeout_seconds", value),
                description="请输入新的秒数，范围 60-600。",
            )
            continue
        if choice == "4":
            return None
        _show_message("输入无效，请重新选择。")


def _handle_key_settings(session: DashboardSession) -> str | None:
    while True:
        _render_panel(
            "Key 设置",
            _build_key_settings_lines(_display_key_status(session)),
        )
        choice = input("请输入菜单序号: ").strip().upper()
        if choice == "1":
            session.current_key = prompt_runtime_llm_key()
            _ensure_key_before_panel(session)
            _show_message("Key 已更新并鉴权成功。")
            continue
        if choice == "2":
            session.current_key = ""
            app_module._persist_runtime_llm_key(session.project_root, "")
            wait_for_enter("Key 已删除，系统将退出，请重新启动程序。")
            return "exit"
        if choice == "3":
            return None
        _show_message("输入无效，请重新选择。")


def _handle_numeric_setting(
    *,
    title: str,
    current_value: int,
    min_value: int,
    max_value: int,
    apply_value,
    description: str,
) -> None:
    while True:
        _render_panel(
            title,
            [
                f"当前值：{current_value}",
                description,
                "输入 0 返回系统配置。",
            ],
        )
        raw = input("请输入新的数值: ").strip().upper()
        if raw == "0":
            return
        try:
            value = int(raw)
        except ValueError:
            _show_message("输入不是有效数字，请重新输入。")
            continue
        if value < min_value or value > max_value:
            _show_message(f"输入超出范围，请输入 {min_value}-{max_value} 之间的数字。")
            continue
        apply_value(value)
        _show_message(f"{title}已更新为 {value}。")
        return


def _match_file_choice(files: list[Path], raw: str) -> Path | None:
    name_map = {path.name.lower(): path for path in files}
    matched = name_map.get(raw.lower())
    if matched is not None:
        return matched
    try:
        choice = int(raw)
    except ValueError:
        return None
    if 1 <= choice <= len(files):
        return files[choice - 1]
    return None


def _list_output_results(output_dir: Path) -> list[Path]:
    if not output_dir.exists():
        return []
    results = []
    for item in output_dir.iterdir():
        if not item.is_file():
            continue
        if item.name.startswith("结果会输出到这里"):
            continue
        results.append(item)
    return sorted(results, key=lambda item: item.name.lower())


def _get_websites_dir(project_root: Path) -> Path:
    return resolve_websites_dir(project_root)


def _open_folder(path: Path) -> None:
    try:
        if os.name == "nt":
            os.startfile(str(path))
            return
        if os.name == "posix":
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        return


def _display_key_status(session: DashboardSession) -> str:
    if not session.current_key:
        return "未设置"
    if len(session.current_key) <= 4:
        return "*" * len(session.current_key)
    return "*" * (len(session.current_key) - 4) + session.current_key[-4:]


def _show_message(message: str) -> None:
    _render_panel("提示", [message])
    wait_for_enter("按回车继续。")


def _render_panel(title: str, lines: list[str]) -> None:
    _clear_screen()
    width = _resolve_panel_width()
    print("+" + "-" * width + "+")
    print("|" + _center_panel_text(title, width) + "|")
    print("+" + "-" * width + "+")
    for line in lines:
        for chunk in _wrap_line(line, width):
            print("|" + _pad_panel_text(chunk, width) + "|")
    print("+" + "-" * width + "+")


def _wrap_line(text: str, width: int) -> list[str]:
    content = str(text or "")
    if not content:
        return [""]
    chunks: list[str] = []
    current = ""
    current_width = 0
    for char in content:
        char_width = _char_display_width(char)
        if current and current_width + char_width > width:
            chunks.append(current)
            current = char
            current_width = char_width
            continue
        current += char
        current_width += char_width
    if current or not chunks:
        chunks.append(current)
    return chunks


def _build_file_select_lines(files: list[Path], selected_input: Path | None) -> list[str]:
    lines = [
        f"当前文件：{selected_input.name if selected_input else '未选择'}",
        "",
    ]
    if files:
        lines.append(f"共发现 {len(files)} 个可抓取表：")
        lines.extend(f"  {index}. {path.name}" for index, path in enumerate(files, start=1))
    else:
        lines.append("当前没有可抓取表，请先把 txt/csv/xlsx 放进 websites 文件夹。")
    lines.extend(["", "0. 返回主菜单", "请输入序号选择文件。"])
    return lines


def _build_system_config_lines(*, key_status: str, concurrency: int, site_timeout_seconds: int) -> list[str]:
    return [
        f"Key 状态：{key_status}",
        f"并发设置：{concurrency}",
        f"单站等待上限：{site_timeout_seconds} 秒",
        "",
        "1. Key 设置",
        "2. 并发设置",
        "3. 单站等待上限",
        "4. 返回主菜单",
    ]


def _build_key_settings_lines(key_status: str) -> list[str]:
    return [
        f"当前状态：{key_status}",
        "",
        "1. 更换 Key",
        "2. 删除 Key 并退出系统",
        "3. 返回系统配置",
    ]


def _pad_panel_text(text: str, width: int) -> str:
    content = str(text or "")
    return content + " " * max(width - _display_width(content), 0)


def _center_panel_text(text: str, width: int) -> str:
    content = str(text or "")
    content_width = _display_width(content)
    if content_width >= width:
        return _pad_panel_text(content, width)
    left = (width - content_width) // 2
    right = width - content_width - left
    return (" " * left) + content + (" " * right)


def _display_width(text: str) -> int:
    return sum(_char_display_width(char) for char in str(text or ""))


def _char_display_width(char: str) -> int:
    if not char:
        return 0
    if unicodedata.combining(char):
        return 0
    return 2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1


def _resolve_panel_width() -> int:
    columns = shutil.get_terminal_size(fallback=(100, 30)).columns
    return max(min(columns - 4, 76), 56)


def _clear_screen() -> None:
    command = "cls" if os.name == "nt" else "clear"
    os.system(command)
