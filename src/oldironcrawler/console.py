from __future__ import annotations

import sys
from typing import Callable, TextIO


def prompt_runtime_llm_key(
    *,
    notice: str | None = None,
    reader: Callable[[], str] | None = None,
    writer: TextIO | None = None,
) -> str:
    read_char = reader or _build_char_reader()
    stream = writer or sys.stdout
    if notice:
        stream.write(f"{notice}\n")
        stream.flush()
    while True:
        stream.write("请输入 Crawler 公司官网系统爬虫密钥: ")
        stream.flush()
        value = _read_masked_line(read_char, stream).strip()
        stream.write("\n")
        stream.flush()
        if value:
            return value
        stream.write("Key 不能为空，请重新输入。\n")
        stream.flush()


def wait_for_llm_retry_confirmation(
    message: str,
    *,
    line_reader: Callable[[], str] | None = None,
    writer: TextIO | None = None,
) -> None:
    stream = writer or sys.stdout
    read_line = line_reader or _build_line_reader()
    stream.write(f"{message}\n")
    stream.write("按回车继续重试当前任务，按 Ctrl+C 退出。\n")
    stream.flush()
    _read_required_line(read_line)


def wait_for_enter(
    message: str = "按回车返回。",
    *,
    line_reader: Callable[[], str] | None = None,
    writer: TextIO | None = None,
) -> None:
    stream = writer or sys.stdout
    read_line = line_reader or _build_line_reader()
    stream.write(f"{message}\n")
    stream.flush()
    _read_required_line(read_line)


def _read_masked_line(read_char: Callable[[], str], stream: TextIO) -> str:
    chars: list[str] = []
    while True:
        char = read_char()
        if not char:
            raise KeyboardInterrupt
        if char == "\x03":
            raise KeyboardInterrupt
        if char in {"\x00", "\xe0"}:
            try:
                read_char()
            except Exception:
                return "".join(chars)
            continue
        if char in {"\r", "\n"}:
            return "".join(chars)
        if char in {"\b", "\x7f"}:
            if chars:
                chars.pop()
                stream.write("\b \b")
                stream.flush()
            continue
        if not char.isprintable():
            continue
        chars.append(char)
        stream.write("*")
        stream.flush()


def _read_required_line(read_line: Callable[[], str]) -> str:
    value = read_line()
    if value == "":
        raise KeyboardInterrupt
    return value


def _build_char_reader() -> Callable[[], str]:
    if sys.platform == "win32":
        import msvcrt

        return msvcrt.getwch
    import termios
    import tty

    def _read_char() -> str:
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            return sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    return _read_char


def _build_line_reader() -> Callable[[], str]:
    return sys.stdin.readline
