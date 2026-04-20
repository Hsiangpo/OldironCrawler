from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> Path:
    frozen = bool(getattr(sys, "frozen", False))
    entry_file = Path(__file__).resolve()
    if not frozen:
        src_dir = entry_file.parent / "src"
        if str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
    from oldironcrawler.bootstrap import configure_stdio_utf8, resolve_runtime_root

    configure_stdio_utf8()
    executable_path = Path(sys.executable).resolve() if frozen else None
    return resolve_runtime_root(
        entry_file=entry_file,
        frozen=frozen,
        executable_path=executable_path,
    )


def _load_initial_llm_key(project_root: Path) -> str:
    from oldironcrawler.config import read_saved_llm_key

    return read_saved_llm_key(project_root)


def main() -> int:
    try:
        project_root = _bootstrap()
        llm_key = _load_initial_llm_key(project_root)
        from oldironcrawler.dashboard import run_dashboard

        return run_dashboard(project_root, initial_key=llm_key)
    except KeyboardInterrupt:
        print("已取消运行。")
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"启动失败: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
