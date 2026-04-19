from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    project_root = Path(__file__).resolve().parent
    src_dir = project_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main() -> int:
    _bootstrap()
    from oldironcrawler.app import run_interactive

    try:
        return run_interactive()
    except Exception as exc:  # noqa: BLE001
        print(f"启动失败: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
