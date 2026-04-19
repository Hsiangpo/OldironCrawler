from __future__ import annotations

import resource
from pathlib import Path

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.llm_client import WebsiteLlmClient
from oldironcrawler.importer import choose_input_file, compute_rows_fingerprint, load_websites
from oldironcrawler.runner import run_crawl_session
from oldironcrawler.runtime.store import RuntimeStore


def run_interactive() -> int:
    project_root = Path(__file__).resolve().parents[2]
    _raise_nofile_soft_limit()
    config = AppConfig.load(project_root)
    config.ensure_directories()
    config.validate()
    _validate_llm_runtime(config)

    input_path = choose_input_file(config.websites_dir)
    rows = load_websites(input_path)
    if not rows:
        raise RuntimeError(f"输入文件没有识别到任何有效网站：{input_path.name}")

    db_path = config.runtime_dir / f"{input_path.stem}.sqlite3"
    delivery_path = config.delivery_dir / f"{input_path.stem}.csv"
    store = RuntimeStore(db_path)
    try:
        store.prepare_job(
            input_name=input_path.name,
            fingerprint=compute_rows_fingerprint(rows),
            rows=rows,
        )
        store.reset_running_tasks()
        rerun_reset = store.reset_completed_job_for_rerun()
        progress = store.progress()
        if rerun_reset:
            print(f"检测到 {input_path.name} 上次已经跑完，本次已重置后重新跑。", flush=True)
        print(f"开始任务：file={input_path.name} total={progress['total']} db={db_path}", flush=True)
        run_crawl_session(config, store, delivery_path)
        print(f"交付完成：{delivery_path}", flush=True)
        return 0
    finally:
        store.close()


def _validate_llm_runtime(config: AppConfig) -> None:
    llm = WebsiteLlmClient(
        api_key=config.llm_key,
        base_url=config.llm_base_url,
        model=config.llm_model,
        api_style=config.llm_api_style,
        reasoning_effort=config.llm_reasoning_effort,
        proxy_url=config.proxy_url,
        timeout_seconds=config.request_timeout_seconds * 2,
        concurrency_limit=config.llm_concurrency,
    )
    try:
        llm.ping()
    finally:
        llm.close()


def _raise_nofile_soft_limit(target: int = 65536) -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = max(int(target), 1024)
    if soft >= desired:
        return
    next_soft = desired if hard == resource.RLIM_INFINITY else min(desired, hard)
    if next_soft <= soft:
        return
    resource.setrlimit(resource.RLIMIT_NOFILE, (next_soft, hard))
