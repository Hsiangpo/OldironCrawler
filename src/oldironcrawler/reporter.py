from __future__ import annotations

import csv
from pathlib import Path

from oldironcrawler.runtime.store import SiteStageMetrics


def print_site_result(
    *,
    completed_index: int,
    total: int,
    website: str,
    company_name: str,
    representative: str,
    emails: str,
    reason: str = "",
    stage_metrics: SiteStageMetrics | None = None,
) -> None:
    print(f"[{completed_index}/{total}] {website}", flush=True)
    print(f"  公司名: {_display(company_name)}", flush=True)
    print(f"  姓名: {_display(representative)}", flush=True)
    print(f"  邮箱: {_display(emails)}", flush=True)
    if stage_metrics is not None:
        print(f"  阶段耗时: {_format_stage_timing(stage_metrics)}", flush=True)
        print(f"  页面统计: {_format_stage_counts(stage_metrics)}", flush=True)
    if str(reason or "").strip():
        print(f"  原因: {reason.strip()}", flush=True)
    print("  ------------------------------------------------------------", flush=True)


def print_progress_heartbeat(*, total: int, done: int, running: int, dropped: int, pending: int) -> None:
    print(
        f"[进度] total={total} done={done} running={running} dropped={dropped} pending={pending}",
        flush=True,
    )


def write_delivery_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["company_name", "representative", "emails", "website"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _display(value: str) -> str:
    text = str(value or "").strip()
    return text if text else "未找到"


def _format_stage_timing(metrics: SiteStageMetrics) -> str:
    return " | ".join(
        [
            f"发现 {metrics.discover_ms / 1000:.1f}s",
            f"LLM选页 {metrics.llm_pick_ms / 1000:.1f}s",
            f"抓页 {metrics.fetch_pages_ms / 1000:.1f}s",
            f"LLM抽取 {metrics.llm_extract_ms / 1000:.1f}s",
            f"邮箱规则 {metrics.email_rule_ms / 1000:.1f}s",
            f"公司规则 {metrics.company_rule_ms / 1000:.1f}s",
        ]
    )


def _format_stage_counts(metrics: SiteStageMetrics) -> str:
    return " | ".join(
        [
            f"候选 {metrics.discovered_url_count}",
            f"负责人页 {metrics.rep_url_count}",
            f"邮箱页 {metrics.email_url_count}",
            f"目标页 {metrics.target_url_count}",
            f"实抓 {metrics.fetched_page_count}",
        ]
    )
