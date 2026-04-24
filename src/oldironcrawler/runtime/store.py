from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, fields
from pathlib import Path

from oldironcrawler.importer import ImportedWebsite


@dataclass
class SiteTask:
    id: int
    input_index: int
    website: str
    dedupe_key: str
    retry_count: int


@dataclass
class SiteResult:
    company_name: str
    representative: str
    emails: str
    website: str
    phones: str = ""
    evidence_url: str = ""
    evidence_quote: str = ""


@dataclass
class SiteStageMetrics:
    discover_ms: int = 0
    llm_pick_ms: int = 0
    fetch_pages_ms: int = 0
    llm_extract_ms: int = 0
    email_rule_ms: int = 0
    company_rule_ms: int = 0
    discovered_url_count: int = 0
    rep_url_count: int = 0
    email_url_count: int = 0
    target_url_count: int = 0
    fetched_page_count: int = 0


class RuntimeStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._thread_connections: dict[int, sqlite3.Connection] = {}
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        thread_id = threading.get_ident()
        with self._conn_lock:
            conn = self._thread_connections.get(thread_id)
            if conn is not None and _connection_is_alive(conn):
                return conn
            if conn is not None:
                _close_connection_quietly(conn)
            conn = self._open_connection()
            self._thread_connections[thread_id] = conn
            return conn

    def _open_connection(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def close(self) -> None:
        with self._conn_lock:
            connections = list(self._thread_connections.values())
            self._thread_connections.clear()
        for conn in connections:
            _close_connection_quietly(conn)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS job_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    input_name TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    total_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    input_index INTEGER NOT NULL,
                    raw_website TEXT NOT NULL,
                    website TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    company_name TEXT NOT NULL DEFAULT '',
                    representative TEXT NOT NULL DEFAULT '',
                    emails TEXT NOT NULL DEFAULT '',
                    phones TEXT NOT NULL DEFAULT '',
                    evidence_url TEXT NOT NULL DEFAULT '',
                    evidence_quote TEXT NOT NULL DEFAULT '',
                    discover_ms INTEGER NOT NULL DEFAULT 0,
                    llm_pick_ms INTEGER NOT NULL DEFAULT 0,
                    fetch_pages_ms INTEGER NOT NULL DEFAULT 0,
                    llm_extract_ms INTEGER NOT NULL DEFAULT 0,
                    email_rule_ms INTEGER NOT NULL DEFAULT 0,
                    company_rule_ms INTEGER NOT NULL DEFAULT 0,
                    discovered_url_count INTEGER NOT NULL DEFAULT 0,
                    rep_url_count INTEGER NOT NULL DEFAULT 0,
                    email_url_count INTEGER NOT NULL DEFAULT 0,
                    target_url_count INTEGER NOT NULL DEFAULT 0,
                    fetched_page_count INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_sites_status_input
                ON sites(status, retry_count, input_index);

                CREATE TABLE IF NOT EXISTS learned_tokens (
                    kind TEXT NOT NULL,
                    token TEXT NOT NULL,
                    weight INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(kind, token)
                );
                """
            )
            self._ensure_site_text_columns(conn)
            self._ensure_site_metrics_columns(conn)

    def _ensure_site_text_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sites)").fetchall()
        }
        if "phones" not in existing:
            conn.execute("ALTER TABLE sites ADD COLUMN phones TEXT NOT NULL DEFAULT ''")

    def _ensure_site_metrics_columns(self, conn: sqlite3.Connection) -> None:
        existing = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sites)").fetchall()
        }
        for name in _METRIC_COLUMNS:
            if name in existing:
                continue
            conn.execute(f"ALTER TABLE sites ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0")

    def prepare_job(self, *, input_name: str, fingerprint: str, rows: list[ImportedWebsite]) -> None:
        with self._write_lock, self._connect() as conn:
            current = conn.execute("SELECT input_name, fingerprint FROM job_meta WHERE id = 1").fetchone()
            if current is not None and current["input_name"] == input_name and current["fingerprint"] == fingerprint:
                existing = conn.execute("SELECT COUNT(*) AS cnt FROM sites").fetchone()
                if existing is not None and int(existing["cnt"] or 0) > 0:
                    return
            conn.executescript(
                """
                DELETE FROM job_meta;
                DELETE FROM sites;
                DELETE FROM sqlite_sequence WHERE name = 'sites';
                """
            )
            conn.execute(
                "INSERT INTO job_meta(id, input_name, fingerprint, total_count) VALUES(1, ?, ?, ?)",
                (input_name, fingerprint, len(rows)),
            )
            conn.executemany(
                """
                INSERT INTO sites(input_index, raw_website, website, dedupe_key)
                VALUES(?, ?, ?, ?)
                """,
                [(row.input_index, row.raw_website, row.website, row.dedupe_key) for row in rows],
            )

    def reset_running_tasks(self) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'pending',
                    started_at = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0
                WHERE status = 'running'
                """
            )

    def reset_completed_job_for_rerun(self) -> bool:
        with self._write_lock, self._connect() as conn:
            counts = {
                status: int(
                    conn.execute("SELECT COUNT(*) AS cnt FROM sites WHERE status = ?", (status,)).fetchone()["cnt"]
                )
                for status in ("pending", "running", "failed_temp", "done", "dropped")
            }
            total = sum(counts.values())
            if total <= 0:
                return False
            if counts["pending"] > 0 or counts["running"] > 0 or counts["failed_temp"] > 0:
                return False
            conn.execute(
                """
                UPDATE sites
                SET status = 'pending',
                    retry_count = 0,
                    last_error = '',
                    company_name = '',
                    representative = '',
                    emails = '',
                    phones = '',
                    evidence_url = '',
                    evidence_quote = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0,
                    started_at = '',
                    finished_at = ''
                """
            )
            return True

    def claim_next_site(self) -> SiteTask | None:
        with self._write_lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, input_index, website, dedupe_key, retry_count
                FROM sites
                WHERE status IN ('pending', 'failed_temp')
                ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, input_index ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE sites
                SET status = 'running',
                    started_at = CURRENT_TIMESTAMP,
                    finished_at = '',
                    last_error = '',
                    discover_ms = 0,
                    llm_pick_ms = 0,
                    fetch_pages_ms = 0,
                    llm_extract_ms = 0,
                    email_rule_ms = 0,
                    company_rule_ms = 0,
                    discovered_url_count = 0,
                    rep_url_count = 0,
                    email_url_count = 0,
                    target_url_count = 0,
                    fetched_page_count = 0
                WHERE id = ?
                """,
                (int(row["id"]),),
            )
            return SiteTask(
                id=int(row["id"]),
                input_index=int(row["input_index"]),
                website=str(row["website"]),
                dedupe_key=str(row["dedupe_key"]),
                retry_count=int(row["retry_count"] or 0),
            )

    def mark_done(self, site_id: int, result: SiteResult) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'done',
                    company_name = ?,
                    representative = ?,
                    emails = ?,
                    website = ?,
                    phones = ?,
                    evidence_url = ?,
                    evidence_quote = ?,
                    finished_at = CURRENT_TIMESTAMP,
                    last_error = ''
                WHERE id = ?
                """,
                (
                    result.company_name,
                    result.representative,
                    result.emails,
                    result.website,
                    result.phones,
                    result.evidence_url,
                    result.evidence_quote,
                    site_id,
                ),
            )

    def update_stage_metrics(self, site_id: int, metrics: SiteStageMetrics) -> None:
        values = tuple(int(getattr(metrics, name) or 0) for name in _METRIC_COLUMNS)
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET discover_ms = ?,
                    llm_pick_ms = ?,
                    fetch_pages_ms = ?,
                    llm_extract_ms = ?,
                    email_rule_ms = ?,
                    company_rule_ms = ?,
                    discovered_url_count = ?,
                    rep_url_count = ?,
                    email_url_count = ?,
                    target_url_count = ?,
                    fetched_page_count = ?
                WHERE id = ?
                """,
                (*values, site_id),
            )

    def load_stage_metrics(self, site_id: int) -> SiteStageMetrics:
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT {', '.join(_METRIC_COLUMNS)} FROM sites WHERE id = ?",
                (site_id,),
            ).fetchone()
        if row is None:
            return SiteStageMetrics()
        return SiteStageMetrics(**{name: int(row[name] or 0) for name in _METRIC_COLUMNS})

    def mark_failed(self, site_id: int, error_text: str) -> str:
        with self._write_lock, self._connect() as conn:
            row = conn.execute("SELECT retry_count FROM sites WHERE id = ?", (site_id,)).fetchone()
            retry_count = int(row["retry_count"] or 0) if row is not None else 0
            max_retry_count = _max_retry_count_for_error(error_text)
            if retry_count < max_retry_count:
                conn.execute(
                    """
                    UPDATE sites
                    SET status = 'failed_temp',
                        retry_count = retry_count + 1,
                        last_error = ?,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (error_text, site_id),
                )
                return "failed_temp"
            conn.execute(
                """
                UPDATE sites
                SET status = 'dropped',
                    retry_count = retry_count + 1,
                    last_error = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (error_text, site_id),
            )
            return "dropped"

    def mark_dropped(self, site_id: int, error_text: str) -> None:
        with self._write_lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET status = 'dropped',
                    last_error = ?,
                    finished_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (error_text, site_id),
            )

    def progress(self) -> dict[str, int]:
        with self._connect() as conn:
            counts = {
                status: int(
                    conn.execute("SELECT COUNT(*) AS cnt FROM sites WHERE status = ?", (status,)).fetchone()["cnt"]
                )
                for status in ("pending", "running", "done", "failed_temp", "dropped")
            }
            counts["total"] = int(conn.execute("SELECT COUNT(*) AS cnt FROM sites").fetchone()["cnt"])
            return counts

    def delivery_rows(self) -> list[dict[str, str]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT company_name, representative, emails, phones, website
                FROM sites
                WHERE status IN ('done', 'dropped')
                ORDER BY input_index ASC
                """
            ).fetchall()
        return [
            {
                "company_name": str(row["company_name"] or ""),
                "representative": str(row["representative"] or ""),
                "emails": str(row["emails"] or ""),
                "phones": str(row["phones"] or ""),
                "website": str(row["website"] or ""),
            }
            for row in rows
        ]

    def load_learned_tokens(self, kind: str) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT token, weight FROM learned_tokens WHERE kind = ? ORDER BY weight DESC, token ASC",
                (kind,),
            ).fetchall()
        return {str(row["token"]): int(row["weight"] or 0) for row in rows}

    def bump_learned_tokens(self, kind: str, tokens: list[str]) -> None:
        cleaned = [str(token or "").strip().lower() for token in tokens if str(token or "").strip()]
        if not cleaned:
            return
        with self._write_lock, self._connect() as conn:
            for token in cleaned:
                conn.execute(
                    """
                    INSERT INTO learned_tokens(kind, token, weight)
                    VALUES(?, ?, 1)
                    ON CONFLICT(kind, token) DO UPDATE SET
                        weight = learned_tokens.weight + 1,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (kind, token),
                )


def _connection_is_alive(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT 1")
        return True
    except sqlite3.Error:
        return False


def _close_connection_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except sqlite3.Error:
        return None


_METRIC_COLUMNS = tuple(field.name for field in fields(SiteStageMetrics))


def _max_retry_count_for_error(error_text: str) -> int:
    lowered = str(error_text or "").lower()
    if any(
        token in lowered
        for token in (
            "tls connect error",
            "tlsv1_alert",
            "sslv3_alert_handshake_failure",
            "openssl_internal",
            "getaddrinfo() thread failed to start",
            "thread failed to start",
            "request_slot_timeout",
            "llm_queue_timeout",
            "resource temporarily unavailable",
            "[errno 35]",
            "page_batch_timeout",
            "empty_page_batch",
            "site_deadline_exceeded",
            "temporary_request:",
        )
    ):
        return 2
    return 1
