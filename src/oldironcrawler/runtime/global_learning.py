from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

_SUCCESS_DELTA = 3
_FAILURE_DELTA = 1
_MIN_SCORE = -12
_MAX_SCORE = 60
_DECAY_WINDOW_DAYS = 14
_DECAY_STEP = 1
_POSITIVE_DECAY_FLOOR = 1
_SQLITE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


class GlobalLearningStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._write_lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._thread_connections: dict[int, sqlite3.Connection] = {}
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def close(self) -> None:
        with self._conn_lock:
            connections = list(self._thread_connections.values())
            self._thread_connections.clear()
        for conn in connections:
            _close_connection_quietly(conn)

    def load_scores(self, kind: str) -> dict[str, int]:
        with self._write_lock, self._connect() as conn:
            _apply_lazy_positive_decay(conn, kind)
            rows = conn.execute(
                """
                SELECT feature, score
                FROM learning_features
                WHERE kind = ?
                ORDER BY score DESC, feature ASC
                """,
                (kind,),
            ).fetchall()
        return {
            str(row["feature"]): int(row["score"] or 0)
            for row in rows
            if int(row["score"] or 0) != 0
        }

    def record_success(self, kind: str, features: list[str]) -> None:
        self._record(kind, features, success_delta=_SUCCESS_DELTA, failure_delta=0)

    def record_failure(self, kind: str, features: list[str]) -> None:
        self._record(kind, features, success_delta=0, failure_delta=_FAILURE_DELTA)

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
        conn = sqlite3.connect(str(self._db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS learning_features (
                    kind TEXT NOT NULL,
                    feature TEXT NOT NULL,
                    score INTEGER NOT NULL DEFAULT 0,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(kind, feature)
                );
                """
            )

    def _record(
        self,
        kind: str,
        features: list[str],
        *,
        success_delta: int,
        failure_delta: int,
    ) -> None:
        cleaned = _clean_features(features)
        if not cleaned:
            return
        with self._write_lock, self._connect() as conn:
            for feature in cleaned:
                conn.execute(
                    """
                    INSERT INTO learning_features(
                        kind,
                        feature,
                        score,
                        success_count,
                        failure_count
                    )
                    VALUES(?, ?, ?, ?, ?)
                    ON CONFLICT(kind, feature) DO UPDATE SET
                        score = MIN(
                            MAX(learning_features.score + ?, ?),
                            ?
                        ),
                        success_count = learning_features.success_count + ?,
                        failure_count = learning_features.failure_count + ?,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        kind,
                        feature,
                        success_delta - failure_delta,
                        1 if success_delta else 0,
                        1 if failure_delta else 0,
                        success_delta - failure_delta,
                        _MIN_SCORE,
                        _MAX_SCORE,
                        1 if success_delta else 0,
                        1 if failure_delta else 0,
                    ),
                )


def _clean_features(features: list[str]) -> list[str]:
    cleaned: list[str] = []
    for feature in features:
        value = str(feature or "").strip().lower()
        if not value or value in cleaned:
            continue
        cleaned.append(value)
    return cleaned


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


def _apply_lazy_positive_decay(conn: sqlite3.Connection, kind: str) -> None:
    rows = conn.execute(
        """
        SELECT feature, score, updated_at
        FROM learning_features
        WHERE kind = ? AND score > 0
        """,
        (kind,),
    ).fetchall()
    if not rows:
        return
    now = datetime.now(timezone.utc)
    now_text = now.strftime(_SQLITE_TIMESTAMP_FORMAT)
    for row in rows:
        score = int(row["score"] or 0)
        updated_at = str(row["updated_at"] or "").strip()
        decayed_score, should_refresh = _decay_positive_score(score, updated_at, now)
        if not should_refresh:
            continue
        conn.execute(
            """
            UPDATE learning_features
            SET score = ?, updated_at = ?
            WHERE kind = ? AND feature = ?
            """,
            (decayed_score, now_text, kind, str(row["feature"])),
        )


def _decay_positive_score(score: int, updated_at: str, now: datetime) -> tuple[int, bool]:
    if score <= 0:
        return score, False
    last_updated = _parse_sqlite_timestamp(updated_at)
    if last_updated is None:
        return score, False
    elapsed_seconds = max((now - last_updated).total_seconds(), 0.0)
    decay_windows = int(elapsed_seconds // (_DECAY_WINDOW_DAYS * 24 * 60 * 60))
    if decay_windows <= 0:
        return score, False
    next_score = max(score - (decay_windows * _DECAY_STEP), _POSITIVE_DECAY_FLOOR)
    return next_score, True


def _parse_sqlite_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, _SQLITE_TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
