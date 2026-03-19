from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._initialize()

    def _initialize(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS pages (
            url TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            text_content TEXT NOT NULL,
            links_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS crawl_jobs (
            job_id TEXT PRIMARY KEY,
            origin_url TEXT NOT NULL,
            max_depth INTEGER NOT NULL,
            rate_limit REAL NOT NULL,
            queue_capacity INTEGER NOT NULL,
            worker_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            pages_discovered INTEGER NOT NULL,
            pages_indexed INTEGER NOT NULL,
            queue_backpressure_count INTEGER NOT NULL,
            rate_backpressure_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_discoveries (
            job_id TEXT NOT NULL,
            url TEXT NOT NULL,
            depth INTEGER NOT NULL,
            PRIMARY KEY (job_id, url)
        );

        CREATE TABLE IF NOT EXISTS job_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            logged_at TEXT NOT NULL,
            message TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_job_discoveries_url
            ON job_discoveries (url);

        CREATE INDEX IF NOT EXISTS idx_job_logs_job_id
            ON job_logs (job_id, id DESC);
        """
        with self._lock:
            self._conn.executescript(schema)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def mark_incomplete_jobs_interrupted(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE crawl_jobs
                SET status = 'interrupted'
                WHERE status IN ('created', 'running')
                """
            )
            self._conn.commit()

    def load_pages(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT url, title, text_content, links_json, fetched_at, status_code, error
                FROM pages
                """
            ).fetchall()
        pages: list[dict[str, Any]] = []
        for row in rows:
            pages.append(
                {
                    "url": row["url"],
                    "title": row["title"],
                    "text": row["text_content"],
                    "links": json.loads(row["links_json"]),
                    "fetched_at": row["fetched_at"],
                    "status_code": row["status_code"],
                    "error": row["error"],
                }
            )
        return pages

    def save_page(self, page: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pages (url, title, text_content, links_json, fetched_at, status_code, error)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title = excluded.title,
                    text_content = excluded.text_content,
                    links_json = excluded.links_json,
                    fetched_at = excluded.fetched_at,
                    status_code = excluded.status_code,
                    error = excluded.error
                """,
                (
                    page["url"],
                    page["title"],
                    page["text"],
                    json.dumps(page["links"]),
                    page["fetched_at"],
                    page["status_code"],
                    page["error"],
                ),
            )
            self._conn.commit()

    def load_job_summaries(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT job_id, origin_url, max_depth, rate_limit, queue_capacity,
                       worker_count, status, created_at, updated_at,
                       pages_discovered, pages_indexed,
                       queue_backpressure_count, rate_backpressure_count
                FROM crawl_jobs
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def save_job_summary(self, summary: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO crawl_jobs (
                    job_id, origin_url, max_depth, rate_limit, queue_capacity,
                    worker_count, status, created_at, updated_at,
                    pages_discovered, pages_indexed,
                    queue_backpressure_count, rate_backpressure_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    origin_url = excluded.origin_url,
                    max_depth = excluded.max_depth,
                    rate_limit = excluded.rate_limit,
                    queue_capacity = excluded.queue_capacity,
                    worker_count = excluded.worker_count,
                    status = excluded.status,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    pages_discovered = excluded.pages_discovered,
                    pages_indexed = excluded.pages_indexed,
                    queue_backpressure_count = excluded.queue_backpressure_count,
                    rate_backpressure_count = excluded.rate_backpressure_count
                """,
                (
                    summary["job_id"],
                    summary["origin_url"],
                    summary["max_depth"],
                    summary["rate_limit"],
                    summary["queue_capacity"],
                    summary["worker_count"],
                    summary["status"],
                    summary["created_at"],
                    summary["updated_at"],
                    summary["pages_discovered"],
                    summary["pages_indexed"],
                    summary["queue_backpressure_count"],
                    summary["rate_backpressure_count"],
                ),
            )
            self._conn.commit()

    def save_job_discovery(self, job_id: str, url: str, depth: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO job_discoveries (job_id, url, depth)
                VALUES (?, ?, ?)
                """,
                (job_id, url, depth),
            )
            self._conn.commit()

    def load_discoveries(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT d.job_id, d.url, d.depth, j.origin_url
                FROM job_discoveries AS d
                JOIN crawl_jobs AS j ON j.job_id = d.job_id
                ORDER BY d.depth ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def append_job_log(self, job_id: str, logged_at: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO job_logs (job_id, logged_at, message)
                VALUES (?, ?, ?)
                """,
                (job_id, logged_at, message),
            )
            self._conn.commit()

    def load_job_logs(self, job_id: str, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT logged_at, message
                FROM job_logs
                WHERE job_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (job_id, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]
