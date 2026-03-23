from __future__ import annotations

import gzip
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .parsing import PageParser, normalize_url, term_frequencies, tokenize
from .storage import FlatFileWordStore, SQLiteStore, WordStorageEntry


MAX_DOWNLOAD_BYTES = 1_000_000
FETCH_TIMEOUT_SECONDS = 10
USER_AGENT = "google-in-a-day/1.0"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class SearchResult:
    relevant_url: str
    origin_url: str
    depth: int
    score: int
    title: str
    frequency: int


@dataclass(frozen=True)
class DiscoveryRecord:
    job_id: str
    origin_url: str
    url: str
    depth: int


@dataclass
class PageRecord:
    url: str
    title: str
    text: str
    links: list[str]
    fetched_at: str
    status_code: int
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None and 200 <= self.status_code < 400

    def to_storage_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "text": self.text,
            "links": self.links,
            "fetched_at": self.fetched_at,
            "status_code": self.status_code,
            "error": self.error,
        }


@dataclass(frozen=True)
class Posting:
    frequency: int
    title_hits: int


@dataclass(frozen=True)
class CrawlTask:
    url: str
    depth: int


class RateLimiter:
    def __init__(self, rate_per_second: float) -> None:
        self.rate_per_second = rate_per_second
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait_for_turn(self) -> float:
        interval = 1.0 / self.rate_per_second
        with self._lock:
            now = time.monotonic()
            wait_time = max(0.0, self._next_allowed - now)
            self._next_allowed = max(self._next_allowed, now) + interval
        if wait_time > 0:
            time.sleep(wait_time)
        return wait_time


class CrawlJob:
    def __init__(
        self,
        engine: "CrawlerEngine",
        job_id: str,
        origin_url: str,
        max_depth: int,
        queue_capacity: int,
        rate_limit: float,
        worker_count: int,
    ) -> None:
        self.engine = engine
        self.job_id = job_id
        self.origin_url = origin_url
        self.max_depth = max_depth
        self.queue_capacity = queue_capacity
        self.rate_limit = rate_limit
        self.worker_count = worker_count
        self.created_at = utc_now()
        self.updated_at = self.created_at
        self.status = "created"

        self.queue: queue.Queue[CrawlTask] = queue.Queue(maxsize=queue_capacity)
        self.rate_limiter = RateLimiter(rate_limit)
        self.stop_event = threading.Event()
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._monitor_thread: threading.Thread | None = None
        self._recent_logs: deque[dict[str, str]] = deque(maxlen=100)
        self._seen_urls: set[str] = set()
        self._ready_urls: set[str] = set()
        self._active_workers = 0
        self._pending_shared_pages = 0
        self._pages_discovered = 0
        self._pages_indexed = 0
        self._queue_backpressure_count = 0
        self._rate_backpressure_count = 0
        self._last_queue_backpressure_at = 0.0
        self._last_rate_backpressure_at = 0.0

    def start(self) -> None:
        self.status = "running"
        self.updated_at = utc_now()
        self.engine.persist_job_summary(self.snapshot())
        self.log(f"Started crawl from {self.origin_url} up to depth {self.max_depth}.")
        self.discover_url(self.origin_url, 0)

        for index in range(self.worker_count):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"crawl-worker-{self.job_id}-{index}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"crawl-monitor-{self.job_id}",
            daemon=True,
        )
        self._monitor_thread.start()

    def log(self, message: str) -> None:
        entry = {"logged_at": utc_now(), "message": message}
        with self._lock:
            self._recent_logs.append(entry)
            self.updated_at = entry["logged_at"]
        self.engine.store.append_job_log(self.job_id, entry["logged_at"], message)
        self.engine.persist_job_summary(self.snapshot())

    def discover_url(self, url: str, depth: int) -> bool:
        if depth > self.max_depth:
            return False

        with self._lock:
            if url in self._seen_urls:
                return False
            self._seen_urls.add(url)
            self._pages_discovered += 1
            self.updated_at = utc_now()

        self.engine.record_discovery(self, url, depth)
        task = CrawlTask(url=url, depth=depth)
        while not self.stop_event.is_set():
            try:
                self.queue.put(task, timeout=0.2)
                self.engine.persist_job_summary(self.snapshot())
                return True
            except queue.Full:
                with self._lock:
                    self._queue_backpressure_count += 1
                    self._last_queue_backpressure_at = time.monotonic()
                self.log(f"Queue back pressure active while scheduling {url}.")
        return False

    def register_shared_wait(self, url: str) -> None:
        with self._lock:
            self._pending_shared_pages += 1
            self.updated_at = utc_now()
        self.log(f"Waiting for shared fetch of {url}.")

    def on_page_ready(self, url: str, depth: int, page: PageRecord, fetched_by_this_job: bool) -> None:
        with self._lock:
            if url in self._ready_urls:
                if not fetched_by_this_job and self._pending_shared_pages > 0:
                    self._pending_shared_pages -= 1
                self.updated_at = utc_now()
                return
            self._ready_urls.add(url)
            self._pages_indexed += 1
            if not fetched_by_this_job and self._pending_shared_pages > 0:
                self._pending_shared_pages -= 1
            self.updated_at = utc_now()

        if page.success:
            self.engine.record_word_entries(page, self.origin_url, depth)
            self.log(f"Indexed {url} at depth {depth}.")
            if depth < self.max_depth:
                for link in page.links:
                    self.discover_url(link, depth + 1)
        else:
            self.log(f"Fetch failed for {url}: {page.error or 'unknown error'}.")

        self.engine.persist_job_summary(self.snapshot())

    def on_page_failed(self, url: str, depth: int, error: str, fetched_by_this_job: bool) -> None:
        self.on_page_ready(
            url,
            depth,
            PageRecord(
                url=url,
                title="",
                text="",
                links=[],
                fetched_at=utc_now(),
                status_code=0,
                error=error,
            ),
            fetched_by_this_job=fetched_by_this_job,
        )

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                task = self.queue.get(timeout=0.3)
            except queue.Empty:
                continue

            with self._lock:
                self._active_workers += 1
                self.updated_at = utc_now()

            try:
                action, page = self.engine.prepare_fetch(task.url, self, task.depth)
                if action == "ready":
                    self.on_page_ready(task.url, task.depth, page, fetched_by_this_job=False)
                elif action == "failed":
                    self.on_page_failed(task.url, task.depth, page.error or "previous fetch failed", fetched_by_this_job=False)
                elif action == "wait":
                    self.register_shared_wait(task.url)
                elif action == "fetch":
                    waited = self.rate_limiter.wait_for_turn()
                    if waited > 0:
                        with self._lock:
                            self._rate_backpressure_count += 1
                            self._last_rate_backpressure_at = time.monotonic()
                        self.log(f"Rate limiter paused fetch of {task.url} for {waited:.2f}s.")

                    page = self.engine.fetch_page(task.url)
                    waiters = self.engine.complete_fetch(task.url, page)
                    self.on_page_ready(task.url, task.depth, page, fetched_by_this_job=True)
                    for waiter_job_id, waiter_depth in waiters:
                        waiting_job = self.engine.get_live_job(waiter_job_id)
                        if waiting_job is not None:
                            waiting_job.on_page_ready(task.url, waiter_depth, page, fetched_by_this_job=False)
                else:
                    self.log(f"Unknown engine action {action} for {task.url}.")
            finally:
                with self._lock:
                    self._active_workers = max(0, self._active_workers - 1)
                    self.updated_at = utc_now()
                self.queue.task_done()

    def _monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(0.3)
            with self._lock:
                should_finish = (
                    self.status == "running"
                    and self.queue.empty()
                    and self._active_workers == 0
                    and self._pending_shared_pages == 0
                )
            if should_finish:
                self.complete("completed")
                return

    def complete(self, status: str) -> None:
        with self._lock:
            if self.status in {"completed", "failed", "interrupted"}:
                return
            self.status = status
            self.updated_at = utc_now()
            self.stop_event.set()
        self.engine.persist_job_summary(self.snapshot())
        self.log(f"Job {status}.")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "job_id": self.job_id,
                "origin_url": self.origin_url,
                "max_depth": self.max_depth,
                "queue_capacity": self.queue_capacity,
                "rate_limit": self.rate_limit,
                "worker_count": self.worker_count,
                "status": self.status,
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "pages_discovered": self._pages_discovered,
                "pages_indexed": self._pages_indexed,
                "queue_depth": self.queue.qsize(),
                "active_workers": self._active_workers,
                "pending_shared_pages": self._pending_shared_pages,
                "queue_backpressure_count": self._queue_backpressure_count,
                "rate_backpressure_count": self._rate_backpressure_count,
                "queue_backpressure_active": (time.monotonic() - self._last_queue_backpressure_at) < 2.0,
                "rate_backpressure_active": (time.monotonic() - self._last_rate_backpressure_at) < 2.0,
            }

    def recent_logs(self) -> list[dict[str, str]]:
        with self._lock:
            if self._recent_logs:
                return list(self._recent_logs)
        return self.engine.store.load_job_logs(self.job_id)


class CrawlerEngine:
    def __init__(self, db_path: str = "data/crawler.db", storage_dir: str | None = None) -> None:
        db_path_obj = Path(db_path)
        self.store = SQLiteStore(db_path)
        self.word_store = FlatFileWordStore(storage_dir or str(db_path_obj.parent / "storage"))
        self.store.mark_incomplete_jobs_interrupted()
        self._lock = threading.RLock()
        self._pages: dict[str, PageRecord] = {}
        self._fetch_state: dict[str, str] = {}
        self._postings: dict[str, dict[str, Posting]] = defaultdict(dict)
        self._word_entries: dict[str, dict[tuple[str, str, int], WordStorageEntry]] = defaultdict(dict)
        self._discoveries_by_url: dict[str, list[DiscoveryRecord]] = defaultdict(list)
        self._jobs_live: dict[str, CrawlJob] = {}
        self._job_history: dict[str, dict[str, Any]] = {}
        self._waiters: dict[str, list[tuple[str, int]]] = defaultdict(list)
        self._load_from_storage()

    def _load_from_storage(self) -> None:
        loaded_word_entries = self.word_store.load_entries()
        with self._lock:
            for entry in loaded_word_entries:
                self._word_entries[entry.word][(entry.url, entry.origin_url, entry.depth)] = entry

        for row in self.store.load_pages():
            page = PageRecord(
                url=row["url"],
                title=row["title"],
                text=row["text"],
                links=row["links"],
                fetched_at=row["fetched_at"],
                status_code=row["status_code"],
                error=row["error"],
            )
            with self._lock:
                self._pages[page.url] = page
                self._fetch_state[page.url] = "failed" if page.error else "fetched"
                if page.success:
                    self._index_page_locked(page)

        summaries = self.store.load_job_summaries()
        with self._lock:
            for summary in summaries:
                self._job_history[summary["job_id"]] = {
                    **summary,
                    "queue_depth": 0,
                    "active_workers": 0,
                    "pending_shared_pages": 0,
                    "queue_backpressure_active": False,
                    "rate_backpressure_active": False,
                }

        for discovery in self.store.load_discoveries():
            with self._lock:
                self._discoveries_by_url[discovery["url"]].append(
                    DiscoveryRecord(
                        job_id=discovery["job_id"],
                        origin_url=discovery["origin_url"],
                        url=discovery["url"],
                        depth=discovery["depth"],
                    )
                )

        if not loaded_word_entries and self._pages and self._discoveries_by_url:
            self._rebuild_word_storage_from_sqlite()

    def _index_page_locked(self, page: PageRecord) -> None:
        text_counts = term_frequencies(page.text)
        title_counts = term_frequencies(page.title)
        for term in set(text_counts) | set(title_counts):
            self._postings[term][page.url] = Posting(
                frequency=text_counts.get(term, 0),
                title_hits=title_counts.get(term, 0),
            )

    def _rebuild_word_storage_from_sqlite(self) -> None:
        rebuilt_entries: list[WordStorageEntry] = []
        with self._lock:
            pages = dict(self._pages)
            discoveries = {url: list(records) for url, records in self._discoveries_by_url.items()}

        for url, page in pages.items():
            if not page.success:
                continue
            frequencies = term_frequencies(page.text)
            if not frequencies:
                continue
            for record in discoveries.get(url, []):
                for word, frequency in frequencies.items():
                    rebuilt_entries.append(
                        WordStorageEntry(
                            word=word,
                            url=url,
                            origin_url=record.origin_url,
                            depth=record.depth,
                            frequency=frequency,
                        )
                    )

        with self._lock:
            self._word_entries = defaultdict(dict)
            for entry in rebuilt_entries:
                self._word_entries[entry.word][(entry.url, entry.origin_url, entry.depth)] = entry

        self.word_store.rewrite_all(rebuilt_entries)

    def record_word_entries(self, page: PageRecord, origin_url: str, depth: int) -> None:
        if not page.success:
            return

        frequencies = term_frequencies(page.text)
        if not frequencies:
            return

        new_entries: list[WordStorageEntry] = []
        with self._lock:
            for word, frequency in frequencies.items():
                entry = WordStorageEntry(
                    word=word,
                    url=page.url,
                    origin_url=origin_url,
                    depth=depth,
                    frequency=frequency,
                )
                key = (entry.url, entry.origin_url, entry.depth)
                existing = self._word_entries[word].get(key)
                if existing == entry:
                    continue
                self._word_entries[word][key] = entry
                new_entries.append(entry)

        self.word_store.upsert_entries(new_entries)

    def persist_job_summary(self, summary: dict[str, Any]) -> None:
        self.store.save_job_summary(summary)
        with self._lock:
            self._job_history[summary["job_id"]] = dict(summary)

    def start_job(
        self,
        origin: str,
        max_depth: int,
        queue_capacity: int = 100,
        rate_limit: float = 4.0,
        worker_count: int = 4,
    ) -> dict[str, Any]:
        normalized = normalize_url(origin)
        if not normalized:
            raise ValueError("origin must be a valid http or https URL")
        if max_depth < 0:
            raise ValueError("max_depth must be >= 0")
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be >= 1")
        if rate_limit <= 0:
            raise ValueError("rate_limit must be > 0")
        if worker_count < 1:
            raise ValueError("worker_count must be >= 1")

        job_id = f"{int(time.time() * 1000)}_{threading.get_ident()}"
        job = CrawlJob(
            engine=self,
            job_id=job_id,
            origin_url=normalized,
            max_depth=max_depth,
            queue_capacity=queue_capacity,
            rate_limit=rate_limit,
            worker_count=worker_count,
        )
        with self._lock:
            self._jobs_live[job_id] = job
        job.start()
        return job.snapshot()

    def record_discovery(self, job: CrawlJob, url: str, depth: int) -> None:
        record = DiscoveryRecord(job_id=job.job_id, origin_url=job.origin_url, url=url, depth=depth)
        with self._lock:
            self._discoveries_by_url[url].append(record)
        self.store.save_job_discovery(job.job_id, url, depth)
        self.persist_job_summary(job.snapshot())

    def prepare_fetch(self, url: str, job: CrawlJob, depth: int) -> tuple[str, PageRecord | None]:
        with self._lock:
            fetch_state = self._fetch_state.get(url)
            if fetch_state == "fetched":
                return "ready", self._pages[url]
            if fetch_state == "failed":
                return "failed", self._pages[url]
            if fetch_state == "in_progress":
                self._waiters[url].append((job.job_id, depth))
                return "wait", None

            self._fetch_state[url] = "in_progress"
            return "fetch", None

    def complete_fetch(self, url: str, page: PageRecord) -> list[tuple[str, int]]:
        with self._lock:
            self._pages[url] = page
            self._fetch_state[url] = "failed" if page.error else "fetched"
            if page.success:
                self._index_page_locked(page)
            waiters = list(self._waiters.pop(url, []))
        self.store.save_page(page.to_storage_dict())
        return waiters

    def fetch_page(self, url: str) -> PageRecord:
        request = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
                status_code = getattr(response, "status", 200)
                headers = response.headers
                body = response.read(MAX_DOWNLOAD_BYTES + 1)
                truncated = len(body) > MAX_DOWNLOAD_BYTES
                if truncated:
                    body = body[:MAX_DOWNLOAD_BYTES]
                content_encoding = (headers.get("Content-Encoding") or "").lower()
                if "gzip" in content_encoding:
                    try:
                        body = gzip.decompress(body)
                    except OSError:
                        pass
                charset = self._charset_from_headers(headers)
                text_body = body.decode(charset, errors="replace")
                content_type = headers.get_content_type().lower() if isinstance(headers, Message) else ""

                if "html" in content_type:
                    parser = PageParser(url)
                    parser.feed(text_body)
                    page = PageRecord(
                        url=url,
                        title=parser.title,
                        text=parser.text,
                        links=list(dict.fromkeys(parser.links)),
                        fetched_at=utc_now(),
                        status_code=status_code,
                    )
                else:
                    page = PageRecord(
                        url=url,
                        title=urlsplit(url).path or url,
                        text=text_body,
                        links=[],
                        fetched_at=utc_now(),
                        status_code=status_code,
                    )

                if truncated:
                    page.text = f"{page.text}\n[truncated after {MAX_DOWNLOAD_BYTES} bytes]"
                return page
        except HTTPError as error:
            return PageRecord(
                url=url,
                title="",
                text="",
                links=[],
                fetched_at=utc_now(),
                status_code=error.code,
                error=f"HTTP {error.code}",
            )
        except URLError as error:
            reason = getattr(error, "reason", error)
            return PageRecord(
                url=url,
                title="",
                text="",
                links=[],
                fetched_at=utc_now(),
                status_code=0,
                error=f"URL error: {reason}",
            )
        except Exception as error:
            return PageRecord(
                url=url,
                title="",
                text="",
                links=[],
                fetched_at=utc_now(),
                status_code=0,
                error=f"unexpected error: {error}",
            )

    def _charset_from_headers(self, headers: Message | Any) -> str:
        if isinstance(headers, Message):
            return headers.get_content_charset() or "utf-8"
        return "utf-8"

    def search(self, query: str, sort_by: str | None = None) -> list[SearchResult]:
        if sort_by == "relevance":
            return self._search_by_exact_relevance(query)
        return self._search_by_default_rank(query)

    def _search_by_default_rank(self, query: str) -> list[SearchResult]:
        terms = tokenize(query)
        if not terms:
            return []

        with self._lock:
            term_snapshots = {term: dict(self._postings.get(term, {})) for term in set(terms)}
            pages = dict(self._pages)
            discoveries = {url: list(records) for url, records in self._discoveries_by_url.items()}

        scores: dict[str, int] = defaultdict(int)
        frequencies: dict[str, int] = defaultdict(int)
        for term in terms:
            for url, posting in term_snapshots.get(term, {}).items():
                scores[url] += posting.frequency + (posting.title_hits * 5)
                frequencies[url] += posting.frequency

        results: list[SearchResult] = []
        for url, score in scores.items():
            page = pages.get(url)
            if not page or not page.success:
                continue
            for record in discoveries.get(url, []):
                results.append(
                    SearchResult(
                        relevant_url=url,
                        origin_url=record.origin_url,
                        depth=record.depth,
                        score=score,
                        title=page.title or url,
                        frequency=frequencies[url],
                    )
                )
        
        results.sort(key=lambda item: (-item.score, item.depth, item.relevant_url, item.origin_url))
        return results

    def _search_by_exact_relevance(self, query: str) -> list[SearchResult]:
        terms = tokenize(query)
        if not terms:
            return []

        with self._lock:
            entry_snapshots = {term: list(self._word_entries.get(term, {}).values()) for term in set(terms)}
            pages = dict(self._pages)

        aggregated: dict[tuple[str, str, int], dict[str, Any]] = {}
        for term in terms:
            for entry in entry_snapshots.get(term, []):
                key = (entry.url, entry.origin_url, entry.depth)
                bucket = aggregated.setdefault(
                    key,
                    {
                        "url": entry.url,
                        "origin_url": entry.origin_url,
                        "depth": entry.depth,
                        "matched_terms": {},
                    },
                )
                bucket["matched_terms"][term] = entry.frequency

        results: list[SearchResult] = []
        for bucket in aggregated.values():
            url = bucket["url"]
            depth = bucket["depth"]
            term_frequencies_map: dict[str, int] = bucket["matched_terms"]
            total_frequency = sum(term_frequencies_map.values())
            score = sum((frequency * 10) + 1000 for frequency in term_frequencies_map.values()) - (depth * 5)
            page = pages.get(url)
            results.append(
                SearchResult(
                    relevant_url=url,
                    origin_url=bucket["origin_url"],
                    depth=depth,
                    score=score,
                    title=(page.title if page and page.title else url),
                    frequency=total_frequency,
                )
            )

        results.sort(key=lambda item: (-item.score, item.depth, item.relevant_url, item.origin_url))
        return results

    def global_status(self) -> dict[str, Any]:
        with self._lock:
            live_jobs = [job.snapshot() for job in self._jobs_live.values()]
            live_ids = {job["job_id"] for job in live_jobs}
            history = [dict(summary) for job_id, summary in self._job_history.items() if job_id not in live_ids]
            all_jobs = sorted(live_jobs + history, key=lambda item: item["created_at"], reverse=True)
            active_jobs = [job for job in live_jobs if job["status"] == "running"]

            return {
                "indexing_active": any(job["status"] == "running" for job in live_jobs),
                "backpressure_active": any(
                    job["queue_backpressure_active"] or job["rate_backpressure_active"] for job in active_jobs
                ),
                "pages_indexed": sum(1 for page in self._pages.values() if page.success),
                "pages_failed": sum(1 for page in self._pages.values() if not page.success),
                "visited_urls": len(self._fetch_state),
                "queue_depth": sum(job["queue_depth"] for job in active_jobs),
                "jobs": all_jobs,
            }

    def get_job_snapshot(self, job_id: str) -> dict[str, Any] | None:
        job = self.get_live_job(job_id)
        if job is not None:
            return job.snapshot()
        with self._lock:
            summary = self._job_history.get(job_id)
            return dict(summary) if summary else None

    def get_job_logs(self, job_id: str) -> list[dict[str, str]]:
        job = self.get_live_job(job_id)
        if job is not None:
            return job.recent_logs()
        return self.store.load_job_logs(job_id)

    def get_live_job(self, job_id: str) -> CrawlJob | None:
        with self._lock:
            return self._jobs_live.get(job_id)
