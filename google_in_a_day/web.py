from __future__ import annotations

import argparse
import html
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .engine import CrawlerEngine


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB = str(BASE_DIR / "data" / "crawler.db")


INDEX_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>google-in-a-day</title>
  <style>
    :root {{
      --ink: #102542;
      --ink-soft: #44566c;
      --bg: #f7f4ea;
      --panel: rgba(255,255,255,0.88);
      --line: rgba(16,37,66,0.12);
      --accent: #ff7a18;
      --success: #1f7a57;
      --danger: #992e2e;
      --shadow: 0 24px 60px rgba(16,37,66,0.14);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(255,122,24,0.22), transparent 35%),
        linear-gradient(135deg, #f7f4ea 0%, #f0efe7 45%, #e6eef7 100%);
      min-height: 100vh;
    }}
    a {{ color: var(--ink); }}
    .shell {{
      width: min(1120px, calc(100vw - 32px));
      margin: 32px auto 48px;
    }}
    .hero {{
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      background: linear-gradient(140deg, rgba(255,255,255,0.92), rgba(255,247,239,0.88));
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: clamp(2rem, 5vw, 3.8rem);
      letter-spacing: -0.04em;
    }}
    .lede {{
      margin: 0;
      color: var(--ink-soft);
      font-size: 1.05rem;
      max-width: 64ch;
    }}
    nav {{
      display: flex;
      gap: 16px;
      margin: 22px 0 0;
      flex-wrap: wrap;
    }}
    nav a {{
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.7);
    }}
    .grid {{
      margin-top: 24px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 20px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 22px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(8px);
    }}
    h2 {{
      margin-top: 0;
      font-size: 1.25rem;
    }}
    form {{
      display: grid;
      gap: 12px;
    }}
    label {{
      display: grid;
      gap: 6px;
      font-size: 0.95rem;
    }}
    input {{
      width: 100%;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.96);
      font: inherit;
      color: inherit;
    }}
    button {{
      border: none;
      border-radius: 999px;
      padding: 12px 16px;
      background: linear-gradient(135deg, var(--accent), #ff9f43);
      color: white;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .stat {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: rgba(255,255,255,0.72);
    }}
    .stat strong {{
      display: block;
      font-size: 1.4rem;
      margin-bottom: 4px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 0.82rem;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.8);
    }}
    .badge.live {{ color: var(--success); }}
    .muted {{ color: var(--ink-soft); }}
    .job {{
      padding: 14px 0;
      border-top: 1px solid var(--line);
    }}
    .job:first-child {{ border-top: none; padding-top: 0; }}
    .job-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }}
    .job-meta {{
      font-size: 0.92rem;
      color: var(--ink-soft);
    }}
    .results {{
      margin-top: 14px;
      display: grid;
      gap: 14px;
    }}
    .result {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      background: rgba(255,255,255,0.76);
    }}
    code {{
      background: rgba(16,37,66,0.08);
      padding: 2px 6px;
      border-radius: 6px;
    }}
    .flash {{
      margin-top: 12px;
      min-height: 24px;
      color: var(--ink-soft);
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>google-in-a-day</h1>
      <p class="lede">A single-node crawler and live search engine with bounded queues, rate-based back pressure, thread-safe shared state, raw word storage files, and search results that appear while indexing is still running.</p>
      <nav>
        <a href="/">Crawler</a>
        <a href="/status">Crawler Status</a>
        <a href="/search">Search</a>
      </nav>
    </section>
    {body}
  </main>
</body>
</html>
"""


def render_shell(body: str) -> bytes:
    return INDEX_PAGE.format(body=body).encode("utf-8")


def render_home() -> bytes:
    body = """
    <section class="grid">
      <article class="panel">
        <h2>Start Indexing</h2>
        <form id="crawl-form">
          <label>Origin URL
            <input type="url" name="origin" placeholder="https://example.com" required>
          </label>
          <label>Depth k
            <input type="number" name="max_depth" min="0" value="1" required>
          </label>
          <label>Pages per second
            <input type="number" name="rate_limit" min="0.1" step="0.1" value="4">
          </label>
          <label>Queue capacity
            <input type="number" name="queue_capacity" min="1" value="100">
          </label>
          <label>Workers
            <input type="number" name="worker_count" min="1" value="4">
          </label>
          <button type="submit">Start crawl</button>
        </form>
        <div class="flash" id="crawl-flash"></div>
      </article>
      <article class="panel">
        <h2>System Snapshot</h2>
        <div class="stats" id="global-stats"></div>
        <p class="muted">The status page auto-refreshes and shows queue depth, active workers, and whether back pressure is currently being applied.</p>
      </article>
    </section>
    <section class="panel" style="margin-top:20px;">
      <h2>Recent Crawl Jobs</h2>
      <div id="jobs"></div>
    </section>
    <script>
      const crawlForm = document.getElementById('crawl-form');
      const crawlFlash = document.getElementById('crawl-flash');
      async function refreshStatus() {
        const response = await fetch('/api/status');
        const data = await response.json();
        const stats = [
          ['Indexing active', data.indexing_active ? 'yes' : 'no'],
          ['Back pressure', data.backpressure_active ? 'active' : 'idle'],
          ['Visited URLs', data.visited_urls],
          ['Indexed pages', data.pages_indexed],
          ['Failed pages', data.pages_failed],
          ['Global queue', data.queue_depth]
        ];
        document.getElementById('global-stats').innerHTML = stats.map(([label, value]) =>
          `<div class="stat"><strong>${value}</strong><span>${label}</span></div>`
        ).join('');

        document.getElementById('jobs').innerHTML = data.jobs.length
          ? data.jobs.map(job => `
              <div class="job">
                <div><a href="/jobs/${encodeURIComponent(job.job_id)}"><strong>${job.job_id}</strong></a></div>
                <div class="job-meta">${job.origin_url} | status: ${job.status}</div>
                <div class="job-grid">
                  <div><span class="badge ${job.status === 'running' ? 'live' : ''}">${job.status}</span></div>
                  <div>depth: <code>${job.max_depth}</code></div>
                  <div>discovered: <code>${job.pages_discovered}</code></div>
                  <div>indexed: <code>${job.pages_indexed}</code></div>
                  <div>queue: <code>${job.queue_depth}</code></div>
                  <div>workers: <code>${job.active_workers || 0}</code></div>
                </div>
              </div>
            `).join('')
          : '<p class="muted">No crawl jobs yet.</p>';
      }

      crawlForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        crawlFlash.textContent = 'Starting crawl...';
        const formData = new FormData(crawlForm);
        const payload = Object.fromEntries(formData.entries());
        payload.max_depth = Number(payload.max_depth);
        payload.rate_limit = Number(payload.rate_limit);
        payload.queue_capacity = Number(payload.queue_capacity);
        payload.worker_count = Number(payload.worker_count);
        const response = await fetch('/index', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await response.json();
        if (!response.ok) {
          crawlFlash.textContent = data.error || 'Could not start crawl.';
          return;
        }
        crawlFlash.innerHTML = `Started <a href="/jobs/${encodeURIComponent(data.job_id)}">${data.job_id}</a>`;
        crawlForm.reset();
        refreshStatus();
      });

      refreshStatus();
      setInterval(refreshStatus, 2000);
    </script>
    """
    return render_shell(body)


def render_status_page() -> bytes:
    body = """
    <section class="panel">
      <h2>Live Status</h2>
      <p class="muted">This view makes concurrent indexing visible while it is happening.</p>
      <div id="status-root"></div>
    </section>
    <script>
      async function loadStatus() {
        const response = await fetch('/api/status');
        const data = await response.json();
        document.getElementById('status-root').innerHTML = `
          <div class="stats">
            <div class="stat"><strong>${data.indexing_active ? 'yes' : 'no'}</strong><span>Indexing active</span></div>
            <div class="stat"><strong>${data.backpressure_active ? 'yes' : 'no'}</strong><span>Back pressure active</span></div>
            <div class="stat"><strong>${data.queue_depth}</strong><span>Global queue depth</span></div>
            <div class="stat"><strong>${data.pages_indexed}</strong><span>Indexed pages</span></div>
          </div>
          <div class="results">
            ${
              data.jobs.map(job => `
                <div class="result">
                  <div><a href="/jobs/${encodeURIComponent(job.job_id)}"><strong>${job.job_id}</strong></a></div>
                  <div class="job-meta">${job.origin_url}</div>
                  <div class="job-grid">
                    <div>status: <code>${job.status}</code></div>
                    <div>depth: <code>${job.max_depth}</code></div>
                    <div>queue depth: <code>${job.queue_depth}</code></div>
                    <div>active workers: <code>${job.active_workers || 0}</code></div>
                    <div>queue back pressure: <code>${job.queue_backpressure_active ? 'yes' : 'no'}</code></div>
                    <div>rate back pressure: <code>${job.rate_backpressure_active ? 'yes' : 'no'}</code></div>
                  </div>
                </div>
              `).join('')
            }
          </div>
        `;
      }
      loadStatus();
      setInterval(loadStatus, 2000);
    </script>
    """
    return render_shell(body)


def render_search_page() -> bytes:
    body = """
    <section class="grid">
      <article class="panel">
        <h2>Live Search</h2>
        <form id="search-form">
          <label>Query
            <input type="search" name="q" placeholder="brightwave crawler" required>
          </label>
          <button type="submit">Search</button>
        </form>
        <div class="flash" id="search-meta"></div>
      </article>
      <article class="panel">
        <h2>Ranking</h2>
        <p class="muted">Results are scored with a simple heuristic: query term frequency in the page text plus a heavier bonus for title matches.</p>
      </article>
    </section>
    <section class="panel" style="margin-top:20px;">
      <h2>Results</h2>
      <div id="search-results" class="results"></div>
    </section>
    <script>
      const searchForm = document.getElementById('search-form');
      const searchMeta = document.getElementById('search-meta');
      const searchResults = document.getElementById('search-results');
      let currentQuery = '';

      async function runSearch(query) {
        const response = await fetch(`/search?query=${encodeURIComponent(query)}&sortBy=relevance`);
        const data = await response.json();
        searchMeta.textContent = `${data.results.length} result(s) for "${data.query}"`;
        searchResults.innerHTML = data.results.length
          ? data.results.map(result => `
              <div class="result">
                <div><strong>${result.title || result.url}</strong></div>
                <div><a href="${result.url}" target="_blank" rel="noreferrer">${result.url}</a></div>
                <div class="job-grid">
                  <div>origin: <code>${result.origin_url}</code></div>
                  <div>depth: <code>${result.depth}</code></div>
                  <div>relevance: <code>${result.relevance_score}</code></div>
                  <div>frequency: <code>${result.frequency}</code></div>
                </div>
              </div>
            `).join('')
          : '<p class="muted">No indexed pages match yet. Searches update as crawling continues.</p>';
      }

      searchForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const query = new FormData(searchForm).get('q');
        currentQuery = query;
        runSearch(query);
      });

      setInterval(() => {
        if (currentQuery) {
          runSearch(currentQuery);
        }
      }, 2000);
    </script>
    """
    return render_shell(body)


def render_job_page(job: dict[str, Any], logs: list[dict[str, str]]) -> bytes:
    job_json = html.escape(json.dumps(job, indent=2))
    log_html = "".join(
        f"<div class='job'><div><strong>{html.escape(entry['logged_at'])}</strong></div><div>{html.escape(entry['message'])}</div></div>"
        for entry in logs
    )
    body = f"""
    <section class="panel">
      <h2>Crawler Status</h2>
      <div class="job-meta">{html.escape(job['origin_url'])}</div>
      <div class="job-grid" style="margin-top:14px;">
        <div>status: <code>{html.escape(str(job['status']))}</code></div>
        <div>depth: <code>{html.escape(str(job['max_depth']))}</code></div>
        <div>queue depth: <code>{html.escape(str(job.get('queue_depth', 0)))}</code></div>
        <div>active workers: <code>{html.escape(str(job.get('active_workers', 0)))}</code></div>
        <div>queue back pressure: <code>{'yes' if job.get('queue_backpressure_active') else 'no'}</code></div>
        <div>rate back pressure: <code>{'yes' if job.get('rate_backpressure_active') else 'no'}</code></div>
      </div>
      <pre style="margin-top:18px;overflow:auto;border:1px solid rgba(16,37,66,0.12);padding:14px;border-radius:16px;background:rgba(255,255,255,0.74);">{job_json}</pre>
    </section>
    <section class="panel" style="margin-top:20px;">
      <h2>Recent Logs</h2>
      {log_html or "<p class='muted'>No logs yet.</p>"}
    </section>
    """
    return render_shell(body)


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length else b"{}"
    return json.loads(raw.decode("utf-8") or "{}")


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def html_response(handler: BaseHTTPRequestHandler, body: bytes, status: int = 200) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def make_handler(engine: CrawlerEngine):
    class AppHandler(BaseHTTPRequestHandler):
        server_version = "google-in-a-day/1.0"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                html_response(self, render_home())
                return
            if parsed.path == "/status":
                html_response(self, render_status_page())
                return
            if parsed.path == "/search":
                params = parse_qs(parsed.query)
                if "query" in params:
                    query = params.get("query", [""])[0]
                    sort_by = params.get("sortBy", ["relevance"])[0]
                    results = engine.search(query)
                    if sort_by == "relevance":
                        results = sorted(results, key=lambda item: (-item.score, item.depth, item.relevant_url, item.origin_url))
                    json_response(
                        self,
                        200,
                        {
                            "query": query,
                            "sortBy": sort_by,
                            "results": [
                                {
                                    "url": result.relevant_url,
                                    "relevant_url": result.relevant_url,
                                    "origin_url": result.origin_url,
                                    "origin": result.origin_url,
                                    "depth": result.depth,
                                    "frequency": result.frequency,
                                    "relevance_score": result.score,
                                    "score": result.score,
                                    "title": result.title,
                                }
                                for result in results
                            ],
                        },
                    )
                    return
                html_response(self, render_search_page())
                return
            if parsed.path.startswith("/jobs/"):
                job_id = parsed.path.split("/jobs/", 1)[1]
                job = engine.get_job_snapshot(job_id)
                if not job:
                    self.send_error(HTTPStatus.NOT_FOUND, "Job not found")
                    return
                html_response(self, render_job_page(job, engine.get_job_logs(job_id)))
                return
            if parsed.path == "/api/status":
                json_response(self, 200, engine.global_status())
                return
            if parsed.path == "/api/search":
                params = parse_qs(parsed.query)
                query = params.get("q", params.get("query", [""]))[0]
                results = engine.search(query)
                json_response(
                    self,
                    200,
                    {
                        "query": query,
                        "results": [
                            {
                                "url": result.relevant_url,
                                "relevant_url": result.relevant_url,
                                "origin_url": result.origin_url,
                                "depth": result.depth,
                                "frequency": result.frequency,
                                "relevance_score": result.score,
                                "score": result.score,
                                "title": result.title,
                            }
                            for result in results
                        ],
                    },
                )
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path not in {"/api/index", "/index"}:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                return

            try:
                payload = parse_json_body(self)
                job = engine.start_job(
                    origin=str(payload.get("origin", "")),
                    max_depth=int(payload.get("max_depth", 0)),
                    queue_capacity=int(payload.get("queue_capacity", 100)),
                    rate_limit=float(payload.get("rate_limit", 4)),
                    worker_count=int(payload.get("worker_count", 4)),
                )
                json_response(self, 200, job)
            except ValueError as error:
                json_response(self, 400, {"error": str(error)})
            except json.JSONDecodeError:
                json_response(self, 400, {"error": "invalid JSON payload"})

        def log_message(self, format: str, *args: Any) -> None:
            return

    return AppHandler


def run_server(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the google-in-a-day crawler UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=3600, type=int)
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--storage-dir", default=str(BASE_DIR / "data" / "storage"))
    args = parser.parse_args(argv)

    engine = CrawlerEngine(db_path=args.db, storage_dir=args.storage_dir)
    httpd = ThreadingHTTPServer((args.host, args.port), make_handler(engine))
    print(f"Serving google-in-a-day on http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        engine.store.close()
