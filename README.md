# google-in-a-day

`google-in-a-day` is a localhost web crawler and live search engine built with Python standard-library components only. It crawls from an origin URL to depth `k`, keeps a thread-safe visited set so the same page is never fetched twice, applies back pressure with both a bounded queue and a shared rate limiter, and exposes live search results while indexing is still running.

## What it includes

- `/index` behavior via `POST /api/index` with `origin`, `max_depth`, `queue_capacity`, `rate_limit`, and `worker_count`
- live `/search` behavior via `GET /api/search?q=...`
- a simple localhost UI with three pages:
  - `/` crawler launcher
  - `/status` live system state
  - `/search` live search
- SQLite-backed persistence for pages, job metadata, discoveries, and logs
- automatic recovery of already indexed data after restart

## How relevance works

Search tokenizes the query and scores a page using:

- term frequency in the indexed page text
- a heavier bonus for title matches

Each result is returned with:

- `relevant_url`
- `origin_url`
- `depth`

`depth` is the discovered hop count for that page relative to the crawl origin.

## Run locally

```bash
python app.py
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

Optional flags:

```bash
python app.py --host 127.0.0.1 --port 8000 --db data/crawler.db
```

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Implementation notes

- Networking uses `urllib.request`
- HTML parsing uses `html.parser`
- Shared crawler state is protected with locks
- Each crawl job has its own bounded queue and worker threads
- Search snapshots the in-memory index under lock, so reads remain consistent while crawls are mutating state
- Pages are fetched globally at most once, but can still appear in search results for multiple crawl origins if they were discovered by multiple jobs

## Main files

- `app.py` entrypoint
- `google_in_a_day/engine.py` crawler, job lifecycle, search index
- `google_in_a_day/parsing.py` URL normalization and HTML parsing
- `google_in_a_day/storage.py` SQLite persistence
- `google_in_a_day/web.py` localhost UI and API
- `product_prd.md` build-oriented PRD
- `recommendation.md` short production next steps
