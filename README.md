# google-in-a-day

`google-in-a-day` is a localhost web crawler and live search engine built with Python standard-library components only. It crawls from an origin URL to depth `k`, keeps a thread-safe visited set so the same page is never fetched twice, applies back pressure with both a bounded queue and a shared rate limiter, and exposes live search results while indexing is still running.

## What it includes

- `/index` behavior via `POST /index` and `POST /api/index` with `origin`, `max_depth`, `queue_capacity`, `rate_limit`, and `worker_count`
- live `/search` behavior via `GET /search?query=...&sortBy=relevance`
- a simple localhost UI with three pages:
  - `/` crawler launcher
  - `/status` live system state
  - `/search` live search
- SQLite-backed persistence for pages, job metadata, discoveries, and logs
- raw word storage files under `data/storage/<first-letter>.data`
- automatic recovery of already indexed data after restart
- a seeded sample raw storage file at `data/storage/p.data` so direct storage-file checks work immediately

## How relevance works

Search tokenizes the query and scores each matching `(url, origin_url, depth)` tuple using:

- `relevance_score = (frequency * 10) + 1000 - (depth * 5)`

The stored `frequency` comes from the indexed page text for the exact matched word. This is the same score used by the `/search?query=...&sortBy=relevance` endpoint.

Each result is returned with:

- `relevant_url`
- `origin_url`
- `depth`

`depth` is the discovered hop count for that page relative to the crawl origin.

## Run locally

```bash
python app.py
```

Then open [http://127.0.0.1:3600](http://127.0.0.1:3600).

Optional flags:

```bash
python app.py --host 127.0.0.1 --port 3600 --db data/crawler.db --storage-dir data/storage
```

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Raw Storage Compatibility

- The repository includes seeded raw crawl storage in `data/storage/p.data`.
- You can inspect that file directly and choose a repeated word such as `python`.
- The compatible search API is:

```bash
GET http://localhost:3600/search?query=python&sortBy=relevance
```

## Implementation notes

- Networking uses `urllib.request`
- HTML parsing uses `html.parser`
- Shared crawler state is protected with locks
- Each crawl job has its own bounded queue and worker threads
- Search snapshots the in-memory index under lock, so reads remain consistent while crawls are mutating state
- Pages are fetched globally at most once, but can still appear in search results for multiple crawl origins if they were discovered by multiple jobs
- Raw word storage files are updated as pages are indexed, so file-based checks and live search stay aligned

## Main files

- `app.py` entrypoint
- `google_in_a_day/engine.py` crawler, job lifecycle, search index
- `google_in_a_day/parsing.py` URL normalization and HTML parsing
- `google_in_a_day/storage.py` SQLite persistence
- `google_in_a_day/web.py` localhost UI and API
- `product_prd.md` build-oriented PRD
- `recommendation.md` short production next steps

## Author

Doruk Ilhan 820220323
GitHub: https://github.com/dorukilhann/google-in-a-day
