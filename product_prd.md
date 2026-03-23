# Product Requirement Document

## Project
Google in a Day

## Goal
Build a single-node localhost application that can crawl from a given origin URL, incrementally index discovered pages, and serve search results while crawling is still in progress.

## Primary User Flows

1. A user starts a crawl with an `origin` URL and max depth `k`.
2. The system fetches pages up to `k` hops away without fetching the same page twice.
3. The user can open search immediately and see matching results as pages are indexed.
4. The user can inspect live crawl state, including queue depth, active work, and whether back pressure is being applied.

## Functional Requirements

- Provide an indexing entry point that accepts `origin` and `k` (`max_depth` in the implementation).
- Crawl breadth-first to at most depth `k`.
- Maintain an explicit visited or claimed set so no page is crawled twice, even with concurrent workers.
- Apply back pressure through a bounded queue and/or a maximum fetch rate.
- Persist enough state locally to survive restarts without losing already indexed pages.
- Persist a raw word-storage view under `data/storage/<first-letter>.data` so indexed term data can be inspected directly.
- Provide a search entry point that accepts a free-text query and returns `(relevant_url, origin_url, depth)`.
- Allow search to run concurrently with active indexing and reflect newly indexed pages.
- Use a simple explainable ranking heuristic such as keyword frequency plus title boosting.
- Support an exact-word relevance sort using `relevance_score = (frequency * 10) + 1000 - (depth * 5)`.
- Provide a simple localhost UI or CLI for:
  - starting indexing
  - running search
  - viewing live system state

## Non-Functional Requirements

- Prefer language-native functionality over high-level crawling frameworks.
- Shared state must be thread-safe.
- Design for meaningful scale on one machine rather than for distributed execution.
- Keep the implementation intentionally simple and explainable within a 3-5 hour project scope.

## Suggested Architecture

- Python standard library only for the core workflow
- `urllib.request` for HTTP fetches
- `html.parser` for HTML text and link extraction
- `threading` and `queue.Queue` for concurrent crawl workers and bounded work queues
- in-memory inverted index for low-latency live search
- SQLite for local persistence of pages, jobs, discoveries, and logs
- flat files under `data/storage/` for direct inspection of indexed word entries
- a small `http.server` based UI/API served on localhost

## Acceptance Criteria

- Starting a crawl from the UI or API begins indexing immediately.
- A page is fetched at most once globally.
- Search can return relevant results before the crawl has completed.
- Raw storage files are present and searchable in a way that matches the exposed relevance API.
- Status output clearly shows whether indexing is active and whether back pressure is active.
- The repository contains runnable code, this PRD, a user-facing README, and a short production recommendation document.
