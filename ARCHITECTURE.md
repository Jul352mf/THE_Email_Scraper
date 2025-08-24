# THE Email Scraper Architecture (Async Streaming Edition)

## High-Level Pipeline

1. Input Loading (CSV/XLSX) -> company list
2. Streaming Google Search (async generator) -> (company, raw results)
3. Domain Scoring -> best candidate domain (sync, pure function)
4. Domain De-duplication & Scheduling (AsyncDomainTracker)
5. Domain Processing Task
   - Access pattern probe (HEAD requests)
   - Base page fetch
   - Priority / discovery pages (bounded per-domain concurrency)
   - Hybrid async fallback (JS render via Playwright)
6. Email Extraction & Filtering
7. Row Streaming Callback -> incremental CSV write
8. Final Excel export + metrics

## Key Async Components

| Component | Responsibility | Notes |
|-----------|---------------|-------|
| `AsyncGoogleSearchClient` | Parallel Google queries + streaming generator | Early pipeline fill |
| `AsyncOrchestrator` | Coordinates streaming + domain tasks | Exposes `process_companies_streaming` with callbacks |
| `AsyncHttpClient` | Rate limiting, circuit breaker, session pooling | Per-domain & global concurrency guards |
| `AsyncBrowserService` | Non-blocking JS rendering | Playwright based, only on fallback |
| `AsyncDomainTracker` | Dedup & memory bound domain set | Prevents redundant domain work |
| `AsyncTokenBucket` | Per-domain rate limiting | Token bucket, jitter safe |
| `AsyncCircuitBreaker` | Failure isolation | Domain half-open recovery |

## Concurrency Model

```text
Google streaming --> domain tasks queue --> per-domain semaphores --> HTTP
                           |                                ^
                           +--> row callback (flush) -------+
```

Controls:

- Company inflight limit (`max_concurrent_companies`)
- Per-domain page semaphore (default 3)
- Global HTTP semaphore (`AsyncHttpClient`)


## Email Discovery Strategy

1. Base page scrape
2. Discovery URLs (contact/about/team/... limited to 5)
3. Async hybrid fallback (render + extract) if still empty

Filtering removes generic / throwaway patterns and enforces domain relevance.

## Result Streaming

`cli.py` passes an `on_rows` callback to orchestrator. Each completed domain pushes rows directly to a temp CSV. Memory usage stays bounded regardless of total volume.

## Extensibility Points

- Add new discovery heuristics in `smart_discovery` without touching orchestrator
- Introduce adaptive stop (target emails per domain) inside `_process_domain_async`
- Extend metrics by exposing `get_performance_stats` (already implemented)


## Folder Structure (Post-Refactor Incremental Phase)

```text
scraper/
  async_components.py        # Shared async primitives
  async_http_client.py       # HTTP layer (will slim further)
  async_google_search.py     # Google search & streaming
  async_orchestrator.py      # Streaming coordination
  async_browser_service.py   # JS rendering
  email_extractor.py
  hybrid_email_extractor.py
  smart_discovery.py
  domain_scorer.py
  cli.py
  config.py
```

Future optional split:

- `core/` (config, models, tracking)
- `pipeline/` (orchestrator, streaming)
- `net/` (async_http_client, async_components)
- `extract/` (email & discovery)

## Deprecated Removed

Legacy synchronous / experimental modules under `archive/` removed to enforce the async path as the only maintained pipeline.

## Migration Notes

Imports expecting removed archive modules will now fail fast – update any external scripts to use async equivalents. A transitional re-export layer can be added if required.

## Next Improvements (Suggested)

- Adaptive early stop per domain
- Backpressure via bounded domain task queue
- Structured metrics endpoint / JSON export
- Retry budget & dynamic rate adjustment
- Pluggable search providers (Bing, DDG) behind a strategy


Document version: 2025-08-22
