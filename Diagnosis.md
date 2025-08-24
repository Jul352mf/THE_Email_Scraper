Diagnosis (why it’s slow now):

Google stage is effectively serial: log shows “Processing batch of 1 …” repeatedly. Your dedup/batching layer is collapsing all queries or a throttle forces batch_size=1.
Current sync orchestrator still runs (log: scraper.orchestrator) – you’re not using the new fully‑parallel async path.
Domain email phase still waits for Google result per company instead of flooding pipeline with all discovered domains.
Hybrid fallback still adds latency before deciding to move on (JS/render only after empty static pass, per domain).
No pipelined backpressure metrics—bottleneck (Google) went unnoticed.
Immediate tactical fixes (fast patches before redesign):

Force async path: ensure CLI --async switches to AsyncOrchestrator by default (or make async default).
Google client:
Remove/raise artificial batch size; run N (e.g. 25) queries concurrently respecting QPS via token bucket.
Stop over‑deduplicating: dedupe exact company strings only, not whole standardized query template if that collapses variety.
Preconstruct all queries then single asyncio.gather.
Parallel domain scoring: run scoring immediately as each Google result arrives (stream style) instead of after full batch.
Fire domain processing tasks as soon as a (domain, score) passes threshold; don’t wait for rest of company’s pipeline.
Increase HTTP concurrency (global) while adding per-domain semaphore (e.g. max 4 concurrent requests per domain).
Early stop crawl: once you have X (configurable, maybe 2) valid unique company‑domain emails, cancel outstanding low‑priority tasks for that domain.
Stream results: append rows to CSV incrementally; periodically flush to reduce RAM and allow long runs.
Metrics: add gauges for google_queue, domains_in_progress, pages_inflight, emails_found_per_sec; log every 5s to spot new bottlenecks.
Strategic redesign (clean async pipeline): Pipeline stages (all async, streaming): [Stage 0] Ingest companies → queue_companies [Stage 1] Google Query Fan‑out (concurrent gather) → emits (company, candidate_domains) [Stage 2] Domain Scoring (lightweight sync CPU, run in event loop) → (company, accepted_domain) [Stage 3] Domain Processor Manager (dedupe domains globally; per-domain semaphore) launches:

Seed fetch (homepage) + prioritized discovery URLs (parallel)
Adaptive expansion (only if < target_emails and budget pages left) [Stage 4] Email Extraction (pure functions) → emit emails immediately [Stage 5] Result Sink (async writer) writes CSV/Parquet; flush interval Cross‑cutting:
Caches: domain_done set (weak LRU), negative_email_cache (domains tried with no email), pattern cache.
Rate control: global http semaphore + per-domain semaphore + sliding window token bucket.
Observability: structured events per stage (timestamp, stage, latency_ms, counts).
Key concurrency knobs:

google_concurrency
domain_concurrency (domains processed in parallel)
per_domain_page_concurrency
max_pages_per_domain
max_pages_without_new_email (adaptive stop)
target_emails_per_domain
Phased implementation plan: Phase 1 (1–2 hrs): Make existing AsyncOrchestrator default, fix Google batch (remove batch=1), add per-domain semaphore + streaming result sink. Phase 2 (2–3 hrs): Restructure AsyncOrchestrator into pipeline queues (asyncio.Queue for domains & pages). Emit emails immediately. Phase 3 (1–2 hrs): Adaptive crawl controller (stop conditions, cancel pending page tasks). Phase 4 (1 hr): Observability metrics & periodic log snapshot (emails/sec, queue depths). Phase 5 (optional): Parquet sink + memory guard (auto prune caches > threshold).

Concrete first edits (suggested):

async_google_search: add parameter max_inflight; perform gather over all queries up to limit.
async_orchestrator: split process_company_async into:
schedule_google_query()
on_google_result() -> score -> maybe enqueue domain
domain_worker() tasks reading a domain_queue.
Add AsyncResultSink class with an internal queue & writer coroutine.
Throughput target approach:

Suppose 8 companies → average 1 good domain each; 50 pages/domain cap.
Aim: 200 req/min baseline. With aiohttp + keepalive you can push 20–40 req/s easily on laptop (network permitting). Start with global=60, per-domain=4.
Measure emails/sec; tune until CPU or outbound bandwidth saturates, not event loop idle.
Why not full restart from scratch?

You already have working async HTTP, Google, browser, extractor pieces. Cost-effective path: recompose orchestration (the “glue”), not re-implement primitives.
If you want, I can start by:

Making async path default & enlarging Google concurrency.
Adding domain & page queues, refactoring orchestrator. Say “proceed” and I’ll implement Phase 1 now.
Your call: proceed with Phase 1?