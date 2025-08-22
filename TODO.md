# TODO / Audit Roadmap

This file is the canonical, repo-level TODO for the current audit and follow-up work. The assistant will update this file as tasks are started/completed so future chats can pick up the exact state.

Guiding process
- Pick a single task by ID (e.g. `TASK-001`).
- The assistant will implement the task, add a small test or smoke check, run quick validation, and commit directly to the current branch.
- Each task entry contains: priority, contract (inputs/outputs), files to change, smoke tests, and suggested commit message.

Legend
- Priority: HIGH / MEDIUM / LOW
- Status: Open / In Progress / Done / Blocked

---

## Audit summary (short)
- High-priority fixes: TokenBucket sleep under lock, rotating User-Agent overwrite, Playwright graceful fallback, crawler default worker bug, orchestrator reset bug.
- Infra: tests, CI, logging, dev setup.

---

## Tasks

### TASK-001 — Fix TokenBucket (HIGH) [Done]
- Files: `scraper/http.py`
- Contract: TokenBucket.consume must never hold its internal lock while sleeping; it should still enforce rate limits correctly.
- Smoke test: run a small multi-threaded script calling .consume concurrently and assert threads don't block; run `scraper` with a 1-row input and confirm no global blocking.
- Commit message: `fix(http): avoid holding TokenBucket lock while sleeping (concurrency)`
- Result: Implemented on commit 9c731f7; added `tests/test_tokenbucket.py` verifying concurrent consumers complete within bounds.

### TASK-002 — Preserve rotating User-Agent (HIGH) [Done]
- Files: `scraper/http.py`
- Contract: If headers not provided, choose rotating UA and include it in request; if headers provided, merge UA only if User-Agent not set.
- Smoke test: run a small script making multiple requests and verify User-Agent rotates (DEBUG dumps or network capture).
- Commit message: `fix(http): preserve rotating User-Agent and merge provided headers`

### TASK-003 — Fix crawler default workers (MEDIUM) [Done]
- Files: `scraper/crawler.py`
- Contract: Use `config.max_workers` as default worker count when `num_workers` not provided.
- Smoke test: set `MAX_WORKERS=2` and verify `crawler.crawl_small` spawns 2 worker threads.
- Commit message: `fix(crawler): use config.max_workers as default worker count`

### TASK-027 — Domain Access Pattern Optimization (HIGH) [Done]
- Files: `scraper/http.py`, `scraper/orchestrator.py`, `scraper/sitemap.py`, `scraper/crawler.py`
- Contract: Implement domain pattern caching to avoid redundant www/http fallback attempts; probe access methods upfront and reuse patterns.
- Commit message: `perf(http): implement domain access pattern optimization to reduce redundant requests`
- Result: Added DomainPattern system with AccessMethod enum, domain probing logic, and integration across all components. Significantly reduces HTTP requests by eliminating redundant fallback attempts.

### TASK-004 — Clear orchestrator seen/in-progress on reset (MEDIUM) [Open]
- Files: `scraper/orchestrator.py`
- Contract: `Orchestrator.reset_stats()` should clear `_global_seen` and `_global_in_progress` under lock.
- Smoke test: run two sequential orchestrator runs in same process and ensure second run does not skip domains.
- Commit message: `fix(orchestrator): clear global seen and in-progress sets on reset`

### TASK-005 — BrowserService graceful degrade (HIGH) [Open]
- Files: `scraper/browser_service.py`
- Contract: If Playwright or browser binaries are not available, BrowserService.run() must not crash the process; `render()` should return empty string and log a clear warning.
- Smoke test: simulate Playwright launch failure (or run without browsers installed) and confirm scraper continues, JS fallback returns empty, and logs instruct `playwright install`.
- Commit message: `feat(browser): graceful degrade when Playwright or browser binaries are missing`

### TASK-006 — Reduce per-request logging (MEDIUM) [Open]
- Files: `scraper/http.py`, `scraper/cli.py`
- Contract: Change per-request `log.info` to `log.debug` and make default log level configurable via env var `LOGLEVEL`.
- Smoke test: run a short scrape and confirm fewer INFO lines.
- Commit message: `chore(log): lower per-request log level and add LOGLEVEL control`

### TASK-007 — Add --skip-google CLI flag (MEDIUM) [Open]
- Files: `scraper/cli.py`, `scraper/config.py`
- Contract: Allow running scraper without valid Google API keys by skipping Google validation and using fallback (if available).
- Smoke test: run without GOOGLE_API_KEY and pass `--skip-google` and confirm run proceeds (fallback search used).
- Commit message: `feat(cli): add --skip-google to allow runs without Google API keys`

### TASK-008 — Move / clean experimental extractor (LOW) [Open]
- Files: `scraper/email_extractor_manus.py` -> move to `scraper/experimental/` or remove
- Contract: Do not break imports; keep experimental code out of main path.
- Smoke test: run tests and sample run.
- Commit message: `chore(experimental): move experimental extractor out of main package`

### TASK-009 — Add unit tests (tests for http, extractor, domain scorer) (HIGH) [Open]
- Files: new `tests/test_http.py`, `tests/test_email_extractor.py`, `tests/test_domain_scorer.py`
- Contract: Add fast unit tests covering critical path and edge cases.
- Smoke test: run `pytest -q` and confirm tests pass.
- Commit message: `test: add unit tests for http TokenBucket, email extractor and domain scorer`

### TASK-010 — Add CI workflow (GitHub Actions) (MEDIUM) [Open]
- Files: `.github/workflows/ci.yml`
- Contract: Run lint, pytest and run `playwright install` (cached) on Ubuntu runner.
- Smoke test: push branch and inspect Actions run.
- Commit message: `ci: add GitHub Actions workflow (lint + tests + playwright install)`

### TASK-011 — Lint/format + pre-commit (MEDIUM) [Open]
- Files: `.pre-commit-config.yaml`, `pyproject.toml` (tool configs), optionally `setup.cfg`
- Contract: Add Black and Flake8 via pre-commit; configure line length and ignores; enable on CI.
- Smoke test: `pre-commit run -a` passes locally; CI runs `pre-commit`.
- Commit message: `chore(lint): add black+flake8 with pre-commit and CI hook`

### TASK-012 — Packaging & entrypoints (MEDIUM) [Open]
- Files: `pyproject.toml`, `scraper/__main__.py`, `scraper/cli.py`
- Contract: Ensure `pyproject.toml` has name/version/metadata and console_script `scraper=scraper.cli:main`; `python -m scraper` works.
- Smoke test: `pip install -e .` then `scraper --help` and `python -m scraper --help`.
- Commit message: `build(pkg): finalize pyproject metadata and console entrypoint`

### TASK-013 — Docs: dev/CONTRIBUTING/CHANGELOG/CoC (MEDIUM) [Open]
- Files: `README.dev.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `CODE_OF_CONDUCT.md`, update main `README.md`
- Contract: Add dev setup, workflows, “playwright install” note, PR rules, versioning.
- Smoke test: Lint markdown locally; links resolve.
- Commit message: `docs: add developer guide, contributing, changelog, code of conduct`

### TASK-014 — Secrets handling & .env flow (HIGH) [Open]
- Files: `.env.example`, `.gitignore`, `scraper/config.py`, `.github/workflows/ci.yml`
- Contract: Load env via `python-dotenv` (if present); document secrets in `.env.example`; ensure CI uses GitHub Secrets; never print secrets.
- Smoke test: local run with `.env`; CI run without secrets when `--skip-google` set.
- Commit message: `sec(ci): add .env example, dotenv support, and proper secret usage in CI`

### TASK-015 — Sitemap threadpool expression cleanup (LOW) [Open]
- Files: `scraper/sitemap.py`
- Contract: Replace `min(len(sitemap_urls), 4 or 1)` with clear bounded logic and handle zero URLs.
- Smoke test: run sitemap parsing with 0/1/10 URLs; no crash.
- Commit message: `chore(sitemap): clarify max_workers expression and guard zero`

### TASK-016 — Hybrid extractor cache limits/TTL + docs (LOW) [Open]
- Files: `scraper/hybrid_email_extractor.py`
- Contract: Make render cache size/TTL configurable; document lru_cache behavior (per-instance keying).
- Smoke test: set tiny TTL and verify cache evicts/refreshes.
- Commit message: `perf(extractor): add cache TTL/limits and document caching behavior`

### TASK-017 — Canonicalization/visited hardening + tests (MEDIUM) [Open]
- Files: `scraper/http.py` (helpers), `tests/test_urls.py`
- Contract: Add stricter helpers + tests for canonicalise/normalise and visited-set interactions to avoid false loops.
- Smoke test: run tests; crawl sample where www/non-www + http/https normalize correctly.
- Commit message: `fix(urls): harden canonicalization & visited handling with tests`

### TASK-018 — Google rate limiter robustness (LOW) [Open]
- Files: `scraper/google_search.py`
- Contract: Use `time.monotonic()` and simplify locking; unit tests for concurrent calls and spacing.
- Smoke test: threaded test ensures minimum interval respected without deadlocks.
- Commit message: `refactor(google): monotonic clock + cleaner locking for rate limit`

### TASK-019 — Dev setup scripts (MEDIUM) [Open]
- Files: `scripts/dev_setup.ps1`, `scripts/dev_setup.sh`
- Contract: One-step bootstrap: venv, `pip install -e .[dev]`, `playwright install`, `pre-commit install`.
- Smoke test: run both scripts on Windows/Linux; `scraper --help` works.
- Commit message: `chore(dev): add cross-platform dev setup scripts`

### TASK-020 — Centralize worker sizing (LOW) [Open]
- Files: `scraper/config.py`, `scraper/cli.py`, `scraper/crawler.py`, `scraper/orchestrator.py`
- Contract: Single source of truth for worker counts; avoid nested thread explosion.
- Smoke test: trace worker creation count with `LOGLEVEL=DEBUG`.
- Commit message: `refactor(concurrency): centralize worker sizing to avoid nested pools`

### TASK-021 — Requests session/adapter reuse review (LOW) [Open]
- Files: `scraper/http.py`
- Contract: Ensure per-thread session reuse is safe; tune adapter pool sizes; add tests for concurrency and connection reuse.
- Smoke test: benchmark small crawl before/after; connections reused (DEBUG logs).
- Commit message: `perf(http): ensure safe session/adapter reuse and tune pools`

### TASK-022 — Input sanitation for robots/sitemaps (MEDIUM) [Open]
- Files: `scraper/sitemap.py`, `scraper/robots.py` (if present), `tests/test_parsers.py`
- Contract: Sanitize/validate inputs; robust to malformed XML/headers; avoid injection into logs/paths.
- Smoke test: fuzz samples; no crashes, safe logging.
- Commit message: `sec(parse): sanitize inputs and harden robots/sitemap parsing`

### TASK-023 — Async I/O prototype path (LOW) [Open]
- Files: `async_scraper.py`, `docs/async-notes.md`
- Contract: Document existing async path; add micro-benchmark vs threads; outline migration risks.
- Smoke test: run prototype on 50 URLs; compare throughput.
- Commit message: `docs(async): benchmark and notes for async scraper path`

### TASK-024 — Dependency pinning/constraints (MEDIUM) [Open]
- Files: `requirements.txt` (or `requirements.in` + `requirements.txt`), `pyproject.toml`
- Contract: Pin critical versions; optionally use pip-tools to compile; document update process.
- Smoke test: clean venv install reproduces; CI green.
- Commit message: `build(deps): pin critical versions and document update flow`

### TASK-025 — Test matrix via tox/nox (LOW) [Open]
- Files: `tox.ini` or `noxfile.py`, CI workflow update
- Contract: Run tests/lint across Python versions; integrate with CI.
- Smoke test: CI shows matrix runs passing.
- Commit message: `ci: add tox/nox test matrix and integrate with GitHub Actions`

### TASK-026 — Documentation: config & examples (.env, Playwright) (LOW) [Open]
- Files: `README.md`, `.env.example`
- Contract: Expand config docs including `--skip-google`, Playwright install, LOGLEVEL; cross-link to developer docs.
- Smoke test: New user can follow README to first successful run without Google keys.
- Commit message: `docs: expand configuration examples and first-run guidance`

---

## Performance & Efficiency Improvements

### TASK-028 — Concurrent Domain Processing (HIGH) [Done]
- Files: `scraper/orchestrator.py`, `scraper/cli.py`
- Contract: Process multiple domains in parallel using ThreadPoolExecutor; batch domain probing; pipeline processing.
- Smoke test: Process 10 companies and verify multiple domains processed simultaneously with proper synchronization.
- Commit message: `perf(orchestrator): add concurrent domain processing with ThreadPoolExecutor`
- Result: Implemented 3-phase concurrent processing: domain collection, batch probing, and parallel domain processing. Added proper synchronization with global state tracking and early termination handling.

### TASK-029 — Smarter Email Discovery Strategy (HIGH) [Open]
- Files: `scraper/orchestrator.py`, `scraper/config.py`
- Contract: Prioritize "Contact", "About", "Team" pages; stop early when emails found; cache common email patterns per domain.
- Smoke test: Verify contact pages are processed before generic pages; early stopping works when emails found on homepage.
- Commit message: `perf(email): implement smart email discovery with page prioritization and early stopping`

### TASK-030 — Content-Type Filtering (MEDIUM) [Open]
- Files: `scraper/http.py`, `scraper/crawler.py`
- Contract: Use HEAD requests to check Content-Type before downloading; whitelist only text/html and text/plain.
- Smoke test: Verify images/PDFs/videos are skipped without full download; only HTML pages processed.
- Commit message: `perf(http): add Content-Type filtering to skip non-HTML content`

### TASK-031 — Response Caching & Deduplication (MEDIUM) [Open]
- Files: `scraper/http.py`, `scraper/email_extractor.py`
- Contract: Cache identical page content across URLs using content hashes; avoid re-processing duplicate content.
- Smoke test: Verify duplicate content is detected and cached; email extraction skipped for duplicates.
- Commit message: `perf(cache): implement response content caching and deduplication`

### TASK-032 — Database/Persistent Storage (MEDIUM) [Open]
- Files: `scraper/storage.py`, `scraper/orchestrator.py`, `scraper/config.py`
- Contract: Cache domain patterns, company results, and email findings to disk; support resume functionality.
- Smoke test: Verify results persist across runs; interrupted batches can be resumed; no duplicate processing.
- Commit message: `feat(storage): add persistent storage for domain patterns and results caching`

### TASK-033 — Intelligent Crawl Depth (MEDIUM) [Open]
- Files: `scraper/crawler.py`, `scraper/orchestrator.py`
- Contract: Dynamic crawl depth based on email discovery rate; priority queue for URLs; stop early when no emails found.
- Smoke test: Verify crawl depth adapts based on email discovery; contact pages prioritized over blog posts.
- Commit message: `perf(crawler): implement adaptive crawl depth and URL prioritization`

### TASK-034 — Network Optimizations (LOW) [Open]
- Files: `scraper/http.py`
- Contract: HTTP/2 connection reuse, request compression (gzip/deflate), improved connection pooling, DNS caching.
- Smoke test: Verify HTTP/2 connections reused; compression enabled; DNS lookups cached.
- Commit message: `perf(network): add HTTP/2 support, compression, and enhanced connection pooling`

### TASK-035 — Preprocessing & URL Filtering (LOW) [Open]
- Files: `scraper/crawler.py`, `scraper/config.py`
- Contract: Skip obviously non-email URLs (/blog/, /news/, etc.); filter by extensions (.jpg, .png, etc.).
- Smoke test: Verify blog posts and media files are skipped; only relevant URLs crawled.
- Commit message: `perf(crawler): add URL pattern filtering to skip non-email content`

### TASK-036 — Machine Learning URL Scoring (LOW) [Open]
- Files: `scraper/ml_scorer.py`, `scraper/crawler.py`
- Contract: Train lightweight model to predict email probability from URL patterns; focus crawling on high-probability URLs.
- Smoke test: Verify /contact scores higher than /blog/post-123; crawling focuses on high-scoring URLs.
- Commit message: `feat(ml): add ML-based URL scoring for email probability prediction`

### TASK-037 — Batch Processing Optimizations (MEDIUM) [Open]
- Files: `scraper/orchestrator.py`, `scraper/cli.py`
- Contract: Company clustering by domain similarity; domain deduplication; result streaming; progress tracking.
- Smoke test: Verify companies with same domain are deduplicated; results written incrementally.
- Commit message: `perf(batch): add company clustering, domain deduplication, and result streaming`

### TASK-038 — Resource Monitoring & Adaptive Limits (LOW) [Open]
- Files: `scraper/orchestrator.py`, `scraper/config.py`
- Contract: Monitor memory/CPU usage; adjust concurrency dynamically; prevent system overload.
- Smoke test: Verify worker threads reduced when memory usage high; system remains responsive.
- Commit message: `perf(resources): add adaptive resource monitoring and concurrency adjustment`

### TASK-039 — Smart Retry Logic & Circuit Breaker (LOW) [Open]
- Files: `scraper/http.py`
- Contract: Exponential backoff with jitter; circuit breaker for consistently failing domains; selective retry by error type.
- Smoke test: Verify exponential backoff works; circuit breaker prevents repeated failures; 404s not retried.
- Commit message: `perf(retry): implement smart retry logic with circuit breaker pattern`

---

## Minor corrections & clarifications (added)

- lru_cache on instance methods: note that `functools.lru_cache` used on instance methods will include `self` in the cache key, so caches are per-instance/per-process. This is acceptable but should be documented next to the decorator so future maintainers understand the scope.
- `sitemap` thread-pool expression clarity: change `min(len(sitemap_urls), 4 or 1)` to `min(max(1, len(sitemap_urls)), 4)` or `max(1, min(len(sitemap_urls), 4))` to make intent explicit and correctly handle empty lists.
- canonicalisation unit tests: add small unit test matrix for `canonicalise` / `normalise_domain` to cover domain-only inputs, scheme+host inputs, trailing slash normalization, uppercase host, and default ports.

## Small gaps worth adding (added)

- BrowserService shutdown handling: ensure the child process terminates cleanly on parent exit or on interrupt (Ctrl-C). Add explicit termination/cleanup in orchestrator shutdown path to avoid orphan processes.
- BrowserService IPC/backpressure: handle full IPC queues or service-unavailable cases to avoid producer blocking or dropped requests — add queue size guards and timeouts and have `render()` return quickly when service unavailable.
- Playwright Windows path/permission check: add a runtime check that prints the exact missing-executable path and the Python environment used to run `playwright install`, with clear instructions for Windows users (e.g., run from activated venv/powershell).
- Dependency pinning recommendation: add a `requirements-dev.txt` or `pyproject.toml` note and recommend pinning critical deps for reproducible builds; include this as a medium-term infra task (CI/TASK-010 related).
- Sitemap/robots DoS protection: when parsing remote sitemap/robots files, limit total bytes parsed and time spent parsing to avoid large-file DoS vectors.

---

## How to mark progress
- When you ask the assistant to start a task, it will set `Status: In Progress` and update this file.
- When done, assistant will set `Status: Done`, include a short summary, and commit the changes directly to the current branch.

---

## Notes
- Keep changes small and test-driven. We will implement one task per commit and smoke test before committing.
- Use the backup branch `main-backup-YYYYMMDD_HHMMSS` (already created earlier) if you need to restore previous `main`.

---

Last updated: 2025-08-22 (TASK-027 done, performance improvements added)

