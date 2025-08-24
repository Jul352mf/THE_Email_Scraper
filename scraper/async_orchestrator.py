"""Async orchestration module.

Coordinates async scraping, Google search, domain probing, and hybrid
email extraction with layered heuristics.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from .config import (
    config as settings,  # Config instance (avoid module-level shadowing)
)
from . import (
    domain_scorer as domain_scorer_module,
    smart_discovery,
    sitemap as sitemap_parser,
)
from .hybrid_email_extractor import hybrid_email_extractor
from . import email_extractor
from .http_client import normalise_domain, validate_url
from .async_browser_service import get_async_browser_service
import logging
log = logging.getLogger(__name__)


class AsyncOrchestrator:
    """Main async coordinator for company -> domain -> email extraction.

    Parameters
    ----------
    max_concurrent_companies : int
        Limit of companies processed in parallel.
    per_domain_page_concurrency : int | None
        Optional override for per-domain page fetch concurrency. If not
        provided, falls back to config.async_per_domain_page_concurrency or 3.
    """

    def __init__(
        self,
        max_concurrent_companies: int = 50,
        per_domain_page_concurrency: Optional[int] = None,
        **kwargs,
    ):
        """Create orchestrator.

        Accepts legacy/extra kwargs (ignored) so older CLI code doesn't break.
        Recognised legacy keys:
          - async_per_domain_page_concurrency
          - company_concurrency
        """
        # Backward compatibility remapping
        if per_domain_page_concurrency is None:
            if 'async_per_domain_page_concurrency' in kwargs:
                per_domain_page_concurrency = kwargs.get(
                    'async_per_domain_page_concurrency'
                )
            elif 'per_domain_page_concurrency' in kwargs:  # defensive
                per_domain_page_concurrency = kwargs.get(
                    'per_domain_page_concurrency'
                )
        if 'company_concurrency' in kwargs and max_concurrent_companies == 50:
            # Allow override if caller passed legacy name
            max_concurrent_companies = kwargs['company_concurrency']

        self.max_concurrent_companies = max_concurrent_companies
        self._company_semaphore = asyncio.Semaphore(max_concurrent_companies)
        self.per_domain_page_concurrency = (
            per_domain_page_concurrency
            if per_domain_page_concurrency is not None
            else getattr(settings, 'async_per_domain_page_concurrency', 3)
        )

        # Initialize core async service clients
        from .async_http_client import AsyncHttpClient
        from .async_google_search import AsyncGoogleSearchClient

        self.http_client = AsyncHttpClient()
        self.google_client = AsyncGoogleSearchClient()
        # Simple async domain tracker (processed/in-progress sets)
        self._processed_domains: set[str] = set()
        self._in_progress_domains: set[str] = set()
        self._domain_lock = asyncio.Lock()
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        # Lightweight async domain tracking facade (backward compatible API)
        self.domain_tracker = self._DomainTracker(self)

    class _DomainTracker:
        """Internal async domain tracking facade.

        Provides the API expected by existing orchestrator code while
        leveraging the parent instance's sets & lock.
        """

        def __init__(self, parent: "AsyncOrchestrator"):
            self._p = parent

        async def is_domain_processed(self, domain: str) -> bool:
            async with self._p._domain_lock:
                return domain in self._p._processed_domains

        async def mark_domain_in_progress(self, domain: str) -> bool:
            async with self._p._domain_lock:
                if (
                    domain in self._p._processed_domains
                    or domain in self._p._in_progress_domains
                ):
                    return False
                self._p._in_progress_domains.add(domain)
                return True

        async def mark_domain_completed(self, domain: str):  # pragma: no cover
            async with self._p._domain_lock:
                self._p._in_progress_domains.discard(domain)
                self._p._processed_domains.add(domain)

        async def get_stats(self) -> Dict[str, int]:  # pragma: no cover
            async with self._p._domain_lock:
                return {
                    "processed": len(self._p._processed_domains),
                    "in_progress": len(self._p._in_progress_domains),
                }

    # Context management -------------------------------------------------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover
        await self.http_client.close()
        await self.google_client.close()
        return False

    # Utility ------------------------------------------------------------
    def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domain_semaphores:
            self._domain_semaphores[domain] = asyncio.Semaphore(
                self.per_domain_page_concurrency
            )
        return self._domain_semaphores[domain]

    # Batch company processing -------------------------------------------
    async def process_companies_batch(
        self, companies: List[str]
    ) -> Tuple[Counter, List[Dict[str, str]]]:
        start_time = time.time()

        async def process_with_semaphore(company: str):
            async with self._company_semaphore:
                return await self.process_company_async(company)

        try:
            results = await asyncio.gather(
                *[process_with_semaphore(c) for c in companies],
                return_exceptions=True,
            )
            combined_stats = Counter()
            combined_rows: List[Dict[str, str]] = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    log.error(
                        "Company processing failed: %s - %s",
                        companies[i],
                        result,
                    )
                    combined_stats["processing_errors"] += 1
                else:
                    stats, rows = result
                    combined_stats.update(stats)
                    combined_rows.extend(rows)
            elapsed = time.time() - start_time
            cps = len(companies) / elapsed if elapsed > 0 else 0
            log.info(
                (
                    "Async batch completed: %d companies in %.2fs "
                    "(%.2f companies/sec, %d emails)"
                ),
                len(companies),
                elapsed,
                cps,
                len(combined_rows),
            )
            return combined_stats, combined_rows
        except Exception as e:  # pragma: no cover
            log.error("Async batch processing failed: %s", e)
            return Counter({"batch_processing_errors": 1}), []
    
    async def process_company_async(
        self, company: str
    ) -> Tuple[Counter, List[Dict[str, str]]]:
        """
        Process a single company with full async pipeline.
        """
        stats = Counter()
        rows = []
        stats["leads"] += 1
        
        log.debug("▶ Async processing company: %s", company)
        
        try:
            # Phase 1: Google search + parse snippet/title for emails
            search_results = await self.google_client.search_single(
                f'"{company}" contact email',
                num_results=10
            )
            
            if not search_results:
                stats["no_google"] += 1
                log.warning("No Google results for: %s", company)
                if getattr(settings, 'enable_domain_guessing', True):
                    guess = company.split()[0].lower().replace(',', '')
                    guessed_domain = f"{guess}.com"
                    log.info(
                        "Domain guessing enabled; trying %s for %s",
                        guessed_domain,
                        company,
                    )
                    # Minimal domain stats path
                    try:
                        domain_stats, domain_rows = await (
                            self._process_domain_async(
                                company,
                                guessed_domain,
                                50,
                            )
                        )
                        stats.update(domain_stats)
                        rows.extend(domain_rows)
                    except Exception:
                        pass
                    return stats, rows
                return stats, rows
            
            # Snippet harvesting removed for precision (option deprecated)
            # (Legacy block intentionally eliminated.)

            # Phase 2: Domain scoring (or simple domain match)
            try:
                simple_match = getattr(settings, 'simple_domain_match', True)
                score = 0
                link = None
                if simple_match:
                    # pick first result whose displayLink or link contains
                    # the company root token
                    company_token = company.split()[0].lower().replace(',', '')
                    for item in search_results:
                        candidate = (
                            item.get('displayLink')
                            or item.get('link')
                            or ''
                        ).lower()
                        if not candidate:
                            continue
                        if company_token in candidate:
                            link = (
                                item.get('link')
                                or item.get('formattedUrl')
                                or candidate
                            )
                            score = 100  # trust simple match
                            break
                if not link:
                    score, link = (
                        domain_scorer_module.domain_scorer.find_best_domain(
                            company,
                            search_results,
                        )
                    )
                if score < settings.domain_score_threshold:
                    stats["domain_unclear"] += 1
                    return stats, rows
            except Exception as e:
                stats["domain_error"] += 1
                log.error("Domain scoring error for %s: %s", company, e)
                return stats, rows
            
            # Phase 3: Domain processing (async)
            domain = normalise_domain(link)
            
            # Check if domain already processed (with async lock)
            if await self.domain_tracker.is_domain_processed(domain):
                log.debug("↩ Skipping %s: domain already processed", domain)
                stats["skipped_domain"] += 1
                return stats, rows
            
            # Mark domain as in progress
            if not await self.domain_tracker.mark_domain_in_progress(domain):
                log.debug("↩ Domain %s taken by another task", domain)
                stats["skipped_domain"] += 1
                return stats, rows
            
            try:
                # Process domain with async operations
                domain_stats, domain_rows = await self._process_domain_async(
                    company, domain, score
                )
                
                stats.update(domain_stats)
                rows.extend(domain_rows)
                
            finally:
                # Always mark domain as completed
                await self.domain_tracker.mark_domain_completed(domain)
            
            return stats, rows
            
        except Exception as e:
            log.error("Company processing error for %s: %s", company, e)
            stats["processing_errors"] += 1
            return stats, rows
    
    async def _process_domain_async(
        self, company: str, domain: str, score: int
    ) -> Tuple[Counter, List[Dict[str, str]]]:
        """
        Process a domain with async operations.
        """
        stats = Counter()
        rows = []
        
        log.debug("🌐 Processing domain: %s (score: %d)", domain, score)
        
        try:
            # Probe domain access patterns (async)
            pattern = await self.http_client.probe_domain_access(domain)
            if not pattern:
                log.warning(
                    "Could not establish connection to domain: %s",
                    domain,
                )
                stats["connection_failed"] += 1
                return stats, rows
            
            # Build optimized URL using successful pattern
            base_url = pattern.build_url()
            log.debug("Using optimized URL pattern: %s", base_url)
            
            # Email extraction with async HTTP requests
            emails_found = await self._extract_emails_async(
                company, domain, base_url
            )
            
            if emails_found:
                stats["success"] += 1
                stats["emails_found"] += len(emails_found)
                
                # Create result rows
                for email in emails_found:
                    rows.append({
                        "company": company,
                        "domain": domain,
                        "email": email,
                        "score": str(score),
                        "source": "async_extraction"
                    })
                
                log.info(
                    "✅ Found %d emails for %s (%s)",
                    len(emails_found), company, domain
                )
            else:
                stats["no_email"] += 1
                log.debug("No emails found for %s (%s)", company, domain)
            
            return stats, rows
            
        except Exception as e:
            log.error(
                "Domain processing error for %s (%s): %s",
                company,
                domain,
                e,
            )
            stats["domain_processing_errors"] += 1
            return stats, rows
    
    async def _extract_emails_async(
        self, company: str, domain: str, base_url: str
    ) -> List[str]:
        """Extract emails using async HTTP operations with layered strategies.

        Reuse the proven hybrid static extraction pass (Cloudflare decode,
        ROT13, Base64, JS char codes) for each fetched HTML before applying
        relevance filtering. JS rendering only as a last resort.
        """
        raw_candidates: set[str] = set()
        emails: set[str] = set()
        visited_urls: set[str] = set()
        enable_sitemap_first = getattr(settings, 'enable_sitemap_first', True)
        early_stop_threshold = getattr(settings, 'early_stop_threshold', 100)
        max_emails_cap = getattr(settings, 'max_emails_per_domain', 150)

        try:
            # Main page
            domain_sem = self._get_domain_semaphore(domain)
            async with domain_sem:
                if base_url not in visited_urls:
                    main_resp = await self.http_client.safe_get(
                        base_url, timeout=15
                    )
                    visited_urls.add(base_url)
                else:
                    main_resp = None
            content = ""
            if main_resp and main_resp.status == 200:
                try:
                    content = await main_resp.text()
                    home_emails = hybrid_email_extractor._static_pass(
                        content, base_url
                    )
                    if home_emails:
                        raw_candidates.update(home_emails)
                        if getattr(settings, 'debug_email_candidates', False):
                            log.debug("Home candidates=%d", len(home_emails))
                except Exception:
                    pass

            # Sitemap (always run if enabled)
            sitemap_urls: List[str] = []
            if enable_sitemap_first:
                try:
                    pri, _used = await asyncio.to_thread(
                        sitemap_parser.get_priority_urls, domain
                    )
                    if pri:
                        limit = getattr(settings, 'max_fallback_pages', 20)
                        sitemap_urls = pri[:limit]
                        if getattr(settings, 'debug_email_candidates', False):
                            log.debug(
                                "[SITEMAP] %d priority URLs", len(pri)
                            )
                except Exception:
                    pass
            if sitemap_urls:
                async def fetch_s(u: str):
                    if u in visited_urls:
                        return u, None
                    async with domain_sem:
                        r = await self.http_client.safe_get(u, timeout=12)
                        visited_urls.add(u)
                        return u, r
                tasks = [
                    asyncio.create_task(fetch_s(u))
                    for u in sitemap_urls[:10]
                ]
                for coro in asyncio.as_completed(tasks):
                    try:
                        url, resp = await coro
                    except Exception:
                        continue
                    if not resp or resp.status != 200:
                        continue
                    try:
                        sc = await resp.text()
                        sm_emails = hybrid_email_extractor._static_pass(
                            sc, url
                        )
                        if sm_emails:
                            before = len(raw_candidates)
                            raw_candidates.update(sm_emails)
                            if (
                                getattr(
                                    settings,
                                    'debug_email_candidates',
                                    False,
                                )
                                and len(raw_candidates) > before
                            ):
                                log.debug(
                                    "[SITEMAP] %s +%d (total=%d)",
                                    url,
                                    len(raw_candidates) - before,
                                    len(raw_candidates),
                                )
                        if (
                            getattr(settings, 'enable_early_stopping', False)
                            and len(raw_candidates) >= early_stop_threshold
                        ):
                            break
                    except Exception:
                        continue

            # BFS crawl if below threshold
            candidate_threshold = getattr(
                settings, 'email_candidate_threshold', 3
            )
            if len(raw_candidates) < candidate_threshold:
                from urllib.parse import (
                    urlparse, urljoin, parse_qsl, urlencode, urlunparse
                )
                from collections import deque
                from bs4 import BeautifulSoup
                
                def _canonicalize(u: str) -> str:
                    try:
                        parsed = urlparse(u)
                        scheme, netloc, path, params, query, _f = parsed
                        pairs = parse_qsl(query, keep_blank_values=True)
                        pairs = [
                            p for p in pairs if not p[0].startswith('utm_')
                        ]
                        pairs.sort()
                        qn = urlencode(pairs)
                        return urlunparse(
                            (scheme, netloc, path, params, qn, '')
                        )
                    except Exception:
                        return u
                q = deque()
                seen_canon: set[str] = set()
                
                def _enqueue(u: str):
                    c = _canonicalize(u)
                    if c in seen_canon or c in visited_urls:
                        return
                    seen_canon.add(c)
                    q.append(c)
                try:
                    if content:
                        soup0 = BeautifulSoup(content, 'html.parser')
                        for a in soup0.find_all('a', href=True):
                            href = a['href'].strip()
                            if (
                                href.startswith('#')
                                or href.lower().startswith('mailto:')
                            ):
                                continue
                            full = urljoin(base_url, href)
                            if not validate_url(full):
                                continue
                            if domain not in normalise_domain(
                                urlparse(full).netloc
                            ):
                                continue
                            _enqueue(full)
                except Exception:
                    pass
                page_limit = getattr(settings, 'max_fallback_pages', 50)
                start_t = time.time()
                max_time = min(60, page_limit * 2)
                fetched = 0
                while (
                    q
                    and fetched < page_limit
                    and len(raw_candidates) < candidate_threshold
                ):
                    if time.time() - start_t > max_time:
                        break
                    nxt = q.popleft()
                    if nxt in visited_urls:
                        continue
                    async with domain_sem:
                        r2 = await self.http_client.safe_get(nxt, timeout=10)
                        visited_urls.add(nxt)
                    if not r2 or r2.status != 200:
                        continue
                    fetched += 1
                    try:
                        h2 = await r2.text()
                        try:
                            pe = hybrid_email_extractor._static_pass(h2, nxt)
                        except Exception:
                            pe = set()
                        if pe:
                            b = len(raw_candidates)
                            raw_candidates.update(pe)
                            if (
                                getattr(
                                    settings,
                                    'debug_email_candidates',
                                    False,
                                )
                                and len(raw_candidates) > b
                            ):
                                log.debug(
                                    "[CRAWL] %s +%d (total=%d)",
                                    nxt,
                                    len(raw_candidates) - b,
                                    len(raw_candidates),
                                )
                        try:
                            soup2 = BeautifulSoup(h2, 'html.parser')
                            for a in soup2.find_all('a', href=True):
                                href = a['href'].strip()
                                if (
                                    href.startswith('#')
                                    or href.lower().startswith('mailto:')
                                ):
                                    continue
                                full = urljoin(nxt, href)
                                if not validate_url(full):
                                    continue
                                if domain not in normalise_domain(
                                    urlparse(full).netloc
                                ):
                                    continue
                                _enqueue(full)
                        except Exception:
                            pass
                    except Exception:
                        continue

            # JS fallback
            if not raw_candidates:
                try:
                    async with domain_sem:
                        js_em = await self._hybrid_async_fallback(
                            company, domain, base_url
                        )
                    if js_em:
                        raw_candidates.update(js_em)
                        if getattr(settings, 'debug_email_candidates', False):
                            log.debug("[JS] candidates=%d", len(js_em))
                except Exception:
                    pass

            # Filtering
            minimal = getattr(settings, 'minimal_email_validation', True)
            if raw_candidates:
                if minimal:
                    emails = raw_candidates
                else:
                    emails = (
                        smart_discovery.filter_relevant_emails(
                            raw_candidates, domain
                        )
                        or set()
                    )
            valid = [
                e for e in emails
                if (minimal or self._is_valid_email(e, domain))
            ]
            if getattr(settings, 'debug_email_candidates', False):
                log.debug(
                    "Summary %s raw=%d filtered=%d valid=%d",
                    domain,
                    len(raw_candidates),
                    len(emails),
                    len(valid),
                )
            if len(valid) > max_emails_cap:
                valid = valid[:max_emails_cap]
            return valid
        except Exception as e:  # pragma: no cover
            log.error("Email extraction failed for %s: %s", domain, e)
            return []
    
    def _is_valid_email(self, email: str, domain: str) -> bool:
        """
        Validate email address quality and relevance.
        """
        email_lower = email.lower()
        
        # Skip patterns (except allowlisted role emails if enabled)
        role_patterns = {'support@', 'info@', 'sales@', 'admin@'}
        skip_patterns = [
            'example@', '@example', 'test@', '@test',
            'noreply@', 'no-reply@', 'donotreply@'
        ]
        if not getattr(settings, 'allow_generic_role_emails', True):
            skip_patterns.extend(role_patterns)
        
        for pattern in skip_patterns:
            if pattern in email_lower:
                return False
        
        # Prefer emails from the same domain
        if f"@{domain.lower()}" in email_lower:
            return True
        
        # Check for reasonable email format
        if '@' not in email or '.' not in email.split('@')[1]:
            return False
        
        return True
    
    def _generate_discovery_urls(self, base_url: str) -> List[str]:
        """Generate common URLs where emails are likely to be found."""
        if not base_url:
            return []
        
        # Extract domain from base_url
        if '://' in base_url:
            domain = base_url.split('://')[1].split('/')[0]
        else:
            domain = base_url.split('/')[0]
        
        # Use existing priority path parts from config
        from scraper.config import HIGH_PRIORITY_PARTS, MEDIUM_PRIORITY_PARTS
        
        # Combine high and medium priority parts for discovery
        priority_paths = HIGH_PRIORITY_PARTS + MEDIUM_PRIORITY_PARTS[:5]
        
        discovery_urls = []
        base = f"https://{domain}"
        
        for path in priority_paths:
            discovery_urls.append(f"{base}/{path}")
            discovery_urls.append(f"{base}/{path}/")
        
        return discovery_urls

    # ------------------------------------------------------------------
    async def process_companies_streaming(
        self,
        companies: List[str],
        *,
        on_company_start: Optional[
            Union[Callable[[str], None], Callable[[str], Awaitable[None]]]
        ] = None,
        on_rows: Optional[Callable[[List[Dict[str, str]]], None]] = None,
    ) -> Tuple[Counter, List[Dict[str, str]]]:
        """Stream Google searches and schedule domain work immediately.
        
    This reduces latency vs per-company sequential path and allows
    early saturation of HTTP layer.
    """
        stats = Counter()
        rows: List[Dict[str, str]] = []
        domain_tasks = set()

        log.info(
            "Starting streaming processing of %d companies", len(companies)
        )

        # NOTE: Pre-marking disabled temporarily to debug stall issue
        # Will re-enable after confirming Google streaming works
        
        log.info("[ORCHESTRATOR] About to start Google streaming generator")
        async for (
            company,
            results,
        ) in self.google_client.search_companies_streaming(companies):
            log.info("[ORCHESTRATOR] Got result from streaming: %s", company)
            log.debug(
                f"[TRACE] Pulled company from google search: {company}"
            )
            # Call progress callback now that we have actual result
            if on_company_start:
                try:
                    result = on_company_start(company)
                    if hasattr(result, '__await__'):  # support async callbacks
                        await result
                except Exception as e:
                    log.debug(f"Progress callback error for {company}: {e}")
            stats["leads"] += 1
            if not results:
                log.debug(f"[TRACE] No google results for: {company}")
                stats["no_google"] += 1
                # Domain guessing fallback (not previously in streaming path)
                if getattr(settings, 'enable_domain_guessing', True):
                    guess_token = (
                        company.split()[0].lower().replace(',', '')
                        or company.lower().split()[0]
                    )
                    guessed_domain = f"{guess_token}.com"
                    try:
                        domain = normalise_domain(guessed_domain)
                        if await self.domain_tracker.is_domain_processed(
                            domain
                        ):
                            stats["skipped_domain"] += 1
                            continue
                        if not await self.domain_tracker.mark_domain_in_progress(
                            domain
                        ):
                            stats["skipped_domain"] += 1
                            continue

                        async def run_guess(company: str, domain: str):
                            try:
                                d_stats, d_rows = await (
                                    self._process_domain_async(
                                        company,
                                        domain,
                                        50,  # nominal score guessed
                                    )
                                )
                                return domain, d_stats, d_rows
                            finally:
                                await self.domain_tracker.mark_domain_completed(
                                    domain
                                )

                        task = asyncio.create_task(run_guess(company, domain))
                        domain_tasks.add(task)
                        log.debug(
                            (
                                "[TRACE] Scheduled guessed domain task %s "
                                "for %s"
                            ),
                            domain,
                            company,
                        )
                        continue
                    except Exception as e:  # pragma: no cover
                        log.debug(
                            "[TRACE] Domain guessing failed for %s: %s",
                            company,
                            e,
                        )
                        continue
                else:
                    continue
            try:
                log.info(
                    "[ORCHESTRATOR] Starting domain scoring for: %s", company
                )
                score, link = (
                    domain_scorer_module.domain_scorer.find_best_domain(
                        company, results
                    )
                )
                log.info(
                    "[ORCHESTRATOR] Domain scoring complete: %s -> %s "
                    "(score=%d)",
                    company, link, score
                )
                log.debug(f"[TRACE] Domain scored for {company}: {link}")
            except Exception as e:  # pragma: no cover
                log.debug("Domain scoring error %s: %s", company, e)
                stats["domain_error"] += 1
                continue
            if score < settings.domain_score_threshold:
                log.info(
                    "[ORCHESTRATOR] Domain score too low for %s: %d < %d",
                    company, score, settings.domain_score_threshold
                )
                stats["domain_unclear"] += 1
                continue
            log.info(
                "[ORCHESTRATOR] Domain passed threshold for %s: %d >= %d",
                company, score, settings.domain_score_threshold
            )
            domain = normalise_domain(link)
            log.info(
                "[ORCHESTRATOR] Normalized domain: %s -> %s", link, domain
            )
            
            # Deduplicate domain scheduling
            if await self.domain_tracker.is_domain_processed(domain):
                log.info("[ORCHESTRATOR] Domain already processed: %s", domain)
                stats["skipped_domain"] += 1
                continue
            if not await self.domain_tracker.mark_domain_in_progress(domain):
                log.info(
                    "[ORCHESTRATOR] Domain already in progress: %s", domain
                )
                stats["skipped_domain"] += 1
                continue

            log.info(
                "[ORCHESTRATOR] Creating domain task: %s for company %s",
                domain, company
            )

            async def run_domain(company: str, domain: str, score: int):
                try:
                    d_stats, d_rows = await self._process_domain_async(
                        company, domain, score
                    )
                    return domain, d_stats, d_rows
                finally:
                    await self.domain_tracker.mark_domain_completed(domain)

            task = asyncio.create_task(run_domain(company, domain, score))
            domain_tasks.add(task)
            log.info(
                "[ORCHESTRATOR] Task scheduled. Total domain tasks: %d",
                len(domain_tasks)
            )

        # Collect domain task results as they finish
        log.info(
            "[ORCHESTRATOR] Google streaming complete. Collecting %d domain "
            "tasks", len(domain_tasks)
        )
        for task in asyncio.as_completed(domain_tasks):
            try:
                log.debug("[ORCHESTRATOR] Waiting for domain task completion")
                domain, d_stats, d_rows = await task
                log.info(
                    "[ORCHESTRATOR] Domain task completed: %s (%d rows)",
                    domain, len(d_rows)
                )
                stats.update(d_stats)
                if d_rows:
                    rows.extend(d_rows)
                    if on_rows:
                        try:
                            on_rows(d_rows)
                        except Exception:  # pragma: no cover
                            pass
            except Exception as e:  # pragma: no cover
                log.error("Domain task failed: %s", e)
                stats["domain_processing_errors"] += 1

        return stats, rows

    # ------------------------------------------------------------------
    async def _hybrid_async_fallback(
        self,
        company: str,
        domain: str,
        base_url: str,
        *,
        max_discovery: int = 5,
    ) -> List[str]:
        """Async version of the hybrid extraction to avoid blocking.

    Re-implements a minimal subset of
    `HybridEmailExtractor.extract_emails_hybrid` using async HTTP +
    optional JS rendering via `AsyncBrowserService`.
        Cached results kept in the original (sync) hybrid extractor cache to
        benefit future calls.
        """
        # If sync hybrid already cached, reuse immediately
        try:
            cache = getattr(hybrid_email_extractor, "_domain_cache", {})
            if domain in cache:
                return list(cache[domain])
        except Exception:
            pass

        emails: set[str] = set()

        # 1. Base page (async HTTP)
        try:
            resp = await self.http_client.safe_get(base_url, timeout=15)
            if resp and resp.status == 200:
                try:
                    content = await resp.text()
                    emails.update(
                        email_extractor.extract_emails_from_content(content)
                    )
                except Exception:
                    pass
        except Exception:
            pass

        # 2. Discovery pages (async HTTP, early stop)
        if not emails:
            discovery_urls = self._generate_discovery_urls(base_url)[
                :max_discovery
            ]
            if discovery_urls:
                results = await self.http_client.batch_get(
                    discovery_urls, timeout=10
                )
                for url, response in results:
                    if response and response.status == 200:
                        try:
                            content = await response.text()
                            page_emails = (
                                email_extractor.extract_emails_from_content(
                                    content
                                )
                            )
                            if page_emails:
                                filtered = (
                                    smart_discovery
                                    .filter_relevant_emails(
                                        page_emails,
                                        domain,
                                    )
                                )
                                emails.update(filtered)
                                if len(emails) >= 3:
                                    break
                        except Exception:
                            continue

        # 3. JS rendering fallback (async Playwright)
        if not emails:
            try:
                svc = await get_async_browser_service()
                html = await svc.render(base_url, timeout=20)
                if html:
                    js_emails = email_extractor.extract_emails_from_content(
                        html
                    )
                    if js_emails:
                        minimal_mode = getattr(
                            settings, 'minimal_email_validation', True
                        )
                        if minimal_mode:
                            emails.update(js_emails)
                        else:
                            emails.update(
                                smart_discovery.filter_relevant_emails(
                                    js_emails, domain
                                )
                            )
            except Exception as e:
                log.debug("Async JS render failed for %s: %s", domain, e)

        # Store in sync hybrid cache for reuse
        try:
            if emails:
                # cache sync hybrid extractor for reuse
                hybrid_email_extractor._domain_cache[domain] = set(
                    emails
                )
        except Exception:
            pass

        return [e for e in emails if self._is_valid_email(e, domain)]
    
    async def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        domain_stats = await self.domain_tracker.get_stats()
        google_stats = self.google_client.get_performance_stats()
        http_stats = await self.http_client.get_performance_stats()
        
        return {
            'orchestrator_config': {
                'max_concurrent_companies': self.max_concurrent_companies
            },
            'domain_tracking': domain_stats,
            'google_search_performance': google_stats,
            'http_client_performance': http_stats
        }


# Convenience function for batch processing
async def async_process_companies(
    companies: List[str],
    max_concurrent: int = 50
) -> Tuple[Counter, List[Dict[str, str]]]:
    """
    Convenience function for processing companies asynchronously.
    
    Usage:
        stats, results = await async_process_companies([
            'Company A', 'Company B'
        ])
    """
    async with AsyncOrchestrator(
        max_concurrent_companies=max_concurrent
    ) as orchestrator:
        return await orchestrator.process_companies_batch(companies)


if __name__ == "__main__":
    # Example usage and performance test
    async def test_performance():
        test_companies = [
            "Microsoft Corporation",
            "Google LLC",
            "Apple Inc",
            "Amazon.com Inc"
        ]
        
        print("Testing async orchestrator performance...")
        start = time.time()
        
        stats, results = await async_process_companies(
            test_companies,
            max_concurrent=10
        )
        
        elapsed = time.time() - start
        companies_per_sec = len(test_companies) / elapsed if elapsed > 0 else 0
        
        print(
            f"Processed {len(test_companies)} companies in {elapsed:.2f} "
            f"seconds"
        )
        print(f"Performance: {companies_per_sec:.2f} companies/sec")
        print(f"Results: {len(results)} emails found")
        print(f"Stats: {dict(stats)}")
    
    # Run the test
    asyncio.run(test_performance())
