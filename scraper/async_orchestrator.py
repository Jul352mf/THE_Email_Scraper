"""
Async Orchestrator for THE Email Scraper
TASK-045: Async I/O Migration - Orchestrator Component

This module provides asynchronous orchestration for processing companies with:
- Parallel company processing with asyncio.gather()
- Async Google search integration
- Concurrent domain probing and email extraction
- Non-blocking coordination with async locks
- Massive performance improvements through parallelization
"""

import asyncio
import logging
import time
from collections import Counter, deque
from typing import Tuple, List, Dict, Any

from scraper.config import config
from scraper.http_client import normalise_domain
from scraper.async_google_search import AsyncGoogleSearchClient
from scraper.async_http_client import AsyncHttpClient
from scraper.domain_scorer import domain_scorer
from scraper.email_extractor import email_extractor
from scraper.hybrid_email_extractor import hybrid_email_extractor
from scraper.smart_discovery import smart_discovery

log = logging.getLogger(__name__)


class AsyncDomainTracker:
    """
    Async-safe domain tracking for deduplication and memory management.
    """
    
    def __init__(self, max_domains: int = 10000):
        self.max_domains = max_domains
        self._seen_queue: deque = deque(maxlen=max_domains)
        self._seen: set = set()
        self._in_progress: set = set()
        self._lock = asyncio.Lock()
        self._cleanup_counter = 0
        
    async def is_domain_processed(self, domain: str) -> bool:
        """Check if domain is already processed or in progress."""
        async with self._lock:
            return domain in self._seen or domain in self._in_progress
    
    async def mark_domain_in_progress(self, domain: str) -> bool:
        """
        Mark domain as in progress.
        Returns True if successfully marked, False if already
        processed/in progress.
        """
        async with self._lock:
            if domain in self._seen or domain in self._in_progress:
                return False
            self._in_progress.add(domain)
            return True
    
    async def mark_domain_completed(self, domain: str) -> None:
        """Mark domain as completed and add to seen list."""
        async with self._lock:
            # Remove from in_progress
            self._in_progress.discard(domain)
            
            # Add to seen if not already there
            if domain not in self._seen:
                self._seen_queue.append(domain)
                self._seen.add(domain)
                
                # Memory management - if we exceed capacity, clean up
                if len(self._seen) > self.max_domains:
                    self._seen.clear()
                    self._seen.update(self._seen_queue)
                    log.debug(
                        "Domain seen cache cleanup: reset to %d domains",
                        len(self._seen)
                    )
                
                self._cleanup_counter += 1
                
                # Periodic cleanup of in_progress set
                if self._cleanup_counter % 100 == 0:
                    await self._cleanup_stale_in_progress()
    
    async def _cleanup_stale_in_progress(self) -> None:
        """Clean up stale in_progress entries."""
        # If in_progress grows too large, clear it to prevent memory leaks
        if len(self._in_progress) > 1000:
            old_count = len(self._in_progress)
            self._in_progress.clear()
            log.warning(
                "Cleared stale in_progress domains: %d entries were stuck",
                old_count
            )
    
    async def get_stats(self) -> Dict[str, int]:
        """Get domain tracking statistics."""
        async with self._lock:
            return {
                "seen_domains": len(self._seen),
                "in_progress_domains": len(self._in_progress),
                "max_domains_limit": self.max_domains,
                "cleanup_counter": self._cleanup_counter
            }


class AsyncOrchestrator:
    """
    Async orchestrator with massive performance improvements.
    
    Key performance improvements:
    - Parallel company processing (10-50x improvement)
    - Concurrent Google searches via AsyncGoogleSearchClient 
    - Parallel domain probing and email extraction
    - Non-blocking coordination with async locks
    - Batch processing throughout the pipeline
    """
    
    def __init__(self, max_concurrent_companies: int = 50):
        self.max_concurrent_companies = max_concurrent_companies
        self.domain_tracker = AsyncDomainTracker()
        
        # Async clients
        self.google_client = AsyncGoogleSearchClient(
            max_concurrent_requests=10
        )
        self.http_client = AsyncHttpClient(max_concurrent_requests=30)
        
        # Concurrency control
        self._company_semaphore = asyncio.Semaphore(max_concurrent_companies)
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.google_client.__aenter__()
        await self.http_client.__aenter__()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.google_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.http_client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def process_companies_batch(self, 
                                      companies: List[str]
                                      ) -> Tuple[Counter, List[Dict[str, str]]]:
        """
        Process multiple companies with full parallelization.
        
        This is the main performance improvement - instead of processing 
        companies sequentially, we process them all in parallel.
        
        Expected improvement: 10-50x for typical workloads
        Sequential: 100 companies * 30s per company = 3000s (50 minutes)
        Parallel:   100 companies in parallel = ~60-120s (1-2 minutes)
        """
        if not companies:
            return Counter(), []
        
        log.info(
            "Starting async batch processing for %d companies "
            "(max_concurrent=%d)",
            len(companies), self.max_concurrent_companies
        )
        start_time = time.time()
        
        # Create semaphore to limit concurrent company processing
        async def process_with_semaphore(company: str) -> Tuple[Counter, List[Dict[str, str]]]:
            """Wrapper to limit concurrency."""
            async with self._company_semaphore:
                return await self.process_company_async(company)
        
        # Process all companies in parallel
        try:
            results = await asyncio.gather(
                *[process_with_semaphore(company) for company in companies],
                return_exceptions=True
            )
            
            # Combine results and handle exceptions
            combined_stats = Counter()
            combined_rows = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    log.error(
                        "Company processing failed: %s - %s", 
                        companies[i], result
                    )
                    combined_stats["processing_errors"] += 1
                else:
                    stats, rows = result
                    combined_stats.update(stats)
                    combined_rows.extend(rows)
            
            elapsed = time.time() - start_time
            companies_per_sec = len(companies) / elapsed if elapsed > 0 else 0
            
            log.info(
                "Async batch processing completed: %d companies in %.2f seconds "
                "(%.2f companies/sec, %d emails found)",
                len(companies), elapsed, companies_per_sec, 
                len(combined_rows)
            )
            
            return combined_stats, combined_rows
            
        except Exception as e:
            log.error("Async batch processing failed: %s", e)
            return Counter({"batch_processing_errors": 1}), []
    
    async def process_company_async(self, company: str) -> Tuple[Counter, List[Dict[str, str]]]:
        """
        Process a single company with full async pipeline.
        """
        stats = Counter()
        rows = []
        stats["leads"] += 1
        
        log.debug("▶ Async processing company: %s", company)
        
        try:
            # Phase 1: Google search (async)
            search_results = await self.google_client.search_single(
                f'"{company}" contact email',
                num_results=10
            )
            
            if not search_results:
                stats["no_google"] += 1
                log.warning("No Google search results for: %s", company)
                return stats, rows
            
            # Phase 2: Domain scoring (sync - fast operation)
            try:
                score, link = domain_scorer.find_best_domain(company, search_results)
                
                if score < config.domain_score_threshold:
                    log.debug(
                        "Domain score too low (%d < %d): %s for company %s",
                        score, config.domain_score_threshold, link, company
                    )
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
    
    async def _process_domain_async(self, company: str, domain: str, score: int
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
                log.warning("Could not establish connection to domain: %s", domain)
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
            log.error("Domain processing error for %s (%s): %s", company, domain, e)
            stats["domain_processing_errors"] += 1
            return stats, rows
    
    async def _extract_emails_async(self, company: str, domain: str, base_url: str
                                   ) -> List[str]:
        """
        Extract emails using async HTTP operations.
        """
        emails = set()
        
        try:
            # Strategy 1: Direct page analysis (async)
            main_response = await self.http_client.safe_get(base_url, timeout=15)
            if main_response and main_response.status == 200:
                try:
                    content = await main_response.text()
                    page_emails = email_extractor.extract_emails_from_content(content)
                    emails.update(page_emails)
                    
                    if page_emails:
                        log.debug("Found %d emails from main page", len(page_emails))
                        
                except Exception as e:
                    log.debug("Main page analysis failed for %s: %s", domain, e)
            
            # Strategy 2: Smart discovery for common email pages (async)
            if len(emails) < 3:  # If we don't have enough emails yet
                # Generate common discovery URLs
                discovery_urls = self._generate_discovery_urls(base_url)
                
                # Process discovery URLs in parallel
                if discovery_urls:
                    discovery_results = await self.http_client.batch_get(
                        discovery_urls[:5],  # Limit to 5 for performance
                        timeout=10
                    )
                    
                    for url, response in discovery_results:
                        if response and response.status == 200:
                            try:
                                content = await response.text()
                                page_emails = email_extractor.extract_emails_from_content(content)
                                
                                # Use smart discovery to filter emails
                                if page_emails:
                                    domain = domain or base_url.split('//')[-1].split('/')[0]
                                    filtered_emails = smart_discovery.filter_relevant_emails(page_emails, domain)
                                    emails.update(filtered_emails)
                                    
                                    if filtered_emails:
                                        log.debug(
                                            "Found %d filtered emails from %s", 
                                            len(filtered_emails), url
                                        )
                                    
                            except Exception as e:
                                log.debug("Discovery page analysis failed for %s: %s", url, e)
            
            # Strategy 3: Hybrid extraction if still no results
            if not emails:
                try:
                    # Use existing hybrid extractor (sync operations)
                    hybrid_emails = hybrid_email_extractor.extract_emails_hybrid(
                        company, domain, base_url
                    )
                    emails.update(hybrid_emails)
                    
                    if hybrid_emails:
                        log.debug(
                            "Found %d emails via hybrid extraction", 
                            len(hybrid_emails)
                        )
                        
                except Exception as e:
                    log.debug("Hybrid extraction failed for %s: %s", domain, e)
            
            # Filter and validate emails
            valid_emails = []
            for email in emails:
                if self._is_valid_email(email, domain):
                    valid_emails.append(email)
            
            return valid_emails
            
        except Exception as e:
            log.error("Email extraction failed for %s: %s", domain, e)
            return []
    
    def _is_valid_email(self, email: str, domain: str) -> bool:
        """
        Validate email address quality and relevance.
        """
        email_lower = email.lower()
        
        # Skip common false positives
        skip_patterns = [
            'example@', '@example', 'test@', '@test',
            'noreply@', 'no-reply@', 'donotreply@',
            'support@', 'info@', 'sales@', 'admin@'
        ]
        
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
