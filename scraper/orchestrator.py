"""
Enhanced orchestrator module with improved error handling and coordination.

This module provides robust orchestration logic for processing companies and finding emails
with proper error handling, logging, and coordination features.
"""

import logging
import time
from collections import Counter, deque
from typing import Tuple, List, Dict, Any, Optional
import threading
from concurrent.futures import as_completed
import queue

from scraper.config import config
from scraper.http_client import normalise_domain, http_client
from scraper.google_search import google_client, GoogleApiError, RateLimitExceededError
from scraper.domain_scorer import domain_scorer
from scraper.email_extractor import email_extractor
from scraper.hybrid_email_extractor import hybrid_email_extractor
from scraper.sitemap import sitemap_parser
from scraper.crawler import crawler
from scraper.smart_discovery import smart_discovery
from scraper.performance_optimizer import resource_monitor
from scraper.thread_pool_manager import (
    get_global_pool_manager, TaskType, execute_batch, submit_task
)
from scraper.progress_tracker import (
    update_progress_company_started, update_progress_stats
)

# Initialize logger
log = logging.getLogger(__name__)

_global_lock = threading.Lock()
_global_in_progress: set[str] = set()

# LRU-style domain tracking with size limits
_MAX_SEEN_DOMAINS = 10000  # Configurable limit for memory management
_global_seen_queue: deque = deque(maxlen=_MAX_SEEN_DOMAINS)  # FIFO queue for domain order
_global_seen: set[str] = set()  # Fast lookup

# Memory cleanup tracking
_cleanup_counter = 0
_CLEANUP_INTERVAL = 100  # Cleanup every N domains processed

def _add_domain_to_seen(domain: str) -> None:
    """
    Add a domain to the seen list with memory management.
    
    Args:
        domain: Domain to mark as seen
    """
    global _cleanup_counter
    
    with _global_lock:
        if domain not in _global_seen:
            # Add to both queue and set
            _global_seen_queue.append(domain)
            _global_seen.add(domain)
            
            # If queue reached max capacity, it automatically evicted the oldest
            # We need to remove that from our set too
            if len(_global_seen) > _MAX_SEEN_DOMAINS:
                # Clean up set to match queue
                _global_seen.clear()
                _global_seen.update(_global_seen_queue)
                log.debug("Domain seen cache cleanup: reset to %d domains", len(_global_seen))
            
            _cleanup_counter += 1
            
            # Periodic cleanup of in_progress set (remove stale entries)
            if _cleanup_counter % _CLEANUP_INTERVAL == 0:
                _cleanup_stale_in_progress()

def _cleanup_stale_in_progress() -> None:
    """Clean up stale in_progress entries (no lock needed, called from locked context)."""
    # Note: This is a simple cleanup. In production, you might want to track timestamps
    # For now, we'll just limit the size
    if len(_global_in_progress) > 1000:  # Reasonable limit for concurrent processing
        # Clear everything - this might cause some duplicate work but prevents memory leaks
        old_count = len(_global_in_progress)
        _global_in_progress.clear()
        log.warning("Cleared stale in_progress domains: %d entries were stuck", old_count)

def get_domain_tracking_stats() -> Dict[str, int]:
    """Get statistics about domain tracking for monitoring."""
    with _global_lock:
        return {
            "seen_domains": len(_global_seen),
            "in_progress_domains": len(_global_in_progress),
            "max_seen_limit": _MAX_SEEN_DOMAINS,
            "cleanup_counter": _cleanup_counter
        }

class OrchestratorError(Exception):
    """Exception raised for orchestration errors."""
    pass

class Orchestrator:
    """Enhanced orchestrator with improved error handling and coordination."""
    
    def __init__(self):
        """Initialize the orchestrator."""
        self.global_stats = Counter()
        self.save_domain_only = False  # Whether to save domain even if no emails found
        self.hybrid_extractor = hybrid_email_extractor
        self._domain_batch_queue = queue.Queue()
        self._domain_processing_pool = None
    
    def reset_stats(self) -> None:
        """Reset global statistics."""
        self.global_stats.clear()
        crawler.reset_counters()
        # Clear global domain tracking
        with _global_lock:
            _global_seen.clear()
            _global_seen_queue.clear()
            _global_in_progress.clear()
        global _cleanup_counter
        _cleanup_counter = 0
        log.info("Reset orchestrator stats and cleared domain tracking cache")
    
    def batch_probe_domains(self, domains: List[str]) -> None:
        """Batch probe multiple domains for optimized access patterns using optimized thread pool."""
        if not domains:
            return
            
        log.info("Batch probing %d domains for access patterns", len(domains))
        
        def probe_single_domain(domain: str) -> Optional[str]:
            try:
                http_client.probe_domain_access(domain)
                log.debug("Probed domain: %s", domain)
                return domain
            except Exception as e:
                log.warning("Failed to probe domain %s: %s", domain, e)
                return None
        
        # Use optimized thread pool for domain probing
        # Prepare arguments for batch execution
        probe_args = [(domain,) for domain in domains]
        
        # Calculate max concurrent based on available workers
        pool_manager = get_global_pool_manager()
        load_info = pool_manager.get_load_info()
        max_concurrent = min(len(domains), load_info['available_workers'] or 2)
        
        try:
            # Execute batch with optimized concurrency
            results = execute_batch(
                TaskType.DOMAIN_PROBE,
                probe_single_domain,
                probe_args,
                max_concurrent=max_concurrent,
                timeout=60.0  # 1 minute timeout for all probes
            )
            
            successful_probes = [r for r in results if r is not None]
            log.info("Successfully probed %d/%d domains", len(successful_probes), len(domains))
            
        except Exception as e:
            log.error("Batch domain probing failed: %s", e)
    
    def process_companies_concurrent(self, companies: List[str]) -> Tuple[Counter, List[Dict[str, str]]]:
        """Process multiple companies with concurrent domain processing and batch probing."""
        start_time = time.time()
        all_stats = Counter()
        all_rows: List[Dict[str, str]] = []
        
        # Phase 1: Collect all domains that need processing
        log.info("Phase 1: Collecting domains for %d companies", len(companies))
        domains_to_probe = []
        company_domain_map = {}  # company -> (domain, score, search_results)
        
        for company in companies:
            all_stats["leads"] += 1
            try:
                # Search for company
                try:
                    search_results = google_client.search_with_fallback(company)
                    if not search_results:
                        all_stats["no_google"] += 1
                        log.warning("No Google search results for: %s", company)
                        continue
                except (GoogleApiError, RateLimitExceededError) as e:
                    all_stats["google_error"] += 1
                    update_progress_stats(google_error=1)
                    log.error("Google search error for %s: %s", company, e)
                    continue
                
                # Find best matching domain
                try:
                    score, link = domain_scorer.find_best_domain(company, search_results)
                    
                    # Check if domain score meets threshold
                    if score < config.domain_score_threshold:
                        log.info("Domain score too low (%d < %d): %s for company %s", 
                                score, config.domain_score_threshold, link, company)
                        all_stats["domain_unclear"] += 1
                        continue
                except Exception as e:
                    all_stats["domain_error"] += 1
                    update_progress_stats(processing_error=1)
                    log.error("Domain scoring error for %s: %s", company, e)
                    continue
                
                # Extract and normalize domain
                domain = normalise_domain(link)
                
                # Check if domain already processed or in progress
                with _global_lock:
                    if domain not in _global_seen and domain not in _global_in_progress:
                        company_domain_map[company] = (domain, score, search_results)
                        if domain not in domains_to_probe:  # Avoid duplicates
                            domains_to_probe.append(domain)
                        _global_in_progress.add(domain)
                    else:
                        log.info("↩ Skipping %s: domain %s already processed", company, domain)
                        all_stats["skipped_domain"] += 1
                        
            except Exception as e:
                log.error("Error in domain collection for %s: %s", company, e)
                all_stats["domain_error"] += 1
                update_progress_stats(processing_error=1)
        
        # Phase 2: Batch probe all collected domains
        if domains_to_probe:
            log.info("Phase 2: Batch probing %d unique domains", len(domains_to_probe))
            self.batch_probe_domains(domains_to_probe)
        
        # Phase 3: Process domains concurrently
        log.info("Phase 3: Processing %d companies with valid domains", len(company_domain_map))
        
        def process_single_company_domain(company_data: Tuple[str, Tuple[str, int, List]]) -> Tuple[Counter, List[Dict[str, str]]]:
            company, (domain, score, search_results) = company_data
            return self._process_company_with_domain(company, domain, score)
        
        processed_domains = set()
        
        if not company_domain_map:
            log.info("No companies with domains to process")
            return all_stats, all_rows
        
        try:
            # Use optimized thread pool for concurrent company processing
            def process_company_wrapper(company_data: Tuple[str, Tuple[str, int, List]]) -> Tuple[str, Tuple[Counter, List[Dict[str, str]]]]:
                try:
                    company, domain_info = company_data
                    domain, score, search_results = domain_info  # Explicitly unpack the tuple
                    stats, rows = self._process_company_with_domain(company, domain, score)  # Only pass what's needed
                    return company, (stats, rows)
                except Exception as e:
                    # Ensure we always return the expected tuple structure even on error
                    company = company_data[0] if isinstance(company_data, tuple) and len(company_data) > 0 else "unknown"
                    log.error("Error in process_company_wrapper for %s: %s", company, e)
                    error_stats = Counter()
                    error_stats["processing_error"] += 1
                    return company, (error_stats, [])
            
            # Prepare arguments for batch processing
            company_args = [(item,) for item in company_domain_map.items()]
            
            # Calculate optimal concurrency based on current load
            pool_manager = get_global_pool_manager()
            load_info = pool_manager.get_load_info()
            max_concurrent = min(len(company_domain_map), load_info['available_workers'] or config.max_workers)
            
            log.info("Processing %d companies with %d concurrent workers", 
                     len(company_domain_map), max_concurrent)
            
            # Execute batch processing with timeout
            try:
                # Use individual task submission for better error handling and progress tracking
                futures = []
                for company_data in company_domain_map.items():
                    future = submit_task(TaskType.COMPANY_PROCESSING, process_company_wrapper, company_data)
                    futures.append((future, company_data[0]))  # Store company name with future
                
                # Process results as they complete
                for future, company in futures:
                    try:
                        # Update progress that company processing started
                        update_progress_company_started(company)
                        
                        company_result, (stats, rows) = future.result(timeout=300)  # 5 minute timeout per company
                        all_stats.update(stats)
                        all_rows.extend(rows)
                        
                        # Update progress stats
                        update_progress_stats(
                            with_email=stats.get('with_email', 0),
                            without_email=stats.get('without_email', 0),
                            processing_error=stats.get('processing_error', 0),
                            domain=stats.get('domain', 0),
                            sitemap=stats.get('sitemap', 0)
                        )
                        
                        # Track processed domain
                        if company in company_domain_map:
                            processed_domains.add(company_domain_map[company][0])
                        log.debug("Successfully processed company: %s", company)
                        
                    except Exception as e:
                        log.error("Error processing company %s: %s", company, e)
                        all_stats["processing_error"] += 1
                        
                        # Update progress with error
                        update_progress_stats(processing_error=1)
                        
                        # Still mark domain as processed to avoid retry
                        if company in company_domain_map:
                            processed_domains.add(company_domain_map[company][0])
                            
            except KeyboardInterrupt:
                log.warning("Concurrent processing interrupted by user")
                # Cancel remaining futures
                for future, _ in futures:
                    future.cancel()
                raise
                    
        finally:
            # Mark all processed and attempted domains as seen
            for domain in domains_to_probe:
                with _global_lock:
                    _global_in_progress.discard(domain)
                _add_domain_to_seen(domain)
        
        elapsed = time.time() - start_time
        log.info("Concurrent processing completed in %.2f seconds", elapsed)
        
        return all_stats, all_rows
    
    def _process_company_with_domain(self, company: str, domain: str, score: int) -> Tuple[Counter, List[Dict[str, str]]]:
        """Process a single company with a known domain (used in concurrent processing)."""
        start_time = time.time()
        stats = Counter()
        rows: List[Dict[str, str]] = []
        
        stats["domain"] += 1
        log.info("✓ Processing domain: %s (score: %d) for company: %s", domain, score, company)
        
        try:
            # Always save domain even if no emails found
            domain_row = {"Company": company, "Domain": domain}
            
            # Initialize email set and processing counters
            emails = set()
            pages_processed = 0
            
            # Fetch & cache the home page once, then extract from its HTML
            # Use optimized URL if available
            main_url = http_client.get_optimized_url(domain) or f"https://{domain}"
            try:
                main_resp = http_client.safe_get(main_url, retry_count=2)
                if main_resp:
                    pages_processed += 1
                    try:
                        home_hits = self.hybrid_extractor.extract_from_response(main_resp)
                    except AttributeError:
                        # Fallback if extractor doesn't support response input
                        home_hits = self.hybrid_extractor.extract_from_url(main_url)
                    
                    # Filter and add relevant emails
                    if config.enable_smart_discovery:
                        home_hits = smart_discovery.filter_relevant_emails(home_hits, domain)
                    
                    emails.update(home_hits)
                    log.debug("Found %d emails on main page for %s", len(home_hits), domain)
                    
                    # Check for early stopping after homepage
                    if smart_discovery.should_stop_early(emails, pages_processed):
                        log.info("Early stopping after homepage for %s: found %d emails", domain, len(emails))
                        stats["early_stop_homepage"] += 1
                        if emails:
                            stats["with_email"] += 1
                            rows = [{"Company": company, "Domain": domain, "Email": e} for e in emails]
                            log.info("✓ Found %d emails for %s (early stop)", len(emails), company)
                        else:
                            stats["without_email"] += 1
                            log.info("✗ No emails found for %s (early stop)", company)
                            if self.save_domain_only:
                                rows = [domain_row]
                        return stats, rows
                        
            except Exception as e:
                log.warning("Error fetching or parsing main page %s: %s", main_url, e)
            
            # Check sitemap for priority pages
            sitemap_used = False
            try:
                priority_urls, used_sitemap = sitemap_parser.get_priority_urls(domain)
                sitemap_used = used_sitemap
                
                if priority_urls:
                    log.debug("Found %d priority URLs in sitemap", len(priority_urls))
                    
                    # Process each priority URL with smart discovery
                    for url in set(priority_urls):
                        try:
                            pages_processed += 1
                            url_emails = self.hybrid_extractor.extract_from_url(url)
                            
                            # Filter relevant emails if smart discovery enabled
                            if config.enable_smart_discovery:
                                url_emails = smart_discovery.filter_relevant_emails(url_emails, domain)
                            
                            emails.update(url_emails)
                            if url_emails:
                                log.debug("Found %d emails on %s", len(url_emails), url)
                            
                            # Check for early stopping after each priority page
                            if smart_discovery.should_stop_early(emails, pages_processed):
                                log.info("Early stopping after priority pages for %s: found %d emails", domain, len(emails))
                                stats["early_stop_priority"] += 1
                                break
                                
                        except Exception as e:
                            log.warning("Error extracting emails from %s: %s", url, e)
                            
                if used_sitemap:
                    stats["sitemap"] += 1
                    log.info("Used sitemap for %s", domain)
            except Exception as e:
                log.warning("Error processing sitemap for %s: %s", domain, e)
            
            # If no emails found and early stopping not triggered, try crawling
            if not emails and not smart_discovery.should_stop_early(emails, pages_processed):
                log.info("No emails found in sitemap, attempting fallback crawl: %s", domain)
                try:
                    crawl_emails = crawler.crawl_small(domain, seed_response=main_resp)
                    
                    # Filter relevant emails if smart discovery enabled
                    if config.enable_smart_discovery:
                        crawl_emails = smart_discovery.filter_relevant_emails(crawl_emails, domain)
                    
                    emails.update(crawl_emails)
                    log.debug("Found %d emails from crawling", len(crawl_emails))
                    stats["crawl_used"] += 1
                except Exception as e:
                    log.warning("Error during crawling of %s: %s", domain, e)
            elif emails:
                log.info("Skipping crawl for %s: %d emails already found", domain, len(emails))
            
            # Create result rows
            if emails:
                stats["with_email"] += 1
                rows = [{"Company": company, "Domain": domain, "Email": e} for e in emails]
                log.info("✓ Found %d emails for %s", len(emails), company)
            else:
                stats["without_email"] += 1
                log.info("✗ No emails found for %s", company)
                
                # Include domain even when no emails found if configured
                if self.save_domain_only:
                    rows = [domain_row]
            
            # Log processing time
            elapsed = time.time() - start_time
            log.debug("Processed %s in %.2f seconds", company, elapsed)
            
            return stats, rows
            
        except Exception as e:
            log.error("Unexpected error processing domain %s for company %s: %s", domain, company, e)
            stats["processing_error"] += 1
            return stats, rows
    
    def process_company(self, company: str) -> Tuple[Counter, List[Dict[str, str]]]:
        """
        Process a single company with enhanced error handling and logging.
        
        Args:
            company: Company name to process
            
        Returns:
            Tuple of (stats Counter, rows list)
            
        Raises:
            OrchestratorError: If processing fails
        """
        crawler.reset_counters()
        start_time = time.time()
        stats = Counter()
        rows: List[Dict[str, str]] = []
        stats["leads"] += 1
        log.info("▶ Processing company: %s", company)

        try:
            # Search for company
            try:
                search_results = google_client.search_with_fallback(company)
                if not search_results:
                    stats["no_google"] += 1
                    log.warning("No Google search results for: %s", company)
                    return stats, rows
            except (GoogleApiError, RateLimitExceededError) as e:
                stats["google_error"] += 1
                log.error("Google search error for %s: %s", company, e)
                return stats, rows

            # Find best matching domain
            try:
                score, link = domain_scorer.find_best_domain(company, search_results)
                
                # Check if domain score meets threshold
                if score < config.domain_score_threshold:
                    log.info("Domain score too low (%d < %d): %s for company %s", 
                            score, config.domain_score_threshold, link, company)
                    stats["domain_unclear"] += 1
                    return stats, rows
            except Exception as e:
                stats["domain_error"] += 1
                log.error("Domain scoring error for %s: %s", company, e)
                return stats, rows

            # Extract and normalize domain
            domain = normalise_domain(link)
            
            # Probe domain access patterns for optimization
            log.info("Probing access patterns for: %s", domain)
            http_client.probe_domain_access(domain)
            
            # ─── DOMAIN START: skip if already done ───
            with _global_lock:
                if domain in _global_seen or domain in _global_in_progress:
                    log.info("↩ Skipping %s: domain already processed", domain)
                    stats["skipped_domain"] += 1
                    return stats, rows
                # mark “in progress” so other threads don’t start it
                _global_in_progress.add(domain)

            stats["domain"] += 1
            log.info("✓ Found domain: %s (score: %d)", domain, score)
            
            try:

                # Always save domain even if no emails found
                domain_row = {"Company": company, "Domain": domain}
                
                # Initialize email set
                emails = set()
                
                # Fetch & cache the home page once, then extract from its HTML
                # Use optimized URL if available
                main_url = http_client.get_optimized_url(domain) or f"https://{domain}"
                try:
                    main_resp = http_client.safe_get(main_url, retry_count=2)
                    if main_resp:
                        try:
                            home_hits = self.hybrid_extractor.extract_from_response(main_resp)
                        except AttributeError:
                            # Fallback if extractor doesn’t support response input
                            home_hits = self.hybrid_extractor.extract_from_url(main_url)
                        emails.update(home_hits)
                        log.debug("Found %d emails on main page", len(home_hits))
                except Exception as e:
                    log.warning("Error fetching or parsing main page %s: %s", main_url, e)                        
                
                # Check sitemap for priority pages
                sitemap_used = False
                try:
                    priority_urls, used_sitemap = sitemap_parser.get_priority_urls(domain)
                    sitemap_used = used_sitemap
                    
                    if priority_urls:
                        log.debug("Found %d priority URLs in sitemap", len(priority_urls))
                        
                        # Process each priority URL
                        for url in set(priority_urls):
                            try:
                                url_emails = self.hybrid_extractor.extract_from_url(url)
                                emails.update(url_emails)
                                if url_emails:
                                    log.debug("Found %d emails on %s", len(url_emails), url)
                            except Exception as e:
                                log.warning("Error extracting emails from %s: %s", url, e)
                                
                    if used_sitemap:
                        stats["sitemap"] += 1
                        log.info("Used sitemap for %s", domain)
                except Exception as e:
                    log.warning("Error processing sitemap for %s: %s", domain, e)

                # If no emails found, try crawling
                if not emails:
                    log.info("No emails found in sitemap, attempting fallback crawl: %s", domain)
                    try:
                        crawl_emails = crawler.crawl_small(domain, seed_response=main_resp)
                        emails.update(crawl_emails)
                        log.debug("Found %d emails from crawling", len(crawl_emails))
                    except Exception as e:
                        log.warning("Error during crawling of %s: %s", domain, e)

                # Create result rows
                if emails:
                    stats["with_email"] += 1
                    rows = [{"Company": company, "Domain": domain, "Email": e} for e in emails]
                    log.info("✓ Found %d emails for %s", len(emails), company)
                else:
                    stats["without_email"] += 1
                    log.info("✗ No emails found for %s", company)
                    
                    # Include domain even when no emails found if configured
                    if self.save_domain_only:
                        rows = [domain_row]

                # Log processing time
                elapsed = time.time() - start_time
                log.debug("Processed %s in %.2f seconds", company, elapsed)
                
                return stats, rows
        
            finally:
                # ─── DOMAIN DONE: remove "in progress", mark as seen ───
                with _global_lock:
                    _global_in_progress.discard(domain)
                _add_domain_to_seen(domain)        
            
        except Exception as e:
            log.error("Unexpected error processing company %s: %s", company, e)
            stats["processing_error"] += 1
            return stats, rows
    
    def set_options(self, save_domain_only: bool = False) -> None:
        """
        Set processing options.
        
        Args:
            save_domain_only: Whether to save domain even if no emails found
        """
        self.save_domain_only = save_domain_only
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive memory usage statistics.
        
        Returns:
            Dictionary containing memory usage information
        """
        stats = get_domain_tracking_stats()
        
        # Add additional memory-related stats
        stats.update({
            "orchestrator_stats_size": len(self.global_stats),
            "domain_batch_queue_size": self._domain_batch_queue.qsize() if hasattr(self._domain_batch_queue, 'qsize') else 0,
        })
        
        # Estimate memory usage (rough approximation)
        domain_tracking_memory = (stats["seen_domains"] * 50) + (stats["in_progress_domains"] * 50)  # ~50 bytes per domain string
        stats["estimated_domain_tracking_memory_bytes"] = domain_tracking_memory
        
        # Add thread pool performance stats
        try:
            from scraper.thread_pool_manager import get_pool_stats
            stats['thread_pool'] = get_pool_stats()
        except Exception as e:
            log.debug("Failed to get thread pool stats: %s", e)
        
        return stats

# Create a global orchestrator instance
orchestrator = Orchestrator()
