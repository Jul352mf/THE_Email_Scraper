"""
Enhanced orchestrator module with improved error handling and coordination.

This module provides robust orchestration logic for processing companies and finding emails
with proper error handling, logging, and coordination features.
"""

import logging
import time
from collections import Counter
from typing import Tuple, List, Dict, Any, Optional
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Initialize logger
log = logging.getLogger(__name__)

_global_lock = threading.Lock()
_global_in_progress: set[str] = set()
_global_seen:       set[str] = set()

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
            _global_in_progress.clear()
    
    def batch_probe_domains(self, domains: List[str]) -> None:
        """Batch probe multiple domains for optimized access patterns."""
        if not domains:
            return
            
        log.info("Batch probing %d domains for access patterns", len(domains))
        
        def probe_single_domain(domain: str) -> None:
            try:
                http_client.probe_domain_access(domain)
                log.debug("Probed domain: %s", domain)
            except Exception as e:
                log.warning("Failed to probe domain %s: %s", domain, e)
        
        # Use smaller thread pool for domain probing to avoid overwhelming servers
        max_probe_workers = min(len(domains), config.max_workers // 2, 4)
        with ThreadPoolExecutor(max_workers=max_probe_workers) as executor:
            future_to_domain = {executor.submit(probe_single_domain, domain): domain for domain in domains}
            
            for future in as_completed(future_to_domain):
                domain = future_to_domain[future]
                try:
                    future.result()
                except Exception as e:
                    log.warning("Domain probing failed for %s: %s", domain, e)
    
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
        
        try:
            # Use ThreadPoolExecutor for concurrent domain processing
            max_domain_workers = min(len(company_domain_map), config.max_workers)
            if max_domain_workers == 0:
                log.info("No companies with domains to process")
                return all_stats, all_rows
                
            with ThreadPoolExecutor(max_workers=max_domain_workers) as executor:
                future_to_company = {
                    executor.submit(process_single_company_domain, item): item[0] 
                    for item in company_domain_map.items()
                }
                
                try:
                    for future in as_completed(future_to_company):
                        company = future_to_company[future]
                        try:
                            stats, rows = future.result()
                            all_stats.update(stats)
                            all_rows.extend(rows)
                            # Track which domain was processed for this company
                            if company in company_domain_map:
                                processed_domains.add(company_domain_map[company][0])
                        except Exception as e:
                            log.error("Error processing company %s: %s", company, e)
                            all_stats["processing_error"] += 1
                            # Still mark domain as processed to avoid retry
                            if company in company_domain_map:
                                processed_domains.add(company_domain_map[company][0])
                                
                except KeyboardInterrupt:
                    log.warning("Concurrent processing interrupted by user")
                    executor.shutdown(wait=False)
                    raise
                    
        finally:
            # Mark all processed and attempted domains as seen
            with _global_lock:
                for domain in domains_to_probe:
                    _global_in_progress.discard(domain)
                    _global_seen.add(domain)
        
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
                # ─── DOMAIN DONE: remove “in progress”, mark as seen ───
                with _global_lock:
                    _global_in_progress.discard(domain)
                    _global_seen.add(domain)        
            
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

# Create a global orchestrator instance
orchestrator = Orchestrator()
