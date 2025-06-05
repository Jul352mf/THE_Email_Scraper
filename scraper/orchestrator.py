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

from scraper.config import config
from scraper.http import normalise_domain, http_client
from scraper.google_search import google_client, GoogleApiError, RateLimitExceededError
from scraper.domain_scorer import domain_scorer
from scraper.email_extractor import email_extractor
from scraper.hybrid_email_extractor import hybrid_email_extractor
from scraper.sitemap import sitemap_parser
from scraper.crawler import crawler

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
    
    def reset_stats(self) -> None:
        """Reset global statistics."""
        self.global_stats.clear()
        crawler.reset_counters()
    
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
                main_url = f"https://{domain}"
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
