"""
Enhanced crawler module with improved thread safety and error handling.

This module provides robust website crawling functionality with proper thread safety,
error handling, and security features.
"""

import logging
import threading
import time
from collections import deque, defaultdict
from typing import Set, Dict, Optional, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper.config import config
from scraper.http import http_client, normalise_domain, validate_url
from scraper.email_extractor import email_extractor

# Initialize logger
log = logging.getLogger(__name__)

# Global page count and lock
global_page_count = defaultdict(int)
global_page_lock = threading.Lock()

class CrawlerError(Exception):
    """Exception raised for crawler errors."""
    pass

class Crawler:
    """Enhanced website crawler with improved thread safety and error handling."""
    
    def __init__(self):
        """Initialize the crawler with thread-safe counters."""
        # Thread-local storage for tracking crawl state
        self._thread_local = threading.local()
        
        # Lock for synchronizing access to shared state
        self._lock = threading.Lock()
        
        # Domain-specific crawl limits
        self._domain_limits: Dict[str, int] = {}
        
        self._thread_local.emails = set()
    
    def _init_thread_local(self) -> None:
        """Initialize thread-local storage for the current thread."""
        if not hasattr(self._thread_local, 'initialized'):
            self._thread_local.initialized = True
            self._thread_local.seen_urls = set()
    
    def set_domain_limit(self, domain: str, limit: int) -> None:
        """
        Set a custom page limit for a specific domain.
        
        Args:
            domain: Domain to set limit for
            limit: Maximum number of pages to crawl
        """
        with self._lock:
            self._domain_limits[domain] = limit
    
    def get_domain_limit(self, domain: str) -> int:
        """
        Get the page limit for a specific domain.
        
        Args:
            domain: Domain to get limit for
            
        Returns:
            Maximum number of pages to crawl
        """
        with self._lock:
            return self._domain_limits.get(domain, config.max_fallback_pages)
    
    def reset_counters(self) -> None:
        """Reset all page counters."""
        with global_page_lock:
            global_page_count.clear()
    
    def crawl_small(self, domain: str, limit: Optional[int] = None, 
                   max_time: Optional[int] = None, seed_response: Optional[Any] = None) -> Set[str]:
        """
        Crawl small sites with enhanced thread safety and error handling.
        
        Args:
            domain: Domain to crawl
            limit: Maximum number of pages to crawl (defaults to config value)
            max_time: Maximum crawl time in seconds (defaults to calculated value)
            
        Returns:
            Set of email addresses found
            
        Raises:
            CrawlerError: If crawling fails
        """
        # Initialize thread-local storage
        self._init_thread_local()
        
        # Use domain-specific limit if not specified
        if limit is None:
            limit = self.get_domain_limit(domain)
            
        # Calculate max time if not specified
        if max_time is None:
            avg_page_time = 2.0  # Average seconds per page
            max_time = min(60, limit * avg_page_time)  # Cap at 60 seconds
        
        start = time.time()
        log.info("Starting crawl of %s (limit: %d pages, timeout: %d seconds)", 
                domain, limit, max_time)
        
        # Initialize crawl state
        q = deque([f"https://{domain}"])
        found_emails = set()
        
        q: Deque[str] = deque()

        # if we already fetched the main page, parse it now
        if seed_response is not None:
            self._process_response(seed_response, q)
        else:
            q.append(f"https://{domain}")
                     
            
        try:
            while q:
                # Check timeout
                elapsed = time.time() - start
                if elapsed > max_time and len(self._thread_local.seen_urls) >= limit // 2:
                    log.warning("Timeout on %s after %.1fs", domain, elapsed)
                    break
                    
                # Get next URL to process
                url = q.popleft()
                
                # Skip if already seen
                if url in self._thread_local.seen_urls:
                    continue
                
                # Check page quota with proper locking
                with global_page_lock:
                    current_count = global_page_count[domain]
                    if current_count >= limit:
                        log.debug("Reached page limit for %s: %d", domain, limit)
                        break
                    global_page_count[domain] += 1
                    new_count = global_page_count[domain]
                
                # Double-check after increment
                if new_count > limit:
                    break
                    
                # Mark URL as seen
                self._thread_local.seen_urls.add(url)
                
                # Get page content with retry
                log.debug("Crawling %s (%d/%d)", url, new_count, limit)
                response = http_client.safe_get(url, retry_count=2)
                if not response:
                    continue
                    
                # Extract emails from the page
                try:
                    page_emails = email_extractor.extract_from_html(response.text, url)
                    found_emails.update(page_emails)
                except Exception as e:
                    log.warning("Error extracting emails from %s: %s", url, e)
                
                # Parse links from the response
                try:
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    # Process all links
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        
                        # Check for mailto links
                        if href.lower().startswith("mailto:"):
                            try:
                                email = email_extractor.clean_email(href.split(":", 1)[1])
                                found_emails.add(email)
                            except Exception as e:
                                log.debug("Failed to clean mailto email %s: %s", href, e)
                                
                        # Check for internal links
                        elif href.startswith("/") or domain in href:
                            # Build full URL
                            full_url = urljoin(url, href)
                            
                            # Validate URL
                            if not validate_url(full_url):
                                continue
                                
                            # Only add HTTP/HTTPS URLs
                            if full_url.startswith(("http://", "https://")):
                                # Only add URLs for the same domain
                                if domain in normalise_domain(full_url):
                                    q.append(full_url)
                                    
                except Exception as e:
                    log.warning("Error parsing links from %s: %s", url, e)
                    
        except Exception as e:
            log.error("Error crawling %s: %s", domain, e)
            raise CrawlerError(f"Error crawling {domain}: {e}")
            
        finally:
            # Log crawl statistics
            elapsed = time.time() - start
            pages_crawled = len(self._thread_local.seen_urls)
            emails_found = len(found_emails)
            
            log.info("Crawl of %s completed: %d pages, %d emails, %.1f seconds", 
                    domain, pages_crawled, emails_found, elapsed)
            
            # Clear thread-local seen URLs to prevent memory leaks
            self._thread_local.seen_urls.clear()
        
        return found_emails
    
    def _process_response(self, resp, q: Deque[str]) -> None:
        """Extract emails + internal links from an already-fetched response."""
        url = resp.url
        html = resp.text
        # extract
        try:
            hits = email_extractor.extract_from_html(html, url)
            self._thread_local.emails.update(hits)
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.lower().startswith("mailto:"):
                    continue
                full = urljoin(url, href)
                if validate_url(full) and domain in normalise_domain(full):
                    q.append(full)
        except Exception as e:
            log.warning("Parse error on %s: %s", url, e)

# Create a global crawler instance
crawler = Crawler()
