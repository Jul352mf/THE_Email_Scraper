import logging
import threading
import time
from collections import deque, defaultdict
from typing import Set, Dict, Optional, Deque, Any
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup

from scraper.config import config
from scraper.http import http_client, normalise_domain, validate_url
from scraper.hybrid_email_extractor import hybrid_email_extractor

# Initialize logger
log = logging.getLogger(__name__)

# Global page count and lock
go_global_page_count = defaultdict(int)
_global_page_lock = threading.Lock()


class CrawlerError(Exception):
    """Exception raised for crawler errors."""
    pass


class Crawler:
    """Enhanced website crawler with improved thread safety, URL normalization,
       and multi-threaded fetching."""

    def __init__(self):
        """Initialize crawler with thread-safe state."""
        self._lock = threading.Lock()
        self._seen_urls: Set[str] = set()
        self._domain_limits: Dict[str, int] = {}
        self.email_extractor = hybrid_email_extractor        

    @staticmethod
    def _canonicalize_url(url: str) -> str:
        """
        Normalize a URL by removing fragments, filtering out tracking parameters,
        and sorting query parameters for consistent comparison.
        """
        parsed = urlparse(url)
        scheme, netloc, path, params, query, _ = parsed
        pairs = parse_qsl(query, keep_blank_values=True)
        filtered = [(k, v) for k, v in pairs if not k.startswith("utm_")]
        filtered.sort()
        normalized_query = urlencode(filtered)
        return urlunparse((scheme, netloc, path, params, normalized_query, ""))

    def set_domain_limit(self, domain: str, limit: int) -> None:
        """Set a custom page limit for a specific domain."""
        with self._lock:
            self._domain_limits[domain] = limit

    def get_domain_limit(self, domain: str) -> int:
        """Get the page limit for a specific domain, falling back to config."""
        with self._lock:
            return self._domain_limits.get(domain, config.max_fallback_pages)

    def reset_counters(self) -> None:
        """Reset all page counters."""
        with _global_page_lock:
            go_global_page_count.clear()

    def crawl_small(
        self,
        domain: str,
        limit: Optional[int] = None,
        max_time: Optional[int] = None,
        seed_response: Optional[Any] = None,
        num_workers: Optional[int] = None
    ) -> Set[str]:
        """
        Crawl small sites using multiple threads with enhanced URL deduplication.
        :param domain: domain to crawl
        :param limit: max pages per domain
        :param max_time: timeout in seconds
        :param seed_response: optional pre-fetched homepage response
        :param num_workers: number of concurrent worker threads
        """
        # Initialize and clear state
        with self._lock:
            self._seen_urls.clear()

        limit = limit or self.get_domain_limit(domain)
        max_time = max_time if max_time is not None else min(60, limit * 2)
        num_workers = num_workers or getattr(config, 'default_workers', 4)

        start_time = time.time()
        log.info("Starting crawl of %s (limit: %d pages, timeout: %d seconds, workers: %d)",
                 domain, limit, max_time, num_workers)

        q: Deque[str] = deque()
        found_emails: Set[str] = set()

        # Seed initial URL
        if seed_response is not None:
            start_url = seed_response.url
        else:
            start_url = f"https://{domain}"
        canon_start = self._canonicalize_url(start_url)
        with self._lock:
            self._seen_urls.add(canon_start)
        q.append(canon_start)

        def worker():
            while True:
                # 1) timeout guard
                if time.time() - start_time > max_time:
                    return

                # 2) grab next URL
                with self._lock:
                    if not q:
                        return
                    url = q.popleft()

                # 3) pre-fetch domain‐limit check
                with _global_page_lock:
                    if go_global_page_count[domain] >= limit:
                        return

                # 4) do the actual fetch
                log.debug("[%s] Fetching %s", threading.current_thread().name, url)
                resp = http_client.safe_get(url, retry_count=2)
                if not resp:
                    continue   # failed fetch doesn’t count

                # 5) only now increment
                with _global_page_lock:
                    go_global_page_count[domain] += 1
                    current_count = go_global_page_count[domain]

                # 6) log with the true count
                log.debug("[%s] Crawled %s (%d/%d)",
                          threading.current_thread().name,
                          url,
                          current_count,
                          limit)

                # 7) process contents
                try:
                    self._process_response(resp, q, domain, found_emails)
                except Exception as e:
                    log.warning("Worker parse error on %s: %s", url, e)

                # 8) drop out immediately if we’ve reached the limit
                if current_count >= limit:
                    return

        # Spawn worker threads
        threads = []
        for i in range(num_workers):
            t = threading.Thread(target=worker, name=f"CrawlerThread-{i+1}")
            t.daemon = True
            t.start()
            threads.append(t)

        # Wait for all threads to finish
        for t in threads:
            t.join()

        total_time = time.time() - start_time
        with self._lock:
            seen_count = len(self._seen_urls)
        log.info(
            "Crawl of %s completed: %d pages fetched, %d unique URLs seen, %d emails, %.1f seconds",
            domain,
            go_global_page_count[domain],
            seen_count,
            len(found_emails),
            total_time
        )

        return found_emails

    def _process_response(
        self,
        resp: Any,
        q: Deque[str],
        domain: str,
        found_emails: Set[str],
    ) -> None:
        """
        Extract emails and internal links from a response, enqueueing only new canonical URLs.
        """
        page_url = resp.url
        html = resp.text

        # 1) Extract emails
        try:
            hits = self.email_extractor.extract_from_response(resp)
            found_emails.update(hits)
        except Exception as e:
            log.warning("Email extract error on %s: %s", page_url, e)

        # 2) Discover and enqueue same-domain links
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().startswith("mailto:"):
                continue
            full_url = urljoin(page_url, href)
            if not validate_url(full_url):
                continue
            netloc = urlparse(full_url).netloc
            if domain not in normalise_domain(netloc):
                continue

            canon = self._canonicalize_url(full_url)
            with self._lock:
                if canon not in self._seen_urls:
                    self._seen_urls.add(canon)
                    q.append(canon)

# Global crawler instance
crawler = Crawler()
