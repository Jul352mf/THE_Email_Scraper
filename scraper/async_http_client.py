"""
Async HTTP Client for THE Email Scraper
TASK-045: Async I/O Migration - HTTP Client Component

This module provides asynchronous HTTP client functionality with:
- Parallel request processing with aiohttp
- Async circuit breaker pattern
- Non-blocking rate limiting
- Session management with connection pooling
- Seamless integration with existing features
"""

import asyncio
import logging
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from collections import Counter
import aiohttp

from scraper.config import config
from scraper.http_client import (
    AccessMethod, DomainPattern, validate_url, normalise_domain,
    _domain_patterns
)
from scraper.async_components import (
    AsyncCircuitBreaker, AsyncTokenBucket, AsyncSessionManager
)
log = logging.getLogger(__name__)


class AsyncHttpClient:
    """
    Async HTTP Client with parallel processing capabilities.
    
    Key improvements over sync version:
    - Concurrent request processing
    - Non-blocking rate limiting
    - Async circuit breaker protection
    - Connection pooling with aiohttp
    - Batch request support
    """
    
    def __init__(self, max_concurrent_requests: int = 50):
        """Initialise async HTTP client state."""
        self.max_concurrent_requests = max_concurrent_requests
        self.stats = Counter()
        self.domain_patterns = _domain_patterns
        # Async components
        self._circuit_breaker = AsyncCircuitBreaker()
        self._session_manager = AsyncSessionManager()
        self._rate_limiters: Dict[str, AsyncTokenBucket] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
    # Global redirect loop tracking removed; handled per-domain in orchestrator
    # to avoid prematurely blocking discovery pages.
        self._last_session_cleanup = time.time()
        self._session_cleanup_interval = 900  # seconds
        
    async def __aenter__(self):
        """Async context manager entry."""
        return self
        
    # ------------------------------------------------------------------
    def _is_blocked_domain(self, netloc: str, path: str) -> bool:
        """Lightweight filter to skip obviously irrelevant / static targets.

        Mirrors a subset of the sync client's filtering (kept minimal to avoid
        adding heavy dependencies). This prevents wasting requests on assets
        or large binary files and avoids social media domains unlikely to
        yield direct contact emails.
        """
        # Common static / binary extensions we never need to fetch
        static_ext = (
            '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico',
            '.css', '.js', '.json', '.xml', '.pdf', '.zip', '.rar', '.7z',
            '.tar', '.gz', '.mp4', '.mp3', '.avi', '.mov', '.wmv', '.woff',
            '.woff2', '.ttf', '.otf'
        )
        if path.endswith(static_ext):
            return True
        # Skip huge pagination or tracking query patterns quickly
        if any(token in path for token in ('/wp-json', '/feed', '/tags/')):
            return True
    # Social / user generated content domains (low signal for direct
    # email pages)
        blacklist_substrings = (
            'linkedin.com', 'facebook.com', 'instagram.com', 'twitter.com',
            'tiktok.com', 'youtube.com', 'youtu.be', 'pinterest.com'
        )
        if any(b in netloc for b in blacklist_substrings):
            return True
        return False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def close(self) -> None:
        """Close all sessions and clean up resources."""
        await self._session_manager.close_all()
        log.info("Async HTTP client closed")
    
    def _get_rate_limiter(self, domain: str) -> AsyncTokenBucket:
        """Get or create rate limiter for domain."""
        if domain not in self._rate_limiters:
            # Create rate limiter based on config
            requests_per_second = getattr(config, 'requests_per_second', 1.0)
            capacity = getattr(config, 'burst_capacity', 5.0)
            
            self._rate_limiters[domain] = AsyncTokenBucket(
                capacity=capacity,
                refill_rate=requests_per_second
            )
        
        return self._rate_limiters[domain]
    
    async def safe_get(
        self,
        url: str,
        method: str = "GET",
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 1,
        retry_delay: float = 1.0,
    ) -> Optional[aiohttp.ClientResponse]:
        """
        Async version of safe_get with circuit breaker and rate limiting.
        """
        # Validate URL early
        if not validate_url(url):
            log.warning("Skipping invalid URL: %s", url)
            self.stats["skipped_urls"] += 1
            return None
        
        parsed = urlparse(url)
        domain = normalise_domain(parsed.netloc)
        
        # Check blocked domains
        if self._is_blocked_domain(parsed.netloc.lower(), parsed.path.lower()):
            log.debug("Blocked domain or extension: %s", url)
            self.stats["skipped_urls"] += 1
            return None
        
        # Check circuit breaker
        if not await self._circuit_breaker.can_request(domain):
            log.debug("Circuit breaker open for domain: %s", domain)
            self.stats["circuit_breaker_blocked"] += 1
            return None

        head_mode = method.upper() == "HEAD"

        # Use semaphore to limit concurrent requests
        async with self._semaphore:
            # Rate limiting
            if not head_mode:  # Don't rate limit HEAD requests
                rate_limiter = self._get_rate_limiter(domain)
                await rate_limiter.consume()
            
            # Get session for domain
            session = await self._session_manager.get_session(domain)
            # Opportunistic stale session cleanup (time based)
            now = time.time()
            if (
                now - self._last_session_cleanup
            ) > self._session_cleanup_interval:
                try:
                    await self._session_manager.cleanup_stale_sessions(
                        max_age=1800
                    )
                except Exception:
                    pass
                self._last_session_cleanup = now
            
            # Prepare headers
            request_headers = headers.copy() if headers else {}
            if 'User-Agent' not in request_headers:
                request_headers['User-Agent'] = self._get_user_agent()
            
            # Perform request with retries
            for attempt in range(retry_count + 1):
                try:
                    async with session.request(
                        method,
                        url,
                        headers=request_headers,
                        timeout=timeout or 30,
                        allow_redirects=True,
                    ) as response:
                        # Buffer body inside context so downstream
                        # await resp.text() works after the context
                        try:
                            body_text = await response.text()
                        except Exception:
                            body_text = ""
                        content_type = response.headers.get(
                            "Content-Type", ""
                        )
                        status = response.status
                        # Update stats
                        self.stats["total_requests"] += 1
                        self.stats[f"status_{status}"] += 1
                        await self._circuit_breaker.record_success(domain)
                        log.debug(
                            "Async %s %s -> %d (len=%d)",
                            method,
                            url,
                            status,
                            len(body_text),
                        )
                        
                        class _BufferedResp:
                            __slots__ = ("status", "_body", "headers")

                            def __init__(self, st, body, ct):
                                self.status = st
                                self._body = body
                                self.headers = {"Content-Type": ct}

                            async def text(self):  # mimic aiohttp API
                                return self._body
                        return _BufferedResp(status, body_text, content_type)

                except aiohttp.ClientError as e:
                    error_type = type(e).__name__
                    log.warning(
                        "Async request failed (attempt %d/%d): %s - %s",
                        attempt + 1,
                        retry_count + 1,
                        url,
                        e,
                    )
                    await self._circuit_breaker.record_failure(
                        domain, error_type
                    )
                    self.stats["failed_requests"] += 1
                    self.stats[f"error_{error_type}"] += 1
                    if attempt < retry_count:
                        await asyncio.sleep(retry_delay * (2 ** attempt))
                except Exception as e:
                    log.error(
                        "Unexpected async request error: %s - %s", url, e
                    )
                    await self._circuit_breaker.record_failure(
                        domain, "unexpected_error"
                    )
                    self.stats["unexpected_errors"] += 1
                    break

            return None
    
    def _get_user_agent(self) -> str:
        """Get rotating User-Agent."""
        if hasattr(config, 'user_agents') and config.user_agents:
            return random.choice(config.user_agents)
        return 'THE_Email_Scraper/1.0 Async'
    
    async def batch_get(
        self,
        urls: List[str],
        method: str = "GET",
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> List[Tuple[str, Optional[aiohttp.ClientResponse]]]:
        """
        Perform multiple HTTP requests concurrently.
        
        Returns:
            List of (url, response) tuples. Response is None if failed.
        """
        if not urls:
            return []
        
        log.info("Starting async batch request for %d URLs", len(urls))
        start_time = time.time()
        
        async def fetch_single(
            url: str,
        ) -> Tuple[str, Optional[aiohttp.ClientResponse]]:
            """Fetch single URL and return tuple."""
            try:
                response = await self.safe_get(
                    url, method=method, timeout=timeout, headers=headers
                )
                return (url, response)
            except Exception as e:
                log.error("Batch request failed for %s: %s", url, e)
                return (url, None)
        
        # Execute all requests concurrently
        results = await asyncio.gather(
            *[fetch_single(url) for url in urls],
            return_exceptions=True
        )
        
        # Process results and handle exceptions
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                log.error("Batch request exception: %s", result)
                processed_results.append(("", None))
            else:
                processed_results.append(result)
        
        elapsed = time.time() - start_time
        successful = sum(
            1 for _, resp in processed_results if resp is not None
        )
        
        log.info(
            "Async batch request completed: %d/%d successful in %.2f seconds "
            "(%.2f requests/sec)",
            successful, len(urls), elapsed,
            len(urls) / elapsed if elapsed > 0 else 0
        )
        
        return processed_results
    
    async def probe_domain_access(
        self, domain: str
    ) -> Optional[DomainPattern]:
        """Probe access methods sequentially.

        Order: https -> http -> https_www -> http_www
        """
        existing = self.domain_patterns.get(domain)
        if existing and (time.time() - existing.last_checked) < 86400:
            log.debug(
                "Using cached domain pattern for %s: %s",
                domain,
                existing.method.value,
            )
            return existing

        log.info("Probing domain (sequential) for access method: %s", domain)
        method_sequence: list[tuple[AccessMethod, str]] = [
            (AccessMethod.HTTPS, f"https://{domain}"),
            (AccessMethod.HTTP, f"http://{domain}"),
            (AccessMethod.HTTPS_WWW, f"https://www.{domain}"),
            (AccessMethod.HTTP_WWW, f"http://www.{domain}"),
        ]
        for method_enum, test_url in method_sequence:
            try:
                log.debug(
                    "Testing access method %s: %s", method_enum.value, test_url
                )
                response = await self.safe_get(
                    test_url, method="HEAD", timeout=8
                ) or await self.safe_get(test_url, method="GET", timeout=12)
                if response and response.status < 400:
                    pattern = DomainPattern(
                        domain=domain,
                        method=method_enum,
                        verified=True,
                        last_checked=time.time(),
                    )
                    self.domain_patterns[domain] = pattern
                    log.info(
                        "Selected access method for %s: %s",
                        domain,
                        method_enum.value,
                    )
                    return pattern
            except Exception as e:
                log.debug(
                    "Access method %s failed for %s: %s",
                    method_enum.value,
                    domain,
                    e,
                )
        log.warning("All access methods failed for domain: %s", domain)
        return None
    
    async def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        circuit_stats = await self._circuit_breaker.get_stats()
        session_stats = await self._session_manager.get_stats()
        
        # Rate limiter stats
        rate_limiter_stats = {}
        for domain, limiter in self._rate_limiters.items():
            rate_limiter_stats[domain] = limiter.stats.copy()
        
        return {
            'request_stats': dict(self.stats),
            'circuit_breaker': circuit_stats,
            'session_manager': session_stats,
            'rate_limiters': rate_limiter_stats,
            'concurrent_limit': self.max_concurrent_requests,
            'domains_cached': len(self.domain_patterns),
            'urls_visited': -1  # deprecated global metric
        }


# Global async HTTP client instance
async_http_client = AsyncHttpClient()


# Convenience function for batch requests
async def async_batch_get(
    urls: List[str],
    timeout: Optional[float] = None,
    headers: Optional[Dict[str, str]] = None,
) -> List[Tuple[str, Optional[aiohttp.ClientResponse]]]:
    """
    Convenience function for batch HTTP requests.
    
    Usage:
        results = await async_batch_get([
            'http://example1.com',
            'http://example2.com'
        ])
    """
    async with AsyncHttpClient(max_concurrent_requests=20) as client:
        return await client.batch_get(urls, timeout=timeout, headers=headers)


if __name__ == "__main__":
    # Example usage and performance test
    async def test_performance():
        test_urls = [
            "https://httpbin.org/status/200",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/json",
            "https://httpbin.org/headers",
            "https://httpbin.org/user-agent"
        ]
        
        print("Testing async HTTP client performance...")
        start = time.time()
        
        async with AsyncHttpClient(max_concurrent_requests=10) as client:
            results = await client.batch_get(test_urls)
        
        elapsed = time.time() - start
        successful = sum(1 for _, resp in results if resp is not None)
        
        print(f"Fetched {len(test_urls)} URLs in {elapsed:.2f} seconds")
        print(f"Success rate: {successful}/{len(test_urls)}")
    
    # Run the test
    asyncio.run(test_performance())
