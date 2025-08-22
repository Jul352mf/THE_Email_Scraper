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
import os
import time
import random
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from collections import Counter
import aiohttp
from aiohttp import ClientTimeout, ClientSession, TCPConnector

from scraper.config import config
from scraper.http_client import (
    AccessMethod, DomainPattern, validate_url, normalise_domain,
    canonicalise, _domain_patterns
)

log = logging.getLogger(__name__)


class AsyncCircuitBreaker:
    """
    Async circuit breaker for HTTP requests.
    Prevents cascading failures and implements smart retry logic.
    """
    
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._states: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        
    async def can_request(self, domain: str) -> bool:
        """Check if requests to domain are allowed."""
        async with self._lock:
            state = self._get_state(domain)
            current_time = time.time()
            
            if state['failure_count'] < self.failure_threshold:
                return True  # CLOSED state
                
            # OPEN state - check if recovery time has passed
            time_since_failure = current_time - state['last_failure_time']
            if time_since_failure > self.recovery_timeout:
                state['state'] = 'HALF_OPEN'
                log.info("Circuit breaker HALF_OPEN for domain: %s", domain)
                return True
                
            log.debug(
                "Circuit breaker OPEN for domain: %s (%d failures)",
                domain, state['failure_count']
            )
            return False
    
    async def record_success(self, domain: str) -> None:
        """Record successful request."""
        async with self._lock:
            state = self._get_state(domain)
            if state['state'] == 'HALF_OPEN':
                # Recovery successful
                state['state'] = 'CLOSED'
                state['failure_count'] = 0
                log.info("Circuit breaker CLOSED for domain: %s", domain)
            elif state['failure_count'] > 0:
                # Partial recovery
                state['failure_count'] = max(0, state['failure_count'] - 1)
    
    async def record_failure(self, domain: str, error_type: str) -> None:
        """Record failed request."""
        async with self._lock:
            state = self._get_state(domain)
            state['failure_count'] += 1
            state['last_failure_time'] = time.time()
            state['error_types'][error_type] += 1
            
            if state['failure_count'] >= self.failure_threshold:
                state['state'] = 'OPEN'
                log.warning(
                    "Circuit breaker OPEN for domain: %s "
                    "(%d failures, last: %s)",
                    domain, state['failure_count'], error_type
                )
    
    def _get_state(self, domain: str) -> Dict[str, Any]:
        """Get or create circuit breaker state for domain."""
        if domain not in self._states:
            self._states[domain] = {
                'state': 'CLOSED',
                'failure_count': 0,
                'last_failure_time': 0.0,
                'error_types': Counter()
            }
        return self._states[domain]
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        async with self._lock:
            stats = {
                'domains_tracked': len(self._states),
                'open_circuits': 0,
                'half_open_circuits': 0,
                'total_failures': 0,
                'domain_details': {}
            }
            
            for domain, state in self._states.items():
                if state['state'] == 'OPEN':
                    stats['open_circuits'] += 1
                elif state['state'] == 'HALF_OPEN':
                    stats['half_open_circuits'] += 1
                    
                stats['total_failures'] += state['failure_count']
                stats['domain_details'][domain] = {
                    'state': state['state'],
                    'failure_count': state['failure_count'],
                    'error_types': dict(state['error_types'])
                }
            
            return stats


class AsyncTokenBucket:
    """
    Async token bucket for rate limiting.
    Non-blocking implementation that respects rate limits.
    """
    
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.stats = {
            'requests_allowed': 0,
            'requests_delayed': 0,
            'total_wait_time': 0.0
        }
    
    async def consume(self, tokens: float = 1.0) -> None:
        """Consume tokens, waiting if necessary."""
        async with self._lock:
            await self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                self.stats['requests_allowed'] += 1
                return
            
            # Calculate wait time needed
            tokens_needed = tokens - self.tokens
            wait_time = tokens_needed / self.refill_rate
            
            log.debug(
                "Rate limiting: waiting %.2f seconds for %d tokens",
                wait_time, tokens
            )
            
            self.stats['requests_delayed'] += 1
            self.stats['total_wait_time'] += wait_time
            
        # Wait outside the lock
        await asyncio.sleep(wait_time)
        
        # Try again after waiting
        async with self._lock:
            await self._refill()
            self.tokens = max(0, self.tokens - tokens)
            self.stats['requests_allowed'] += 1
    
    async def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        current_time = time.time()
        elapsed = current_time - self.last_refill
        tokens_to_add = elapsed * self.refill_rate
        
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = current_time


class AsyncSessionManager:
    """
    Async session manager with connection pooling and lifecycle management.
    """
    
    def __init__(self, max_sessions: int = 100):
        self.max_sessions = max_sessions
        self._sessions: Dict[str, ClientSession] = {}
        self._session_stats: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        
    async def get_session(self, domain: str) -> ClientSession:
        """Get or create session for domain."""
        async with self._lock:
            if domain in self._sessions:
                session = self._sessions[domain]
                if not session.closed:
                    # Update access time
                    self._session_stats[domain]['last_access'] = time.time()
                    self._session_stats[domain]['request_count'] += 1
                    return session
                else:
                    # Clean up closed session
                    del self._sessions[domain]
                    del self._session_stats[domain]
            
            # Create new session
            connector = TCPConnector(
                limit=30,  # Connection pool limit per domain
                limit_per_host=10,
                ttl_dns_cache=300,
                use_dns_cache=True,
                ssl=not config.insecure_ssl,
                keepalive_timeout=30
            )
            
            timeout = ClientTimeout(
                total=config.request_timeout[0] + config.request_timeout[1],
                connect=config.request_timeout[0], 
                sock_read=config.request_timeout[1]
            )
            
            session = ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': self._get_user_agent()},
                auto_decompress=True
            )
            
            self._sessions[domain] = session
            self._session_stats[domain] = {
                'created': time.time(),
                'last_access': time.time(),
                'request_count': 0
            }
            
            log.debug("Created new async session for domain: %s", domain)
            return session
    
    def _get_user_agent(self) -> str:
        """Get rotating User-Agent."""
        if hasattr(config, 'user_agents') and config.user_agents:
            return random.choice(config.user_agents)
        return 'THE_Email_Scraper/1.0 Async'
    
    async def close_all(self) -> None:
        """Close all sessions."""
        async with self._lock:
            for session in self._sessions.values():
                if not session.closed:
                    await session.close()
            
            self._sessions.clear()
            self._session_stats.clear()
            log.info("Closed all async HTTP sessions")
    
    async def cleanup_stale_sessions(self, max_age: float = 3600) -> None:
        """Clean up sessions not used recently."""
        current_time = time.time()
        stale_domains = []
        
        async with self._lock:
            for domain, stats in self._session_stats.items():
                if (current_time - stats['last_access']) > max_age:
                    stale_domains.append(domain)
            
            for domain in stale_domains:
                session = self._sessions.get(domain)
                if session and not session.closed:
                    await session.close()
                
                del self._sessions[domain]
                del self._session_stats[domain]
                log.debug("Cleaned up stale session for domain: %s", domain)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get session manager statistics."""
        async with self._lock:
            total_requests = sum(
                stats['request_count'] 
                for stats in self._session_stats.values()
            )
            
            return {
                'active_sessions': len(self._sessions),
                'total_requests': total_requests,
                'max_sessions': self.max_sessions,
                'session_details': dict(self._session_stats)
            }


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
        self.max_concurrent_requests = max_concurrent_requests
        self.stats = Counter()
        self.domain_patterns = _domain_patterns
        
        # Async components
        self._circuit_breaker = AsyncCircuitBreaker()
        self._session_manager = AsyncSessionManager()
        self._rate_limiters: Dict[str, AsyncTokenBucket] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._visited = set()  # Track visited URLs (per client instance)
        
    async def __aenter__(self):
        """Async context manager entry."""
        return self
        
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
    
    async def safe_get(self,
                      url: str,
                      method: str = "GET",
                      timeout: Optional[float] = None,
                      headers: Optional[Dict[str, str]] = None,
                      retry_count: int = 1,
                      retry_delay: float = 1.0
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
        
        # Check for redirect loops (non-HEAD requests only)
        head_mode = method.upper() == "HEAD"
        canon = canonicalise(url)
        if not head_mode and canon in self._visited:
            log.warning("Redirect loop detected – already visited %s", url)
            self.stats["skipped_urls"] += 1
            return None
        
        # Use semaphore to limit concurrent requests
        async with self._semaphore:
            # Rate limiting
            if not head_mode:  # Don't rate limit HEAD requests
                rate_limiter = self._get_rate_limiter(domain)
                await rate_limiter.consume()
            
            # Get session for domain
            session = await self._session_manager.get_session(domain)
            
            # Prepare headers
            request_headers = headers.copy() if headers else {}
            if 'User-Agent' not in request_headers:
                request_headers['User-Agent'] = self._get_user_agent()
            
            # Perform request with retries
            for attempt in range(retry_count + 1):
                try:
                    # Make the HTTP request
                    async with session.request(
                        method,
                        url,
                        headers=request_headers,
                        timeout=timeout or 30,
                        allow_redirects=True
                    ) as response:
                        
                        # Track in visited set for non-HEAD requests
                        if not head_mode:
                            self._visited.add(canon)
                        
                        # Update stats
                        self.stats["total_requests"] += 1
                        status_key = f"status_{response.status}"
                        self.stats[status_key] += 1
                        
                        # Record circuit breaker success
                        await self._circuit_breaker.record_success(domain)
                        
                        log.debug(
                            "Async %s %s -> %d (%d bytes)",
                            method, url, response.status, 
                            response.content_length or 0
                        )
                        
                        # Return response (caller should handle reading)
                        return response
                
                except aiohttp.ClientError as e:
                    error_type = type(e).__name__
                    log.warning(
                        "Async request failed (attempt %d/%d): %s - %s",
                        attempt + 1, retry_count + 1, url, e
                    )
                    
                    # Record circuit breaker failure
                    await self._circuit_breaker.record_failure(domain, error_type)
                    
                    # Update stats
                    self.stats["failed_requests"] += 1
                    self.stats[f"error_{error_type}"] += 1
                    
                    # Retry with delay
                    if attempt < retry_count:
                        await asyncio.sleep(retry_delay * (2 ** attempt))
                    
                except Exception as e:
                    log.error("Unexpected async request error: %s - %s", url, e)
                    await self._circuit_breaker.record_failure(
                        domain, "unexpected_error"
                    )
                    self.stats["unexpected_errors"] += 1
                    break
            
            return None
    
    def _is_blocked_domain(self, host: str, path: str) -> bool:
        """Check if domain or file extension is blocked."""
        blocked = {
            p.strip().lower()
            for p in os.getenv("BLOCKED_DOMAINS", "").split(",")
            if p.strip()
        }
        
        for pat in blocked:
            if not pat.startswith(".") and host.endswith(pat):
                return True
            if pat.startswith(".") and path.endswith(pat):
                return True
        
        return False
    
    def _get_user_agent(self) -> str:
        """Get rotating User-Agent."""
        if hasattr(config, 'user_agents') and config.user_agents:
            return random.choice(config.user_agents)
        return 'THE_Email_Scraper/1.0 Async'
    
    async def batch_get(self,
                       urls: List[str],
                       method: str = "GET",
                       timeout: Optional[float] = None,
                       headers: Optional[Dict[str, str]] = None
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
        
        async def fetch_single(url: str) -> Tuple[str, Optional[aiohttp.ClientResponse]]:
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
        successful = sum(1 for _, resp in processed_results if resp is not None)
        
        log.info(
            "Async batch request completed: %d/%d successful in %.2f seconds "
            "(%.2f requests/sec)",
            successful, len(urls), elapsed,
            len(urls) / elapsed if elapsed > 0 else 0
        )
        
        return processed_results
    
    async def probe_domain_access(self, domain: str) -> Optional[DomainPattern]:
        """
        Async version of domain access probing.
        Tests different access methods and caches the working pattern.
        """
        # Check cached pattern first
        if domain in self.domain_patterns:
            pattern = self.domain_patterns[domain]
            # Refresh if pattern is old (24 hours)
            if time.time() - pattern.last_checked < 86400:
                log.debug(
                    "Using cached domain pattern for %s: %s",
                    domain, pattern.method.value
                )
                return pattern
        
        log.info("Async probing domain access methods for: %s", domain)
        
        # Methods to try in preference order
        methods_to_try = [
            (AccessMethod.HTTPS, f"https://{domain}"),
            (AccessMethod.HTTPS_WWW, f"https://www.{domain}"),
            (AccessMethod.HTTP, f"http://{domain}"),
            (AccessMethod.HTTP_WWW, f"http://www.{domain}"),
        ]
        
        # Try all methods concurrently (with HEAD requests for speed)
        async def test_method(method: AccessMethod, test_url: str) -> Optional[DomainPattern]:
            try:
                log.debug("Async testing %s: %s", method.value, test_url)
                response = await self.safe_get(test_url, method="HEAD", timeout=10)
                
                if response and response.status < 400:
                    pattern = DomainPattern(
                        domain=domain,
                        method=method,
                        verified=True,
                        last_checked=time.time()
                    )
                    log.info(
                        "Found working access method for %s: %s",
                        domain, method.value
                    )
                    return pattern
                    
            except Exception as e:
                log.debug(
                    "Access method %s failed for %s: %s",
                    method.value, domain, e
                )
            
            return None
        
        # Test all methods concurrently
        results = await asyncio.gather(
            *[test_method(method, url) for method, url in methods_to_try],
            return_exceptions=True
        )
        
        # Find first successful pattern
        for result in results:
            if isinstance(result, DomainPattern):
                # Cache successful pattern
                self.domain_patterns[domain] = result
                return result
        
        log.warning("No working access method found for domain: %s", domain)
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
            'urls_visited': len(self._visited)
        }


# Global async HTTP client instance
async_http_client = AsyncHttpClient()


# Convenience function for batch requests
async def async_batch_get(urls: List[str],
                         timeout: Optional[float] = None,
                         headers: Optional[Dict[str, str]] = None
                         ) -> List[Tuple[str, Optional[aiohttp.ClientResponse]]]:
    """
    Convenience function for batch HTTP requests.
    
    Usage:
        results = await async_batch_get(['http://example1.com', 'http://example2.com'])
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
