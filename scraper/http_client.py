"""
Enhanced HTTP client module (rev-3)
==================================

Change in this revision
-----------------------
* **HEAD requests no longer interact with the redirect-loop set**:
  * We *do not* check `visited` before sending a HEAD.
  * We *do not* add the canonical URL to `visited` after a HEAD.

Everything else remains as in rev-2 (TLS verification by default, canonical URL
loop detection, shared per-thread session, debug dump, etc.).
"""

from __future__ import annotations

import logging
import os
import re
from threading import local
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse, urljoin
import random
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, SSLError, RequestException
from urllib3.util.retry import Retry

from scraper.config import config
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from collections import Counter

# Import performance optimizations
try:
    from scraper.performance_optimizer import (
        connection_pool, optimize_request, resource_monitor
    )
    PERFORMANCE_OPTIMIZATIONS_AVAILABLE = True
except ImportError:
    PERFORMANCE_OPTIMIZATIONS_AVAILABLE = False

log = logging.getLogger(__name__)

# suppress urllib3 warnings once at module top
urllib3.disable_warnings(InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

_thread_local = local()
_domain_buckets: dict[str, TokenBucket] = {}


class AccessMethod(Enum):
    """Different ways to access a domain."""
    HTTPS = "https"
    HTTP = "http"
    HTTPS_WWW = "https_www"
    HTTP_WWW = "http_www"


@dataclass
class DomainPattern:
    """Cache successful access patterns for domains."""
    domain: str
    method: AccessMethod
    verified: bool = False
    last_checked: float = 0.0
    
    def build_url(self, path: str = "") -> str:
        """Build URL using this domain pattern."""
        if self.method == AccessMethod.HTTPS:
            base = f"https://{self.domain}"
        elif self.method == AccessMethod.HTTP:
            base = f"http://{self.domain}"
        elif self.method == AccessMethod.HTTPS_WWW:
            base = f"https://www.{self.domain}"
        elif self.method == AccessMethod.HTTP_WWW:
            base = f"http://www.{self.domain}"
        else:
            base = f"https://{self.domain}"  # fallback
        
        if path:
            return urljoin(base, path.lstrip("/"))
        return base


# Domain pattern cache
_domain_patterns: Dict[str, DomainPattern] = {}


# ---------------------------------------------------------------------------
# Circuit Breaker Pattern for Smart Retry Logic
# ---------------------------------------------------------------------------

@dataclass
class CircuitBreakerState:
    """Track circuit breaker state for a domain."""
    failure_count: int = 0
    last_failure_time: float = 0.0
    state: str = "closed"  # closed, open, half-open
    success_count: int = 0
    
    @property
    def is_open(self) -> bool:
        return self.state == "open"
    
    @property
    def is_half_open(self) -> bool:
        return self.state == "half_open"


class CircuitBreaker:
    """Circuit breaker implementation for domain reliability."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self._states: Dict[str, CircuitBreakerState] = {}
        self._lock = threading.Lock()
    
    def _get_state(self, domain: str) -> CircuitBreakerState:
        """Get or create circuit breaker state for domain."""
        if domain not in self._states:
            self._states[domain] = CircuitBreakerState()
        return self._states[domain]
    
    def can_request(self, domain: str) -> bool:
        """Check if requests are allowed to this domain."""
        with self._lock:
            state = self._get_state(domain)
            
            if state.state == "closed":
                return True
            elif state.state == "open":
                # Check if recovery timeout has passed
                current_time = time.time()
                if current_time - state.last_failure_time > self.recovery_timeout:
                    state.state = "half_open"
                    state.success_count = 0
                    log.debug(
                        "Circuit breaker half-open for domain: %s", domain
                    )
                    return True
                return False
            else:  # half_open
                return True
    
    def record_success(self, domain: str) -> None:
        """Record successful request."""
        with self._lock:
            state = self._get_state(domain)
            
            if state.is_half_open:
                state.success_count += 1
                if state.success_count >= self.success_threshold:
                    state.state = "closed"
                    state.failure_count = 0
                    log.info("Circuit breaker closed for domain: %s", domain)
            elif state.state == "closed":
                # Reset failure count on success
                state.failure_count = 0
    
    def record_failure(self, domain: str, error_type: str) -> None:
        """Record failed request."""
        # Don't count client errors (4xx) as circuit breaker failures
        if error_type.startswith('4'):
            return
            
        with self._lock:
            state = self._get_state(domain)
            
            # If we're in half-open and get a failure, go back to open
            if state.is_half_open:
                state.state = "open"
                state.failure_count += 1
                state.last_failure_time = time.time()
                log.warning(
                    "Circuit breaker reopened for domain: %s "
                    "(half-open failure)", domain
                )
            else:
                state.failure_count += 1
                state.last_failure_time = time.time()
                
                if state.failure_count >= self.failure_threshold:
                    state.state = "open"
                    log.warning(
                        "Circuit breaker opened for domain: %s (failures: %d)",
                        domain, state.failure_count
                    )
    
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                domain: {
                    "state": state.state,
                    "failure_count": state.failure_count,
                    "success_count": state.success_count,
                    "last_failure_time": state.last_failure_time
                }
                for domain, state in self._states.items()
            }


class SmartRetryStrategy:
    """Exponential backoff with jitter for retry logic."""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
    
    def should_retry(self, attempt: int, status_code: Optional[int],
                     exception: Optional[Exception]) -> bool:
        """Determine if request should be retried."""
        if attempt >= self.max_retries:
            return False
        
        # Don't retry client errors (4xx)
        if status_code and 400 <= status_code < 500:
            return False
        
        # Don't retry 403 Forbidden specifically
        if status_code == 403:
            return False
        
        # Retry server errors (5xx) and connection issues
        if status_code and status_code >= 500:
            return True
        
        # Retry connection-related exceptions
        if exception and isinstance(exception, (ConnectTimeout, SSLError)):
            return True
        
        return False
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay before retry with exponential backoff."""
        delay = self.base_delay * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            # Add jitter to avoid thundering herd
            delay *= (0.5 + random.random() * 0.5)
        
        return delay


# Global circuit breaker and retry strategy instances
_circuit_breaker = CircuitBreaker()
_retry_strategy = SmartRetryStrategy()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def canonicalise(url: str) -> str:
    p = urlparse(url)
    host = p.netloc.lower().removeprefix("www.")
    path = p.path.rstrip("/") or "/"
    return urlunparse((p.scheme.lower(), host, path, "", "", ""))

def validate_url(url: str) -> bool:
    if not url or len(url) > config.max_url_length:
        return False
    try:
        p = urlparse(url)
        if p.scheme not in {"http", "https"} or not p.netloc:
            return False
        if re.search(r"^(file|data|javascript):", url, re.I):
            return False
        return True
    except Exception:
        return False

def _get_bucket_for(domain: str) -> TokenBucket:
    """Get or create a token bucket for rate limiting a specific domain."""
    rate = 1.0 / config.min_crawl_delay
    cap = config.max_crawl_delay / config.min_crawl_delay
    bucket = _domain_buckets.get(domain)
    if not bucket:
        bucket = TokenBucket(rate_per_sec=rate, capacity=cap)
        _domain_buckets[domain] = bucket
        log.debug("Created token bucket for domain %s (rate=%.2f/s, capacity=%.1f)", 
                  domain, rate, cap)
    return bucket

def get_all_bucket_stats() -> Dict[str, Dict[str, Any]]:
    """Get performance statistics for all domain token buckets."""
    stats = {}
    for domain, bucket in _domain_buckets.items():
        try:
            stats[domain] = bucket.get_stats()
        except Exception as e:
            log.warning("Failed to get stats for domain %s: %s", domain, e)
            stats[domain] = {'error': str(e)}
    return stats

def get_bucket_performance_summary() -> Dict[str, Any]:
    """Get a summary of token bucket performance across all domains."""
    all_stats = get_all_bucket_stats()
    
    if not all_stats:
        return {'total_domains': 0, 'message': 'No token buckets created yet'}
    
    total_requests = sum(stats.get('total_requests', 0) for stats in all_stats.values() 
                        if isinstance(stats, dict) and 'total_requests' in stats)
    total_wait_time = sum(stats.get('total_wait_time_seconds', 0) for stats in all_stats.values()
                         if isinstance(stats, dict) and 'total_wait_time_seconds' in stats)
    total_contention = sum(stats.get('contention_count', 0) for stats in all_stats.values()
                          if isinstance(stats, dict) and 'contention_count' in stats)
    
    valid_domains = [domain for domain, stats in all_stats.items() 
                    if isinstance(stats, dict) and 'error' not in stats]
    
    avg_wait_time = (total_wait_time / max(total_requests, 1)) * 1000  # Convert to ms
    contention_rate = (total_contention / max(total_requests, 1)) * 100
    
    return {
        'total_domains': len(all_stats),
        'valid_domains': len(valid_domains),
        'total_requests': total_requests,
        'total_wait_time_seconds': round(total_wait_time, 3),
        'average_wait_time_ms': round(avg_wait_time, 2),
        'total_contention_events': total_contention,
        'contention_rate_percent': round(contention_rate, 2),
        'most_active_domains': sorted(
            [(domain, stats.get('total_requests', 0)) for domain, stats in all_stats.items()
             if isinstance(stats, dict) and 'total_requests' in stats],
            key=lambda x: x[1], reverse=True
        )[:10]  # Top 10 most active domains
    }


class OptimizedTokenBucket:
    """
    Lock-free token bucket implementation using atomic operations where possible.
    Reduces contention for high-throughput scenarios.
    """
    
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = rate_per_sec
        self.capacity = capacity
        self._lock = threading.RLock()  # Use RLock for nested acquisition
        self._tokens = capacity
        self._last = time.time()
        
        # Statistics for monitoring
        self._total_requests = 0
        self._total_wait_time = 0.0
        self._contention_count = 0
    
    def consume(self, tokens: float = 1.0) -> float:
        """
        Consume tokens from bucket, returning wait time.
        
        Args:
            tokens: Number of tokens to consume
            
        Returns:
            Time spent waiting (for statistics)
        """
        wait_start = time.time()
        self._total_requests += 1
        
        # Fast path: try non-blocking check first
        now = time.time()
        
        # Try to acquire lock with minimal holding time
        lock_acquired = self._lock.acquire(blocking=False)
        if not lock_acquired:
            # Lock contention - track and wait
            self._contention_count += 1
            self._lock.acquire()
        
        try:
            # Refill tokens based on time elapsed
            delta_time = now - self._last
            if delta_time > 0:
                refill_tokens = delta_time * self.rate
                self._tokens = min(self.capacity, self._tokens + refill_tokens)
                self._last = now
            
            # Fast path: tokens available
            if self._tokens >= tokens:
                self._tokens -= tokens
                wait_time = time.time() - wait_start
                self._total_wait_time += wait_time
                return wait_time
            
            # Calculate wait time needed
            tokens_needed = tokens - self._tokens
            wait_duration = tokens_needed / self.rate
            
        finally:
            self._lock.release()
        
        # Sleep outside lock to allow other threads progress
        if wait_duration > 0:
            time.sleep(wait_duration)
        
        # Re-acquire lock and consume tokens
        with self._lock:
            now = time.time()
            delta_time = now - self._last
            if delta_time > 0:
                refill_tokens = delta_time * self.rate
                self._tokens = min(self.capacity, self._tokens + refill_tokens)
                self._last = now
            
            # Consume tokens (should be available now)
            if self._tokens >= tokens:
                self._tokens -= tokens
            else:
                # Edge case: consume what we have
                self._tokens = 0
        
        wait_time = time.time() - wait_start
        self._total_wait_time += wait_time
        return wait_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics for this token bucket."""
        with self._lock:
            total_requests = max(self._total_requests, 1)  # Avoid division by zero
            return {
                'total_requests': self._total_requests,
                'total_wait_time_seconds': round(self._total_wait_time, 3),
                'average_wait_time_ms': round((self._total_wait_time / total_requests) * 1000, 2),
                'contention_count': self._contention_count,
                'contention_rate_percent': round((self._contention_count / total_requests) * 100, 2),
                'current_tokens': round(self._tokens, 2),
                'capacity': self.capacity,
                'rate_per_second': self.rate
            }
    
    def reset_stats(self) -> None:
        """Reset performance statistics."""
        with self._lock:
            self._total_requests = 0
            self._total_wait_time = 0.0
            self._contention_count = 0

# Legacy TokenBucket class for backward compatibility
class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float):
        # Use optimized implementation internally
        self._optimized = OptimizedTokenBucket(rate_per_sec, capacity)
        self.rate = rate_per_sec
        self.capacity = capacity

    def consume(self, tokens: float = 1.0):
        """Legacy interface - returns None for backward compatibility."""
        self._optimized.consume(tokens)
        
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return self._optimized.get_stats()


# ---------------------------------------------------------------------------
# Thread-local session manager
# ---------------------------------------------------------------------------

class _SessionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._template: Optional[requests.Session] = None
        self._session_stats = Counter()  # Track session usage
        self._cleanup_counter = 0
        self._session_access_times: Dict[str, float] = {}  # Track last access time
        self._max_sessions_per_thread = 30  # Reduced from 50
        self._session_ttl = 3600  # 1 hour TTL for unused sessions

    def _build_template(self) -> requests.Session:
        """Build optimized session template with enhanced connection pooling."""
        s = requests.Session()
        s.headers.update({
            "User-Agent": config.user_agents[0],
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,*/*;q=0.8"
            )
        })
        
        # Enhanced retry strategy
        retry = Retry(
            total=3,  # Increased from 2
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            raise_on_status=False  # Don't raise on status errors
        )
        
        # Optimized HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=config.max_workers,  # One pool per worker
            pool_maxsize=20,  # Max connections per pool
            pool_block=False  # Don't block when pool is full
        )
        
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.max_redirects = max(1, config.max_redirects)
        
        log.debug("Created optimized session template with connection pooling")
        return s

    def session(self, domain: str) -> requests.Session:
        # Each thread has a dict of domain->Session
        sess_map = getattr(_thread_local, "sessions", None)
        if sess_map is None:
            sess_map = {}
            _thread_local.sessions = sess_map

        if domain not in sess_map:
            with self._lock:
                if self._template is None:
                    self._template = self._build_template()
                s = requests.Session()
                s.headers = self._template.headers.copy()
                s.adapters = self._template.adapters
                s.cookies = requests.cookies.RequestsCookieJar()
            sess_map[domain] = s
            log.debug("Created new session for domain: %s", domain)

        sess = sess_map[domain]
        sess.verify = not config.insecure_ssl
        
        # Track session usage for monitoring
        current_time = time.time()
        self._session_stats[domain] += 1
        
        # Track access time for TTL cleanup
        thread_id = threading.get_ident()
        access_key = f"{thread_id}:{domain}"
        self._session_access_times[access_key] = current_time
        
        # More frequent cleanup with lower threshold
        self._cleanup_counter += 1
        if self._cleanup_counter > 500:  # Every 500 requests (more frequent)
            self.cleanup_old_sessions()
            self._cleanup_counter = 0
            
        return sess

    def visited(self) -> set[str]:
        v = getattr(_thread_local, "visited", None)
        if v is None:
            v = set()
            _thread_local.visited = v
        return v

    def prune(self, keep: int = 1000) -> None:
        v = self.visited()
        if len(v) > keep:
            v.clear()

    def cleanup_old_sessions(self) -> None:
        """Clean up old sessions to prevent memory leaks using TTL and usage-based cleanup."""
        sess_map = getattr(_thread_local, "sessions", None)
        if not sess_map:
            return
            
        current_time = time.time()
        thread_id = threading.get_ident()
        domains_to_remove = []
        
        # TTL-based cleanup: remove sessions not accessed recently
        for domain in list(sess_map.keys()):
            access_key = f"{thread_id}:{domain}"
            last_access = self._session_access_times.get(access_key, 0)
            
            if current_time - last_access > self._session_ttl:
                domains_to_remove.append(domain)
                log.debug("Session TTL expired for domain: %s", domain)
        
        # Size-based cleanup: if still too many sessions, remove least recently used
        if len(sess_map) - len(domains_to_remove) > self._max_sessions_per_thread:
            # Sort domains by last access time and remove oldest
            remaining_domains = [d for d in sess_map.keys() if d not in domains_to_remove]
            sorted_domains = sorted(
                remaining_domains, 
                key=lambda d: self._session_access_times.get(f"{thread_id}:{d}", 0)
            )
            
            excess_count = len(remaining_domains) - self._max_sessions_per_thread
            domains_to_remove.extend(sorted_domains[:excess_count])
            log.debug("Removing %d excess sessions (limit: %d)", 
                     excess_count, self._max_sessions_per_thread)
        
        # Actually clean up the sessions
        cleaned_count = 0
        for domain in domains_to_remove:
            try:
                if domain in sess_map:
                    sess_map[domain].close()
                    del sess_map[domain]
                    cleaned_count += 1
                    
                # Clean up access time tracking
                access_key = f"{thread_id}:{domain}"
                self._session_access_times.pop(access_key, None)
                
            except Exception as e:
                log.warning("Error cleaning up session for %s: %s", domain, e)
        
        if cleaned_count > 0:
            log.debug("Cleaned up %d old sessions, %d remaining", 
                     cleaned_count, len(sess_map))
        
        # Periodic cleanup of access times for dead threads
        if len(self._session_access_times) > 1000:  # Arbitrary threshold
            self._cleanup_dead_thread_access_times()

    def _cleanup_dead_thread_access_times(self) -> None:
        """Clean up access time entries for threads that no longer exist."""
        active_threads = {t.ident for t in threading.enumerate()}
        keys_to_remove = []
        
        for access_key in list(self._session_access_times.keys()):
            try:
                thread_id_str = access_key.split(':', 1)[0]
                thread_id = int(thread_id_str)
                if thread_id not in active_threads:
                    keys_to_remove.append(access_key)
            except (ValueError, IndexError):
                # Invalid key format, remove it
                keys_to_remove.append(access_key)
        
        for key in keys_to_remove:
            del self._session_access_times[key]
        
        if keys_to_remove:
            log.debug("Cleaned up %d access time entries for dead threads", len(keys_to_remove))

    def get_session_stats(self) -> Dict[str, Any]:
        """Get comprehensive session usage statistics."""
        stats = dict(self._session_stats)
        sess_map = getattr(_thread_local, "sessions", None)
        
        stats.update({
            "active_sessions_current_thread": len(sess_map) if sess_map else 0,
            "max_sessions_per_thread": self._max_sessions_per_thread,
            "session_ttl_seconds": self._session_ttl,
            "tracked_access_times": len(self._session_access_times),
            "cleanup_counter": self._cleanup_counter
        })
        
        return stats

    def get_active_session_count(self) -> int:
        """Get number of active sessions in current thread."""
        sess_map = getattr(_thread_local, "sessions", None)
        return len(sess_map) if sess_map else 0
    
    def force_cleanup_all(self) -> None:
        """Force cleanup of all sessions (useful for testing/debugging)."""
        sess_map = getattr(_thread_local, "sessions", None)
        if sess_map:
            for domain, session in list(sess_map.items()):
                try:
                    session.close()
                    del sess_map[domain]
                except Exception as e:
                    log.warning("Error during force cleanup of session %s: %s", domain, e)
        
        # Clear access times
        thread_id = threading.get_ident()
        keys_to_remove = [k for k in self._session_access_times.keys() if k.startswith(f"{thread_id}:")]
        for key in keys_to_remove:
            del self._session_access_times[key]
        
        log.info("Forced cleanup of all sessions in current thread")


_session_mgr = _SessionManager()


# ---------------------------------------------------------------------------
# Enhanced Connection Pool Management  
# ---------------------------------------------------------------------------

@dataclass
class ConnectionPoolStats:
    """Statistics for connection pool monitoring."""
    total_connections: int = 0
    active_connections: int = 0
    pool_hits: int = 0
    pool_misses: int = 0
    connection_errors: int = 0
    pool_size_per_host: int = 10
    max_retries: int = 3
    
    @property
    def hit_rate_percent(self) -> float:
        total = self.pool_hits + self.pool_misses
        return (self.pool_hits / total * 100) if total > 0 else 0

class EnhancedConnectionPoolManager:
    """Enhanced connection pool manager with warming and statistics."""
    
    def __init__(self):
        self.stats = ConnectionPoolStats()
        self.pool_lock = threading.Lock()
        self.warmed_domains = set()
        self.domain_connection_counts = Counter()
        
    def create_optimized_session(self, domain: str) -> requests.Session:
        """Create a session with optimized connection pooling for domain."""
        session = requests.Session()
        
        # Enhanced adapter configuration
        adapter = HTTPAdapter(
            pool_connections=20,  # Increased pool size
            pool_maxsize=self.stats.pool_size_per_host,
            max_retries=Retry(
                total=self.stats.max_retries,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504, 429],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            ),
            pool_block=False  # Don't block when pool is full
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # Optimize session settings
        session.headers.update({
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'Cache-Control': 'no-cache'
        })
        
        with self.pool_lock:
            self.stats.total_connections += 1
            self.domain_connection_counts[domain] += 1
        
        return session
    
    def warm_connection(self, domain: str, pattern: Optional[DomainPattern] = None) -> bool:
        """Pre-warm connection to domain for better performance."""
        if domain in self.warmed_domains:
            return True
            
        try:
            # Use existing pattern or probe for one
            if not pattern:
                from scraper import http_client  # Avoid circular import
                pattern = http_client.probe_domain_access(domain)
            
            if pattern:
                session = self.create_optimized_session(domain)
                test_url = pattern.build_url()
                
                # Make a lightweight HEAD request to warm the connection
                response = session.head(test_url, timeout=10, allow_redirects=False)
                
                with self.pool_lock:
                    if response.status_code < 400:
                        self.warmed_domains.add(domain)
                        self.stats.pool_hits += 1
                        log.debug("✓ Warmed connection to %s", domain)
                        return True
                    else:
                        self.stats.pool_misses += 1
                        
        except Exception as e:
            with self.pool_lock:
                self.stats.connection_errors += 1
            log.debug("Failed to warm connection to %s: %s", domain, e)
            
        return False
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self.pool_lock:
            return {
                'total_connections': self.stats.total_connections,
                'active_connections': self.stats.active_connections,
                'pool_hit_rate_percent': round(self.stats.hit_rate_percent, 2),
                'pool_hits': self.stats.pool_hits,
                'pool_misses': self.stats.pool_misses,
                'connection_errors': self.stats.connection_errors,
                'warmed_domains': len(self.warmed_domains),
                'pool_size_per_host': self.stats.pool_size_per_host,
                'domain_connections': dict(self.domain_connection_counts.most_common(10))
            }
    
    def update_pool_size(self, new_size: int) -> None:
        """Dynamically update connection pool size."""
        if 1 <= new_size <= 50:
            with self.pool_lock:
                old_size = self.stats.pool_size_per_host
                self.stats.pool_size_per_host = new_size
                log.info("Updated connection pool size from %d to %d", old_size, new_size)
    
    def clear_stats(self) -> None:
        """Clear connection pool statistics."""
        with self.pool_lock:
            self.stats = ConnectionPoolStats()
            self.warmed_domains.clear()
            self.domain_connection_counts.clear()

# Global connection pool manager
_connection_pool_mgr = EnhancedConnectionPoolManager()

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class HttpClient:
    DEBUG = os.getenv("DEBUG_MODE", "0").lower() in {"1", "true", "yes"}
    DEBUG_DIR = os.getenv("DEBUG_DIR", "debug_output")

    def __init__(self) -> None:
        self.stats = Counter()
        self.domain_patterns = _domain_patterns
        if self.DEBUG and not os.path.exists(self.DEBUG_DIR):
            try:
                os.makedirs(self.DEBUG_DIR, exist_ok=True)
            except Exception as exc:
                log.warning("Failed to create debug dir %s: %s", self.DEBUG_DIR, exc)
                self.DEBUG = False

    def probe_domain_access(self, domain: str) -> Optional[DomainPattern]:
        """
        Probe different access methods for a domain and cache the working pattern.
        
        Args:
            domain: Domain to probe (should be normalized, without www or protocol)
            
        Returns:
            DomainPattern if successful access method found, None otherwise
        """
        # Check if we already have a cached pattern
        if domain in self.domain_patterns:
            pattern = self.domain_patterns[domain]
            # Refresh pattern if it's been a while (24 hours)
            if time.time() - pattern.last_checked < 86400:  # 24 hours
                log.debug("Using cached domain pattern for %s: %s", domain, pattern.method.value)
                return pattern
        
        log.info("Probing domain access methods for: %s", domain)
        
        # Try different access methods in order of preference
        methods_to_try = [
            (AccessMethod.HTTPS, f"https://{domain}"),
            (AccessMethod.HTTPS_WWW, f"https://www.{domain}"),
            (AccessMethod.HTTP, f"http://{domain}"),
            (AccessMethod.HTTP_WWW, f"http://www.{domain}"),
        ]
        
        # Try request batching for domain probing if available
        if PERFORMANCE_OPTIMIZATIONS_AVAILABLE:
            try:
                from scraper.performance_optimizer import request_batcher
                return self._probe_domain_batch(
                    domain, methods_to_try, request_batcher
                )
            except Exception as e:
                log.debug(
                    "Batch probing failed for %s, falling back: %s", domain, e
                )
        
        # Fallback to sequential probing
        for method, test_url in methods_to_try:
            log.debug("Trying %s access method: %s", method.value, test_url)
            
            try:
                # Use HEAD request for faster probing
                response = self._probe_url(test_url)
                if response and response.ok:
                    pattern = DomainPattern(
                        domain=domain,
                        method=method,
                        verified=True,
                        last_checked=time.time()
                    )
                    self.domain_patterns[domain] = pattern
                    log.info("✓ Found working access method for %s: %s", domain, method.value)
                    return pattern
            except Exception as e:
                log.debug("Access method %s failed for %s: %s", method.value, domain, e)
                continue
        
        # If no method worked, cache failure to avoid repeated probes
        pattern = DomainPattern(
            domain=domain,
            method=AccessMethod.HTTPS,  # default fallback
            verified=False,
            last_checked=time.time()
        )
        self.domain_patterns[domain] = pattern
        log.warning("✗ No working access method found for %s", domain)
        return None

    def _probe_domain_batch(self, domain: str, methods_to_try: list,
                            request_batcher) -> Optional[DomainPattern]:
        """
        Probe domain using batch requests for better performance.
        
        Args:
            domain: Domain to probe
            methods_to_try: List of (AccessMethod, URL) tuples to try
            request_batcher: Batch request handler
            
        Returns:
            DomainPattern if successful access method found, None otherwise
        """
        successful_method = None
        results = {}
        
        def create_callback(method):
            """Create callback for a specific method."""
            def callback(response):
                nonlocal successful_method
                if response and response.ok:
                    results[method] = True
                    if successful_method is None:
                        successful_method = method
                else:
                    results[method] = False
            return callback
        
        # Add all probe requests to batch
        for method, test_url in methods_to_try:
            callback = create_callback(method)
            request_batcher.add_request(
                test_url,
                callback=callback,
                method='HEAD',
                timeout=5
            )
        
        # Wait for batch completion (with timeout)
        start_time = time.time()
        max_wait = 30
        while (len(results) < len(methods_to_try) and
               time.time() - start_time < max_wait):
            time.sleep(0.1)
        
        # Find the first successful method in order of preference
        for method, test_url in methods_to_try:
            if results.get(method):
                pattern = DomainPattern(
                    domain=domain,
                    method=method,
                    verified=True,
                    last_checked=time.time()
                )
                self.domain_patterns[domain] = pattern
                log.info("✓ Found working access method for %s: %s",
                         domain, method.value)
                return pattern
        
        # If no method worked, cache failure
        pattern = DomainPattern(
            domain=domain,
            method=AccessMethod.HTTPS,
            verified=False,
            last_checked=time.time()
        )
        self.domain_patterns[domain] = pattern
        log.warning("✗ No working access method found for %s (batch)", domain)
        return None

    def _probe_url(self, url: str) -> Optional[requests.Response]:
        """
        Probe a URL with HEAD request, bypassing normal retry logic.
        
        Args:
            url: URL to probe
            
        Returns:
            Response object if successful, None otherwise
        """
        parsed = urlparse(url)
        domain = normalise_domain(parsed.netloc)
        sess = _session_mgr.session(domain)
        
        try:
            # Quick HEAD request with short timeout
            response = sess.head(
                url,
                allow_redirects=True,
                timeout=(5, 10),  # Short timeout for probing
                verify=not config.insecure_ssl
            )
            return response
        except Exception:
            return None

    def get_optimized_url(self, domain: str, path: str = "") -> Optional[str]:
        """
        Get the optimized URL for a domain using cached access patterns.
        
        Args:
            domain: Normalized domain
            path: Optional path to append
            
        Returns:
            Optimized URL if pattern exists, None otherwise
        """
        pattern = self.domain_patterns.get(domain)
        if pattern and pattern.verified:
            return pattern.build_url(path)
        return None

    def safe_get(
        self,
        url: str,
        method: str = "GET",
        timeout: Optional[tuple[float, float]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 1,
        retry_delay: float = 1.0,
        callback: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        # Validate URL early
        if not validate_url(url):
            log.warning("Skipping invalid URL: %s", url)
            self.stats["skipped_urls"] += 1
            return None
        
        # Check circuit breaker
        parsed = urlparse(url)
        domain = normalise_domain(parsed.netloc)
        
        if not _circuit_breaker.can_request(domain):
            log.debug("Circuit breaker open for domain: %s", domain)
            self.stats["circuit_breaker_blocked"] += 1
            return None
        
        # Try performance optimizations first
        if PERFORMANCE_OPTIMIZATIONS_AVAILABLE:
            try:
                # Create kwargs for optimized request
                opt_kwargs = {'timeout': timeout}
                if headers:
                    opt_kwargs['headers'] = headers
                
                optimized_response = optimize_request(url, **opt_kwargs)
                if optimized_response:
                    log.debug("Using optimized request for: %s", url)
                    self.stats["total_requests"] += 1
                    status_key = f"status_{optimized_response.status_code}"
                    self.stats[status_key] += 1
                    _circuit_breaker.record_success(domain)
                    return optimized_response
            except Exception as e:
                log.debug(
                    "Performance optimization failed for %s, falling back: %s",
                    url, e
                )
                _circuit_breaker.record_failure(domain, "optimization_error")
        
        # Try to use optimized URL if we have domain patterns
        parsed = urlparse(url)
        domain = normalise_domain(parsed.netloc)
        optimized_url = self.get_optimized_url(domain, parsed.path)
        if optimized_url and optimized_url != url:
            log.debug("Using optimized URL: %s -> %s", url, optimized_url)
            url = optimized_url

        # Blocked patterns (domains/extensions) from env
        blocked = {
            p.strip().lower()
            for p in os.getenv("BLOCKED_DOMAINS", "").split(",")
            if p.strip()
        }
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        for pat in blocked:
            if not pat.startswith(".") and host.endswith(pat):
                log.debug("Blocked domain %s (matched %s)", host, pat)
                self.stats["skipped_urls"] += 1
                return None
        for pat in blocked:
            if pat.startswith(".") and path.endswith(pat):
                log.debug("Blocked extension %s on %s", pat, url)
                self.stats["skipped_urls"] += 1
                return None

        head_mode = method.upper() == "HEAD"
        canon = canonicalise(url)

        # Throttle only non-HEAD methods
        if not head_mode:
            throttle_domain = normalise_domain(parsed.netloc)
            bucket = _get_bucket_for(throttle_domain)
            bucket.consume()

    # Prepare headers & merge rotating User-Agent.
    # Contract (TASK-002): if caller supplies headers, only inject a UA
    # when no case-insensitive "user-agent" key is present.
    # Rotation occurs per safe_get invocation (not per retry/fallback).
        hdrs = headers.copy() if headers else {}
        if getattr(config, "user_agents", None):
            has_ua = any(k.lower() == "user-agent" for k in hdrs)
            if not has_ua:
                hdrs["User-Agent"] = random.choice(config.user_agents)

        # Track domain (normalised) for session & visited
        domain = normalise_domain(parsed.netloc)
        if not getattr(_thread_local, "visited", None):
            _thread_local.visited = set()

        # Loop guard on canonical URL (only for non-HEAD)
        if not head_mode and canon in _thread_local.visited:
            log.warning("Redirect loop detected – already visited %s", url)
            self.stats["skipped_urls"] += 1
            return None

        if timeout is None:
            timeout = config.request_timeout

        sess = _session_mgr.session(domain)
        request_fn = sess.head if head_mode else sess.get
        self.stats["total_requests"] += 1

        response: Optional[requests.Response] = None
        last_exception = None
        
        # Use smart retry strategy instead of fixed retry logic
        max_attempts = max(retry_count, _retry_strategy.max_retries)
        
        for attempt in range(max_attempts):
            try:
                response = request_fn(
                    url,
                    allow_redirects=True,
                    timeout=timeout,
                    headers=hdrs,
                )
                if response is None:
                    raise requests.exceptions.ConnectionError(
                        f"No response for {url}"
                    )
                
                status = response.status_code
                log.info("HTTP %s %s → %s", method, url, status)
                
                if status == 429:
                    # Use smart retry delay for rate limiting
                    delay = _retry_strategy.get_delay(attempt)
                    log.warning(
                        "429 Too Many Requests for %s; backoff %.1fs "
                        "(attempt %d/%d)",
                        url, delay, attempt + 1, max_attempts
                    )
                    time.sleep(delay)
                    continue
                    
                if not (200 <= status < 300):
                    # Check if we should retry this status code
                    if _retry_strategy.should_retry(attempt, status, None):
                        delay = _retry_strategy.get_delay(attempt)
                        log.debug(
                            "Retrying %s after %.1fs (status %d, attempt %d/%d)",
                            url, delay, status, attempt + 1, max_attempts
                        )
                        time.sleep(delay)
                        continue
                    else:
                        # Don't retry client errors
                        _circuit_breaker.record_failure(domain, str(status))
                        raise requests.exceptions.HTTPError(
                            f"Bad status {status} for {url}"
                        )
                
                # Success - record in circuit breaker
                _circuit_breaker.record_success(domain)
                break  # success
                
            except (
                ConnectTimeout,
                requests.exceptions.ConnectionError,
                SSLError,
                requests.exceptions.RetryError,
                requests.exceptions.HTTPError,
            ) as err:
                last_exception = err
                
                # Check if we should retry this exception
                should_retry = _retry_strategy.should_retry(
                    attempt, None, err
                )
                
                log.debug(
                    "Network/HTTP error (attempt %d/%d) for %s: %s",
                    attempt + 1, max_attempts, url, err
                )
                
                if should_retry and attempt + 1 < max_attempts:
                    delay = _retry_strategy.get_delay(attempt)
                    log.debug(
                        "Retrying %s after %.1fs (attempt %d/%d)",
                        url, delay, attempt + 1, max_attempts
                    )
                    time.sleep(delay)
                    continue
                
                # Record failure in circuit breaker
                _circuit_breaker.record_failure(domain, "connection_error")
                
                # SSL fallback (final attempt) if insecure allowed
                if (
                    isinstance(err, SSLError)
                    and attempt + 1 >= max_attempts
                    and config.insecure_ssl
                ):
                    try:
                        log.info(
                            "SSL failed, retrying with verify=False: %s", url
                        )
                        response = request_fn(
                            url,
                            allow_redirects=True,
                            timeout=timeout,
                            headers=hdrs,
                            verify=False,
                        )
                        if response and response.ok:
                            _circuit_breaker.record_success(domain)
                            break
                    except Exception as e2:
                        log.warning(
                            "SSL-off retry failed for %s: %s", url, e2
                        )
                # Skip fallbacks if we have a verified domain pattern
                pattern = self.domain_patterns.get(domain)
                if pattern and pattern.verified:
                    log.debug(
                        "Skipping fallbacks for %s - using verified pattern",
                        domain
                    )
                    break  # No fallbacks for verified patterns
                
                # If this is the last attempt, try fallbacks
                if attempt + 1 >= max_attempts:
                    # www-prefix fallback
                    parsed_retry = urlparse(url)
                    host_retry = parsed_retry.netloc
                    if not host_retry.startswith("www."):
                        fallback = urlunparse(
                            parsed_retry._replace(netloc="www." + host_retry)
                        )
                        log.info("Retrying with www-prefix: %s", fallback)
                        try:
                            response = request_fn(
                                fallback,
                                allow_redirects=True,
                                timeout=timeout,
                                headers=hdrs,
                                verify=not config.insecure_ssl,
                            )
                            if response and response.ok:
                                _circuit_breaker.record_success(domain)
                                url = fallback
                                break
                        except Exception as e2:
                            log.warning(
                                "www-prefix retry failed for %s: %s",
                                fallback, e2
                            )
                    
                    # http scheme fallback
                    if parsed_retry.scheme == "https":
                        http_url = urlunparse(
                            parsed_retry._replace(scheme="http")
                        )
                        log.info("Retrying with HTTP: %s", http_url)
                        try:
                            response = request_fn(
                                http_url,
                                allow_redirects=True,
                                timeout=timeout,
                                headers=hdrs,
                            )
                            if response and response.ok:
                                _circuit_breaker.record_success(domain)
                                url = http_url
                                break
                        except Exception as e3:
                            log.warning(
                                "HTTP fallback failed for %s: %s", http_url, e3
                            )
                    
                    # All attempts and fallbacks failed
                    break

        status = response.status_code if response else "no-response"
        self.stats[f"status_{status}"] += 1
        if not response or not response.ok:
            return None

        if not hasattr(_thread_local, "visited_subdomains"):
            _thread_local.visited_subdomains = set()
        _thread_local.visited_subdomains.add(response.url)

        if self.DEBUG and not head_mode:
            log.info("DEBUG-DUMP %s → %d", url, response.status_code)
            self._dump_debug(url, response)

        if not head_mode:
            _thread_local.visited.add(canon)

        if callback and not head_mode:
            try:
                callback(response)
            except Exception as exc:
                log.error("Callback raised for %s: %s", url, exc)

        _session_mgr.prune()
        return response

    def _dump_debug(self, url: str, resp: requests.Response) -> None:
        try:
            p = urlparse(url)
            host = p.netloc or "_"
            path = p.path.strip("/").replace("/", "_") or "index"
            fname = f"{host}_{path}.html"
            with open(os.path.join(self.DEBUG_DIR, fname), "wb") as fp:
                fp.write(resp.content)
            log.debug("Saved debug dump for %s → %s", url, fname)
        except Exception as exc:
            log.warning("Failed to save debug dump for %s: %s", url, exc)

    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive performance statistics including HTTP client stats,
        circuit breaker stats, and performance monitor data.
        """
        stats = {
            'http_client': self.stats.copy(),
            'domain_patterns': len(self.domain_patterns),
            'cached_patterns': sum(
                1 for p in self.domain_patterns.values() if p.verified
            ),
            'circuit_breaker': _circuit_breaker.get_stats(),
            'retry_strategy': {
                'max_retries': _retry_strategy.max_retries,
                'base_delay': _retry_strategy.base_delay,
                'max_delay': _retry_strategy.max_delay,
                'exponential_base': _retry_strategy.exponential_base,
                'jitter_enabled': _retry_strategy.jitter
            },
            'token_buckets': get_bucket_performance_summary()
        }
        
        # Add performance optimizer stats if available
        if PERFORMANCE_OPTIMIZATIONS_AVAILABLE:
            try:
                from scraper.performance_optimizer import get_performance_stats
                optimizer_stats = get_performance_stats()
                stats.update(optimizer_stats)
            except Exception as e:
                log.debug("Failed to get performance optimizer stats: %s", e)
        
        return stats
    
    def clear_performance_cache(self) -> None:
        """Clear performance-related caches."""
        # Clear domain patterns cache
        cleared_patterns = len(self.domain_patterns)
        self.domain_patterns.clear()
        log.info("Cleared %d cached domain patterns", cleared_patterns)
        
        # Reset HTTP client stats
        self.stats = Counter()
        log.info("Reset HTTP client statistics")
        
        # Clear connection pool stats
        _connection_pool_mgr.clear_stats()
        log.info("Cleared connection pool statistics")
        
        # Clear performance optimizer caches if available
        if PERFORMANCE_OPTIMIZATIONS_AVAILABLE:
            try:
                from scraper.performance_optimizer import (
                    cleanup_performance_resources
                )
                cleanup_performance_resources()
                log.info("Cleared performance optimization caches")
            except Exception as e:
                log.debug(
                    "Failed to clear performance optimizer caches: %s", e
                )

    def warm_domain_connection(self, domain: str) -> bool:
        """
        Pre-warm connection to domain for better performance.
        
        Args:
            domain: Domain to warm connection for
            
        Returns:
            True if connection was warmed successfully
        """
        normalized_domain = normalise_domain(domain)
        pattern = self.domain_patterns.get(normalized_domain)
        return _connection_pool_mgr.warm_connection(normalized_domain, pattern)
    
    def get_connection_pool_stats(self) -> Dict[str, Any]:
        """Get comprehensive connection pool statistics."""
        return _connection_pool_mgr.get_pool_stats()
    
    def optimize_connection_pools(self, target_domains: List[str]) -> None:
        """
        Optimize connection pools for a list of target domains.
        
        Args:
            target_domains: List of domains to optimize for
        """
        log.info("Optimizing connection pools for %d domains", len(target_domains))
        
        # Warm connections for high-priority domains
        for domain in target_domains[:10]:  # Limit to top 10 to avoid overhead
            try:
                normalized = normalise_domain(domain)
                if self.warm_domain_connection(normalized):
                    log.debug("✓ Warmed connection to %s", normalized)
            except Exception as e:
                log.debug("Failed to warm connection to %s: %s", domain, e)
        
        # Adjust pool size based on number of target domains
        if len(target_domains) > 20:
            _connection_pool_mgr.update_pool_size(15)  # Increase pool size
        elif len(target_domains) < 5:
            _connection_pool_mgr.update_pool_size(5)   # Decrease for small batches
    
    def get_enhanced_session(self, domain: str) -> requests.Session:
        """
        Get an enhanced session with optimized connection pooling.
        
        Args:
            domain: Domain to create session for
            
        Returns:
            Optimized requests session
        """
        normalized_domain = normalise_domain(domain)
        return _connection_pool_mgr.create_optimized_session(normalized_domain)


def normalise_domain(url: str) -> str:
    """Normalize a domain by removing www prefix and lowering case."""
    try:
        host = (
            urlparse(url).netloc
            if url.startswith(("http://", "https://"))
            else url
        )
        return host.lower().removeprefix("www.")
    except Exception as e:  # pragma: no cover (defensive)
        log.warning("Domain normalization error for %s: %s", url, e)
        return url.lower()


def join_url(base: str, path: str) -> str:
    """Join a base URL and a path, handling relative paths correctly."""
    if path.startswith(("http://", "https://")):
        return path

    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"

    joined_url = urljoin(base, path.lstrip("/"))
    if not validate_url(joined_url):
        log.warning("Invalid joined URL: %s + %s", base, path)
        return ""
    return joined_url

# single, shared instance
 

http_client = HttpClient()

