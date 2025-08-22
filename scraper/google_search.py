import logging
import threading
import time
import hashlib
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scraper.config import config, API_KEY, CX_ID

# Initialize logger
log = logging.getLogger(__name__)

# Rate limiting globals
_last_google_ts: float = 0.0
_google_lock = threading.Lock()

# Caching globals
_google_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = threading.Lock()

class GoogleSearchCache:
    """Cache for Google search results with TTL support."""
    
    def __init__(self, ttl_hours: int = 24):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.ttl = timedelta(hours=ttl_hours)
        self.lock = threading.Lock()
        self.hit_count = 0
        self.miss_count = 0
    
    def _generate_key(self, query: str) -> str:
        """Generate cache key for query."""
        # Normalize query for better cache hits
        normalized = query.strip().lower()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def get(self, query: str) -> Optional[List[str]]:
        """Get cached search results if available and not expired."""
        key = self._generate_key(query)
        
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                if datetime.now() - entry['timestamp'] < self.ttl:
                    self.hit_count += 1
                    log.debug(f"Google cache HIT for query: {query}")
                    return entry['results']
                else:
                    # Expired, remove from cache
                    del self.cache[key]
                    log.debug(f"Google cache EXPIRED for query: {query}")
            
            self.miss_count += 1
            log.debug(f"Google cache MISS for query: {query}")
            return None
    
    def put(self, query: str, results: List[str]) -> None:
        """Cache search results."""
        key = self._generate_key(query)
        
        with self.lock:
            self.cache[key] = {
                'results': results.copy(),
                'timestamp': datetime.now()
            }
            log.debug(f"Cached Google results for query: {query} ({len(results)} results)")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_rate = (self.hit_count / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hits': self.hit_count,
            'misses': self.miss_count,
            'hit_rate_percent': round(hit_rate, 2),
            'cache_size': len(self.cache),
            'total_requests': total_requests
        }
    
    def clear(self) -> None:
        """Clear all cached entries."""
        with self.lock:
            self.cache.clear()
            log.info("Google search cache cleared")

# Global cache instance
_search_cache = GoogleSearchCache(ttl_hours=24)

class GoogleApiError(Exception):
    """Exception raised for Google API errors."""
    pass

class RateLimitExceededError(GoogleApiError):
    """Exception raised when Google API rate limit is exceeded."""
    pass

class GoogleSearchClient:
    """Enhanced Google search client with improved error handling and rate limiting."""
    
    def __init__(self):
        """Initialize the Google Search client with API credentials."""
        self._service = None
        self._init_lock = threading.Lock()
        # Initialize the service
        self._initialize_service()
    
    def _initialize_service(self) -> None:
        """
        Initialize the requests session for Google API calls.
        """
        with self._init_lock:
            if self._service is not None:
                return
            try:
                # Create a requests session with proper retry configuration
                session = requests.Session()
                
                # Configure retry strategy
                retry_strategy = Retry(
                    total=3,
                    status_forcelist=[429, 500, 502, 503, 504],
                    backoff_factor=1
                )
                
                adapter = HTTPAdapter(max_retries=retry_strategy)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                
                # Configure SSL verification
                session.verify = not config.insecure_ssl
                
                # Set reasonable timeout
                session.timeout = (10, 30)  # (connect, read)
                
                self._service = session
                log.info("Google API requests session initialized")
            except Exception as e:
                msg = f"Failed to initialize Google API session: {e}"
                log.error(msg)
                raise GoogleApiError(msg)
    
    def _respect_rate(self) -> None:
        """
        Ensure Google API rate limits are respected with improved lock granularity.
        """
        global _last_google_ts
        # Compute wait time under lock, then release lock for sleeping
        with _google_lock:
            now = time.time()
            elapsed = now - _last_google_ts
            wait = config.google_safe_interval - elapsed
        if wait > 0:
            log.debug("Rate limiting: waiting %.2f seconds", wait)
            time.sleep(wait)
        # Update timestamp under lock
        with _google_lock:
            _last_google_ts = time.time()
    
    def search(
        self,
        query: str,
        num_results: int = 10,
        site_restrict: Optional[str] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform a Google search with enhanced error handling, rate limiting, and caching.
        """
        if not query:
            log.warning("Empty search query")
            return []
        if num_results > 10:
            log.warning("Google API limits results to 10, adjusting num_results")
            num_results = 10
        
        full_q = f"{query} {site_restrict}" if site_restrict else query
        
        # Check cache first
        cached_results = _search_cache.get(full_q)
        if cached_results is not None:
            log.debug("Using cached results for: %s", full_q)
            return cached_results
        
        log.debug("Searching Google API for: %s", full_q)
        if self._service is None:
            self._initialize_service()

        backoff = 1
        for attempt in range(config.google_max_retries):
            try:
                self._respect_rate()
                
                # Make direct API call using requests
                params = {
                    'q': full_q,
                    'cx': CX_ID,
                    'key': API_KEY,
                    'num': num_results,
                    'alt': 'json'
                }
                
                response = self._service.get(
                    'https://customsearch.googleapis.com/customsearch/v1',
                    params=params,
                    timeout=(10, 30)
                )
                
                # Handle HTTP errors
                if response.status_code in (403, 429):
                    backoff = backoff * 2
                    log.warning(
                        "Google quota %s – sleeping %ds (attempt %d/%d)",
                        response.status_code, backoff, attempt+1, config.google_max_retries
                    )
                    time.sleep(backoff)
                    continue
                
                response.raise_for_status()
                
                # Parse JSON response
                resp_data = response.json()
                items = resp_data.get("items", [])
                
                if callback and items:
                    try:
                        callback(items)
                    except Exception as cb_e:
                        log.error("Callback error: %s", cb_e)
                
                # Cache successful results
                _search_cache.put(full_q, items)
                
                return items

            except requests.exceptions.HTTPError as he:
                status = he.response.status_code if he.response else None
                # quota exceeded
                if status in (403, 429):
                    backoff = backoff * 2
                    log.warning(
                        "Google quota %s – sleeping %ds (attempt %d/%d)",
                        status, backoff, attempt+1, config.google_max_retries
                    )
                    time.sleep(backoff)
                    continue
                log.error("Google API HTTP error (status %s): %s", status, he)
                raise GoogleApiError(he)

            except (requests.exceptions.ConnectionError, 
                    requests.exceptions.Timeout,
                    requests.exceptions.SSLError) as e:
                # Network, timeout, or SSL errors - retry
                if attempt < config.google_max_retries - 1:
                    wait = 2 ** attempt
                    log.warning(
                        "Google search network error on '%s' (attempt %d/%d), retrying in %ds: %s",
                        query, attempt+1, config.google_max_retries, wait, e
                    )
                    time.sleep(wait)
                    continue
                
                log.error("Google search network error after retries: %s", e)
                raise GoogleApiError(e)
                
            except Exception as e:
                log.error("Unexpected error in Google search: %s", e)
                raise GoogleApiError(e)


        msg = f"Google search failed for '{query}' after {config.google_max_retries} retries"
        log.error(msg)
        raise RateLimitExceededError(msg)
    
    def search_with_fallback(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """
        Perform a Google search with fallback to empty results if all retries fail.
        """
        try:
            return self.search(query, num_results)
        except (GoogleApiError, RateLimitExceededError) as e:
            log.error("Search failed with fallback: %s", e)
            return []
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return _search_cache.get_stats()
    
    def clear_cache(self) -> None:
        """Clear search cache."""
        _search_cache.clear()

# global instance
google_client = GoogleSearchClient()
