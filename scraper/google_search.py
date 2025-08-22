import logging
import threading
import time
import hashlib
import json
import pickle
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable, Set
from collections import OrderedDict

import requests

from scraper.config import config, API_KEY, CX_ID

# Initialize logger
log = logging.getLogger(__name__)

# Rate limiting globals
_last_google_ts: float = 0.0
_google_lock = threading.Lock()

# Caching globals
_google_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = threading.Lock()

class EnhancedGoogleSearchCache:
    """Enhanced cache for Google search results with persistence, normalization, and deduplication."""
    
    def __init__(self, ttl_hours: int = 24, max_memory_size: int = 10000):
        # LRU cache for memory with size limit
        self.memory_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self.max_memory_size = max_memory_size
        self.ttl = timedelta(hours=ttl_hours)
        self.lock = threading.Lock()
        
        # Statistics
        self.hit_count = 0
        self.miss_count = 0
        self.persistence_enabled = False
        
        # Query normalization patterns
        self.normalization_patterns = [
            (r'\s+', ' '),  # Multiple spaces to single space
            (r'[^\w\s-]', ''),  # Remove special chars except dash
            (r'\b(inc|llc|ltd|corp|corporation|company|co)\b', ''),  # Remove company suffixes
            (r'\b(the|a|an)\b', ''),  # Remove articles
        ]
        
        # Initialize persistence if possible
        self.cache_file = os.path.join(os.getcwd(), '.google_search_cache.pkl')
        self._init_persistence()
        
        # Query similarity tracking for better normalization
        self.similar_queries: Dict[str, Set[str]] = {}
    
    def _init_persistence(self) -> None:
        """Initialize persistent cache if possible."""
        try:
            if os.path.exists(self.cache_file):
                self._load_from_disk()
            self.persistence_enabled = True
            log.debug("Google search cache persistence enabled")
        except Exception as e:
            log.warning("Failed to initialize Google search cache persistence: %s", e)
            self.persistence_enabled = False
    
    def _load_from_disk(self) -> None:
        """Load cache from disk."""
        try:
            with open(self.cache_file, 'rb') as f:
                data = pickle.load(f)
                
            # Filter out expired entries
            current_time = datetime.now()
            valid_entries = {
                k: v for k, v in data.items() 
                if current_time - v['timestamp'] < self.ttl
            }
            
            # Load into memory cache (respecting size limit)
            self.memory_cache.clear()
            for k, v in list(valid_entries.items())[:self.max_memory_size]:
                self.memory_cache[k] = v
                
            log.info("Loaded %d cached Google search entries from disk", len(self.memory_cache))
            
        except Exception as e:
            log.warning("Failed to load Google search cache from disk: %s", e)
    
    def _save_to_disk(self) -> None:
        """Save cache to disk."""
        if not self.persistence_enabled:
            return
            
        try:
            # Save current memory cache
            with open(self.cache_file, 'wb') as f:
                pickle.dump(dict(self.memory_cache), f)
            log.debug("Saved Google search cache to disk (%d entries)", len(self.memory_cache))
        except Exception as e:
            log.warning("Failed to save Google search cache to disk: %s", e)
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query for better cache hits and deduplication."""
        normalized = query.strip().lower()
        
        # Apply normalization patterns
        for pattern, replacement in self.normalization_patterns:
            normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
        
        # Remove extra spaces and trim
        normalized = ' '.join(normalized.split())
        return normalized
    
    def _generate_key(self, query: str) -> str:
        """Generate cache key for query with enhanced normalization."""
        normalized = self._normalize_query(query)
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]  # Shorter key
    
    def _update_lru(self, key: str) -> None:
        """Update LRU order for key."""
        if key in self.memory_cache:
            self.memory_cache.move_to_end(key)
    
    def _evict_if_needed(self) -> None:
        """Evict oldest entries if cache is full."""
        while len(self.memory_cache) > self.max_memory_size:
            oldest_key, _ = self.memory_cache.popitem(last=False)
            log.debug("Evicted oldest cache entry: %s", oldest_key)
    
    def get(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached search results if available and not expired."""
        key = self._generate_key(query)
        
        with self.lock:
            if key in self.memory_cache:
                entry = self.memory_cache[key]
                if datetime.now() - entry['timestamp'] < self.ttl:
                    self.hit_count += 1
                    self._update_lru(key)
                    log.debug("Google cache HIT for query: %s", query)
                    return entry['results'].copy() if entry['results'] else []
                else:
                    # Expired, remove from cache
                    del self.memory_cache[key]
                    log.debug("Google cache EXPIRED for query: %s", query)
            
            self.miss_count += 1
            log.debug("Google cache MISS for query: %s", query)
            return None
    
    def put(self, query: str, results: List[Dict[str, Any]]) -> None:
        """Cache search results with deduplication and normalization."""
        key = self._generate_key(query)
        normalized_query = self._normalize_query(query)
        
        with self.lock:
            # Track similar queries for better normalization learning
            if normalized_query in self.similar_queries:
                self.similar_queries[normalized_query].add(query)
            else:
                self.similar_queries[normalized_query] = {query}
            
            # Store in memory cache
            self.memory_cache[key] = {
                'results': [r.copy() for r in results] if results else [],
                'timestamp': datetime.now(),
                'original_query': query,
                'normalized_query': normalized_query
            }
            
            # Update LRU and evict if needed
            self._update_lru(key)
            self._evict_if_needed()
            
            log.debug("Cached Google results for query: %s (%d results)", query, len(results))
            
            # Periodic save to disk
            if len(self.memory_cache) % 100 == 0:  # Every 100 entries
                self._save_to_disk()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_rate = (self.hit_count / total_requests * 100) if total_requests > 0 else 0
        
        with self.lock:
            stats = {
                'hits': self.hit_count,
                'misses': self.miss_count,
                'hit_rate_percent': round(hit_rate, 2),
                'memory_cache_size': len(self.memory_cache),
                'max_memory_size': self.max_memory_size,
                'total_requests': total_requests,
                'persistence_enabled': self.persistence_enabled,
                'similar_query_groups': len(self.similar_queries),
                'ttl_hours': self.ttl.total_seconds() / 3600
            }
            
            # Memory usage estimation
            estimated_memory_mb = len(self.memory_cache) * 0.5  # ~0.5KB per entry
            stats['estimated_memory_usage_mb'] = round(estimated_memory_mb, 2)
        
        return stats
    
    def clear(self) -> None:
        """Clear all cached entries."""
        with self.lock:
            self.memory_cache.clear()
            self.similar_queries.clear()
            
        # Clear disk cache too
        if self.persistence_enabled:
            try:
                if os.path.exists(self.cache_file):
                    os.remove(self.cache_file)
                log.info("Cleared Google search cache (memory and disk)")
            except Exception as e:
                log.warning("Failed to clear disk cache: %s", e)
        else:
            log.info("Cleared Google search cache (memory only)")
    
    def force_save(self) -> None:
        """Force save current cache to disk."""
        self._save_to_disk()
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """Get suggestions for similar cached queries."""
        normalized = self._normalize_query(query)
        return list(self.similar_queries.get(normalized, set()))

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

# Global cache instance - using enhanced cache for better performance
_search_cache = EnhancedGoogleSearchCache(ttl_hours=24, max_memory_size=10000)

class GoogleApiError(Exception):
    """Exception raised for Google API errors."""
    pass

class RateLimitExceededError(GoogleApiError):
    """Exception raised when Google API rate limit is exceeded."""
    pass

class GoogleBatchProcessor:
    """Handles batching and deduplication of Google search requests."""
    
    def __init__(self, batch_window_seconds: float = 2.0, max_batch_size: int = 10):
        self.batch_window = batch_window_seconds
        self.max_batch_size = max_batch_size
        self.lock = threading.Lock()
        
        # Pending requests queue
        self.pending_requests: Dict[str, List[Dict[str, Any]]] = {}
        self.batch_timer: Optional[threading.Timer] = None
        
        # Deduplication tracking
        self.active_requests: Dict[str, threading.Event] = {}
        self.request_results: Dict[str, Any] = {}
    
    def add_request(self, query: str, callback: Callable[[List[Dict[str, Any]]], None], 
                   timeout: float = 30.0) -> bool:
        """
        Add a request to the batch queue.
        
        Args:
            query: Search query
            callback: Callback to call with results
            timeout: Request timeout
            
        Returns:
            True if request was added, False if duplicate is already processing
        """
        # Normalize query for deduplication
        normalized_query = _search_cache._normalize_query(query) if hasattr(_search_cache, '_normalize_query') else query.strip().lower()
        
        with self.lock:
            # Check if identical request is already being processed
            if normalized_query in self.active_requests:
                # Wait for existing request to complete
                event = self.active_requests[normalized_query]
                
                def wait_for_result():
                    if event.wait(timeout):
                        result = self.request_results.get(normalized_query, [])
                        callback(result)
                    else:
                        callback([])  # Timeout
                
                # Start waiting thread
                wait_thread = threading.Thread(target=wait_for_result, daemon=True)
                wait_thread.start()
                return True
            
            # Add to pending batch
            if normalized_query not in self.pending_requests:
                self.pending_requests[normalized_query] = []
            
            self.pending_requests[normalized_query].append({
                'original_query': query,
                'callback': callback,
                'timestamp': time.time()
            })
            
            # Start batch timer if not already running
            if self.batch_timer is None or not self.batch_timer.is_alive():
                self.batch_timer = threading.Timer(self.batch_window, self._process_batch)
                self.batch_timer.daemon = True
                self.batch_timer.start()
            
            # Process immediately if batch is full
            if len(self.pending_requests) >= self.max_batch_size:
                if self.batch_timer:
                    self.batch_timer.cancel()
                self._process_batch()
            
            return True
    
    def _process_batch(self) -> None:
        """Process the current batch of requests."""
        with self.lock:
            if not self.pending_requests:
                return
                
            # Take current batch
            current_batch = dict(self.pending_requests)
            self.pending_requests.clear()
            self.batch_timer = None
            
            # Create events for active requests
            for normalized_query in current_batch:
                self.active_requests[normalized_query] = threading.Event()
        
        log.info("Processing batch of %d unique Google search queries", len(current_batch))
        
        # Process each unique query
        for normalized_query, request_list in current_batch.items():
            self._process_single_query(normalized_query, request_list)
    
    def _process_single_query(self, normalized_query: str, request_list: List[Dict[str, Any]]) -> None:
        """Process a single deduplicated query."""
        # Use the first original query for the actual search
        first_request = request_list[0]
        search_query = first_request['original_query']
        
        try:
            # Perform the actual search
            from scraper.google_search import google_client
            results = google_client._direct_search(search_query)
            
            # Store result for any waiting threads
            self.request_results[normalized_query] = results
            
            # Call all callbacks
            for request_info in request_list:
                try:
                    request_info['callback'](results)
                except Exception as e:
                    log.error("Error in batch callback for query '%s': %s", search_query, e)
            
        except Exception as e:
            log.error("Error processing batch query '%s': %s", search_query, e)
            # Call callbacks with empty results on error
            for request_info in request_list:
                try:
                    request_info['callback']([])
                except Exception:
                    pass
        
        finally:
            # Clean up tracking
            with self.lock:
                event = self.active_requests.pop(normalized_query, None)
                self.request_results.pop(normalized_query, None)
                if event:
                    event.set()

# Global batch processor
_batch_processor = GoogleBatchProcessor(batch_window_seconds=2.0, max_batch_size=10)

class GoogleSearchClient:
    """Enhanced Google search client with improved error handling and rate limiting."""
    
    def __init__(self):
        """Initialize the Google Search client with API credentials."""
        self._service = None
        self._init_lock = threading.Lock()
        self.enable_batching = True  # Feature flag
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
                # Use centralized session management for better reuse
                from scraper.http_client import _session_mgr
                session = _session_mgr.session("googleapis.com")
                
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
    
    def _direct_search(
        self,
        query: str,
        num_results: int = 10,
        site_restrict: Optional[str] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Direct search method used internally and by batch processor.
        Does not use batching or deduplication.
        """
        return self._perform_search(query, num_results, site_restrict, callback)
    
    def search(
        self,
        query: str,
        num_results: int = 10,
        site_restrict: Optional[str] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform a Google search with enhanced error handling, rate limiting, caching, and batching.
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
            if callback and cached_results:
                try:
                    callback(cached_results)
                except Exception as cb_e:
                    log.error("Callback error: %s", cb_e)
            return cached_results
        
        # Use batching if enabled and no callback provided
        if self.enable_batching and callback is None:
            return self._search_with_batching(full_q, num_results)
        else:
            # Direct search for callbacks or when batching is disabled
            return self._perform_search(full_q, num_results, site_restrict, callback)
    
    def _search_with_batching(self, query: str, num_results: int) -> List[Dict[str, Any]]:
        """
        Perform search using batch processor for deduplication and efficiency.
        """
        result_container = {'results': None, 'event': threading.Event()}
        
        def batch_callback(results: List[Dict[str, Any]]):
            result_container['results'] = results
            result_container['event'].set()
        
        # Add to batch
        _batch_processor.add_request(query, batch_callback)
        
        # Wait for result with timeout
        if result_container['event'].wait(timeout=60):  # 60 second timeout
            return result_container['results'] or []
        else:
            log.warning("Batch search timeout for query: %s", query)
            return []
    
    def _perform_search(
        self,
        full_q: str,
        num_results: int = 10,
        site_restrict: Optional[str] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform the actual Google search API call.
        """
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
                        full_q, attempt+1, config.google_max_retries, wait, e
                    )
                    time.sleep(wait)
                    continue
                
                log.error("Google search network error after retries: %s", e)
                raise GoogleApiError(e)
                
            except Exception as e:
                log.error("Unexpected error in Google search: %s", e)
                raise GoogleApiError(e)

        msg = f"Google search failed for '{full_q}' after {config.google_max_retries} retries"
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
        """Get comprehensive cache statistics."""
        return _search_cache.get_stats()
    
    def clear_cache(self) -> None:
        """Clear search cache."""
        _search_cache.clear()
    
    def force_cache_save(self) -> None:
        """Force save cache to disk (if persistence enabled)."""
        if hasattr(_search_cache, 'force_save'):
            _search_cache.force_save()
    
    def get_query_suggestions(self, query: str) -> List[str]:
        """Get suggestions for similar cached queries."""
        if hasattr(_search_cache, 'get_query_suggestions'):
            return _search_cache.get_query_suggestions(query)
        return []
    
    def optimize_cache_performance(self) -> Dict[str, Any]:
        """
        Analyze and optimize cache performance.
        
        Returns:
            Dictionary with optimization suggestions and current performance metrics
        """
        stats = self.get_cache_stats()
        optimization_report = {
            'current_performance': stats,
            'recommendations': []
        }
        
        # Analyze hit rate and suggest improvements
        hit_rate = stats.get('hit_rate_percent', 0)
        if hit_rate < 50:
            optimization_report['recommendations'].append({
                'issue': 'Low cache hit rate',
                'suggestion': 'Consider increasing TTL or improving query normalization',
                'current_hit_rate': hit_rate,
                'target_hit_rate': '>70%'
            })
        
        # Analyze memory usage
        memory_usage = stats.get('estimated_memory_usage_mb', 0)
        if memory_usage > 50:  # 50MB threshold
            optimization_report['recommendations'].append({
                'issue': 'High memory usage',
                'suggestion': 'Consider reducing max_memory_size or forcing more frequent saves',
                'current_memory_mb': memory_usage,
                'max_recommended_mb': 50
            })
        
        # Analyze cache size vs requests
        cache_size = stats.get('memory_cache_size', 0)
        total_requests = stats.get('total_requests', 0)
        if total_requests > 0 and cache_size / total_requests > 0.8:
            optimization_report['recommendations'].append({
                'issue': 'Cache growing too fast',
                'suggestion': 'Improve query normalization to increase cache reuse',
                'cache_to_request_ratio': cache_size / total_requests
            })
        
        return optimization_report
    
    def set_batching_enabled(self, enabled: bool) -> None:
        """Enable or disable request batching."""
        self.enable_batching = enabled
        log.info("Google search batching %s", "enabled" if enabled else "disabled")
    
    def get_batch_stats(self) -> Dict[str, Any]:
        """Get statistics about request batching."""
        with _batch_processor.lock:
            return {
                'batching_enabled': self.enable_batching,
                'pending_requests': len(_batch_processor.pending_requests),
                'active_requests': len(_batch_processor.active_requests),
                'batch_window_seconds': _batch_processor.batch_window,
                'max_batch_size': _batch_processor.max_batch_size,
                'batch_timer_active': _batch_processor.batch_timer is not None and _batch_processor.batch_timer.is_alive()
            }
    
    def flush_pending_batches(self) -> None:
        """Force process any pending batch requests immediately."""
        if _batch_processor.batch_timer and _batch_processor.batch_timer.is_alive():
            _batch_processor.batch_timer.cancel()
        _batch_processor._process_batch()
        log.info("Flushed all pending Google search batches")

# global instance
google_client = GoogleSearchClient()
