"""
Performance optimization module for THE Email Scraper.

This module implements critical performance improvements including:
- Enhanced connection pooling
- Request batching and optimization  
- Intelligent caching strategies
- Resource usage optimization
"""

import time
import logging
from collections import defaultdict, deque
from typing import Dict, List, Set, Optional, Tuple, Any
from urllib.parse import urlparse
import threading
from datetime import datetime, timedelta
import json
import hashlib

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.poolmanager import PoolManager

from scraper.config import config

log = logging.getLogger(__name__)

class ConnectionPoolOptimizer:
    """Optimized connection pooling for better HTTP performance."""
    
    def __init__(self):
        self.domain_sessions: Dict[str, requests.Session] = {}
        self.session_lock = threading.Lock()
        self.connection_stats = defaultdict(int)
        
    def get_optimized_session(self, domain: str) -> requests.Session:
        """Get optimized session for a specific domain."""
        with self.session_lock:
            if domain not in self.domain_sessions:
                session = requests.Session()
                
                # Configure connection pooling
                adapter = HTTPAdapter(
                    pool_connections=10,  # Number of connection pools
                    pool_maxsize=20,      # Max connections per pool
                    max_retries=Retry(
                        total=3,
                        backoff_factor=0.5,
                        status_forcelist=[429, 500, 502, 503, 504]
                    ),
                    pool_block=False
                )
                
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                
                # Configure timeouts and headers
                session.timeout = (10, 30)  # (connect, read)
                session.headers.update({
                    'Connection': 'keep-alive',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                })
                
                self.domain_sessions[domain] = session
                log.debug(f"Created optimized session for {domain}")
                
            self.connection_stats[domain] += 1
            return self.domain_sessions[domain]
    
    def get_stats(self) -> Dict[str, int]:
        """Get connection usage statistics."""
        return dict(self.connection_stats)

class ResponseCache:
    """Intelligent response caching system."""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        self.cache: Dict[str, Tuple[Any, datetime]] = {}
        self.max_size = max_size
        self.ttl = timedelta(seconds=ttl_seconds)
        self.cache_lock = threading.Lock()
        self.hit_count = 0
        self.miss_count = 0
        
    def _generate_key(self, url: str, method: str = 'GET', data: Any = None) -> str:
        """Generate cache key for request."""
        key_data = f"{method}:{url}"
        if data:
            key_data += f":{json.dumps(data, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, url: str, method: str = 'GET', data: Any = None) -> Optional[Any]:
        """Get cached response if available and not expired."""
        key = self._generate_key(url, method, data)
        
        with self.cache_lock:
            if key in self.cache:
                response, timestamp = self.cache[key]
                if datetime.now() - timestamp < self.ttl:
                    self.hit_count += 1
                    log.debug(f"Cache HIT for {url}")
                    return response
                else:
                    # Expired, remove from cache
                    del self.cache[key]
            
            self.miss_count += 1
            log.debug(f"Cache MISS for {url}")
            return None
    
    def put(self, url: str, response: Any, method: str = 'GET', data: Any = None) -> None:
        """Cache response."""
        key = self._generate_key(url, method, data)
        
        with self.cache_lock:
            # Implement LRU eviction if cache is full
            if len(self.cache) >= self.max_size:
                # Remove oldest entry
                oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
                del self.cache[oldest_key]
                log.debug(f"Evicted oldest cache entry: {oldest_key}")
            
            self.cache[key] = (response, datetime.now())
            log.debug(f"Cached response for {url}")
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        total_requests = self.hit_count + self.miss_count
        hit_rate = (self.hit_count / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hits': self.hit_count,
            'misses': self.miss_count,
            'hit_rate_percent': round(hit_rate, 2),
            'cache_size': len(self.cache)
        }

class BatchRequestProcessor:
    """Process requests in batches for better efficiency."""
    
    def __init__(self, batch_size: int = 10, batch_timeout: float = 2.0):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_requests: List[Dict] = []
        self.batch_lock = threading.Lock()
        self.batch_timer: Optional[threading.Timer] = None
        
    def add_request(self, url: str, callback, **kwargs) -> None:
        """Add request to batch queue."""
        request_data = {
            'url': url,
            'callback': callback,
            'kwargs': kwargs,
            'timestamp': time.time()
        }
        
        with self.batch_lock:
            self.pending_requests.append(request_data)
            
            # Process batch if full
            if len(self.pending_requests) >= self.batch_size:
                self._process_batch()
            elif not self.batch_timer:
                # Start timer for timeout-based processing
                self.batch_timer = threading.Timer(self.batch_timeout, self._process_batch)
                self.batch_timer.start()
    
    def _process_batch(self) -> None:
        """Process current batch of requests."""
        with self.batch_lock:
            if not self.pending_requests:
                return
                
            batch = self.pending_requests.copy()
            self.pending_requests.clear()
            
            if self.batch_timer:
                self.batch_timer.cancel()
                self.batch_timer = None
        
        log.debug(f"Processing batch of {len(batch)} requests")
        
        # Group by domain for connection reuse
        domain_groups = defaultdict(list)
        for request in batch:
            domain = urlparse(request['url']).netloc
            domain_groups[domain].append(request)
        
        # Process each domain group
        for domain, requests_list in domain_groups.items():
            self._process_domain_batch(domain, requests_list)
    
    def _process_domain_batch(self, domain: str, requests_list: List[Dict]) -> None:
        """Process batch of requests for a single domain."""
        session = connection_pool.get_optimized_session(domain)
        
        for request in requests_list:
            try:
                # Check cache first
                cached_response = response_cache.get(request['url'])
                if cached_response:
                    request['callback'](cached_response)
                    continue
                
                # Make actual request
                response = session.get(request['url'], **request['kwargs'])
                
                # Cache successful responses
                if response.status_code == 200:
                    response_cache.put(request['url'], response)
                
                request['callback'](response)
                
            except Exception as e:
                log.error(f"Batch request failed for {request['url']}: {e}")
                request['callback'](None)

class ResourceMonitor:
    """Monitor and optimize resource usage."""
    
    def __init__(self):
        self.request_times: deque = deque(maxlen=100)
        self.memory_samples: deque = deque(maxlen=50)
        self.start_time = time.time()
        
    def record_request_time(self, duration: float) -> None:
        """Record request processing time."""
        self.request_times.append(duration)
    
    def get_average_request_time(self) -> float:
        """Get average request processing time."""
        if not self.request_times:
            return 0.0
        return sum(self.request_times) / len(self.request_times)
    
    def get_requests_per_second(self) -> float:
        """Calculate current requests per second."""
        if not self.request_times:
            return 0.0
        
        recent_requests = len([t for t in self.request_times if time.time() - t < 60])
        return recent_requests / 60.0
    
    def suggest_worker_adjustment(self) -> Optional[int]:
        """Suggest optimal worker count based on performance."""
        avg_time = self.get_average_request_time()
        current_workers = config.max_workers
        
        if avg_time > 5.0:  # Slow requests
            # Reduce workers to avoid overwhelming servers
            return max(1, current_workers - 1)
        elif avg_time < 1.0 and len(self.request_times) > 50:  # Fast requests
            # Increase workers for better throughput
            return min(16, current_workers + 2)
        
        return None
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Generate performance report."""
        uptime = time.time() - self.start_time
        
        return {
            'uptime_seconds': round(uptime, 2),
            'average_request_time': round(self.get_average_request_time(), 3),
            'requests_per_second': round(self.get_requests_per_second(), 2),
            'total_requests_tracked': len(self.request_times),
            'connection_stats': connection_pool.get_stats(),
            'cache_stats': response_cache.get_stats(),
            'suggested_workers': self.suggest_worker_adjustment()
        }

# Global instances
connection_pool = ConnectionPoolOptimizer()
response_cache = ResponseCache()
batch_processor = BatchRequestProcessor()
resource_monitor = ResourceMonitor()

def get_performance_report() -> Dict[str, Any]:
    """Get comprehensive performance report."""
    return resource_monitor.get_performance_report()

def optimize_request(url: str, session: Optional[requests.Session] = None, **kwargs) -> Optional[requests.Response]:
    """
    Make an optimized HTTP request with caching and connection pooling.
    
    Args:
        url: URL to request
        session: Optional session to use
        **kwargs: Additional request parameters
        
    Returns:
        Response object or None if failed
    """
    start_time = time.time()
    
    try:
        # Check cache first
        cached_response = response_cache.get(url)
        if cached_response:
            return cached_response
        
        # Use optimized session
        domain = urlparse(url).netloc
        if not session:
            session = connection_pool.get_optimized_session(domain)
        
        # Make request
        response = session.get(url, **kwargs)
        
        # Cache successful responses
        if response.status_code == 200:
            response_cache.put(url, response)
        
        return response
        
    except Exception as e:
        log.error(f"Optimized request failed for {url}: {e}")
        return None
        
    finally:
        # Record timing
        duration = time.time() - start_time
        resource_monitor.record_request_time(duration)