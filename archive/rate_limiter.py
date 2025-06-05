"""
Rate limiting module for controlling request rates.

This module provides functionality for rate limiting requests to external services,
with support for domain-specific limits and backoff strategies.
"""

import logging
import threading
import time
from typing import Dict, Optional, Callable

# Initialize logger
log = logging.getLogger(__name__)

class RateLimiter:
    """
    Rate limiter for controlling request rates to external services.
    
    This class provides:
    - Global and domain-specific rate limits
    - Automatic waiting to respect limits
    - Exponential backoff for failures
    """
    
    def __init__(self, default_rate: float = 1.0):
        """
        Initialize the rate limiter.
        
        Args:
            default_rate: Default requests per second
        """
        self.default_rate = default_rate
        
        # Last request timestamps
        self.last_request_time: Dict[str, float] = {}
        
        # Domain-specific rates (requests per second)
        self.domain_rates: Dict[str, float] = {}
        
        # Thread safety
        self.lock = threading.RLock()
    
    def set_rate(self, domain: str, rate: float) -> None:
        """
        Set rate limit for a specific domain.
        
        Args:
            domain: Domain to set rate for
            rate: Requests per second
        """
        with self.lock:
            self.domain_rates[domain] = max(0.01, rate)  # Minimum 0.01 req/sec
    
    def wait(self, domain: Optional[str] = None) -> None:
        """
        Wait to respect rate limits.
        
        Args:
            domain: Optional domain for domain-specific rate
        """
        with self.lock:
            # Get rate for this domain
            rate = self.domain_rates.get(domain, self.default_rate) if domain else self.default_rate
            
            # Calculate minimum interval between requests
            interval = 1.0 / rate
            
            # Get last request time
            key = domain or "_global_"
            last_time = self.last_request_time.get(key, 0)
            
            # Calculate time to wait
            now = time.time()
            wait_time = max(0, last_time + interval - now)
            
            if wait_time > 0:
                # Release lock during wait
                self.lock.release()
                try:
                    time.sleep(wait_time)
                finally:
                    # Reacquire lock
                    self.lock.acquire()
            
            # Update last request time
            self.last_request_time[key] = time.time()
    
    def execute_with_rate_limit(self, func: Callable, domain: Optional[str] = None, 
                              retry_count: int = 1, backoff_factor: float = 2.0):
        """
        Execute a function with rate limiting and retries.
        
        Args:
            func: Function to execute
            domain: Optional domain for domain-specific rate
            retry_count: Number of retries on failure
            backoff_factor: Backoff factor for retries
            
        Returns:
            Result of the function
        """
        last_error = None
        
        for attempt in range(retry_count + 1):
            try:
                # Wait for rate limit
                self.wait(domain)
                
                # Execute function
                return func()
                
            except Exception as e:
                last_error = e
                
                # Log error
                log.warning("Error in rate-limited function (attempt %d/%d): %s", 
                           attempt + 1, retry_count + 1, e)
                
                # Stop if this was the last attempt
                if attempt >= retry_count:
                    break
                    
                # Calculate backoff time
                backoff_time = backoff_factor ** attempt
                
                # Log and wait
                log.info("Backing off for %.1f seconds before retry", backoff_time)
                time.sleep(backoff_time)
        
        # If we get here, all attempts failed
        if last_error:
            raise last_error
        
        # This should never happen
        raise RuntimeError("Unexpected error in rate-limited function")

# Create a global rate limiter instance
rate_limiter = RateLimiter()
