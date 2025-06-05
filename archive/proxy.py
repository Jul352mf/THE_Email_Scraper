"""
Proxy support module for HTTP requests.

This module provides functionality for using proxies with HTTP requests,
including proxy rotation and validation.
"""

import logging
import random
import threading
import time
from typing import List, Dict, Any, Optional, Set, Tuple

import requests

from scraper.config import config
from scraper.http import http_client

# Initialize logger
log = logging.getLogger(__name__)

class ProxyError(Exception):
    """Exception raised for proxy errors."""
    pass

class Proxy:
    """
    Proxy configuration with metadata and statistics.
    """
    
    def __init__(self, url: str, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize the proxy.
        
        Args:
            url: Proxy URL (e.g., "http://proxy.example.com:8080")
            username: Optional proxy username
            password: Optional proxy password
        """
        self.url = url
        self.username = username
        self.password = password
        
        # Statistics
        self.success_count = 0
        self.failure_count = 0
        self.last_used = 0.0
        self.last_success = 0.0
        self.last_failure = 0.0
        self.banned_until = 0.0
        
        # Parse protocol
        self.protocol = url.split("://")[0] if "://" in url else "http"
    
    def get_proxy_dict(self) -> Dict[str, str]:
        """
        Get proxy dictionary for requests.
        
        Returns:
            Proxy dictionary for requests
        """
        proxy_url = self.url
        
        # Add authentication if provided
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
            if "://" in proxy_url:
                protocol, rest = proxy_url.split("://", 1)
                proxy_url = f"{protocol}://{auth}{rest}"
            else:
                proxy_url = f"http://{auth}{proxy_url}"
        
        return {self.protocol: proxy_url}
    
    def is_available(self) -> bool:
        """
        Check if proxy is available (not banned).
        
        Returns:
            True if proxy is available, False otherwise
        """
        return time.time() > self.banned_until
    
    def mark_success(self) -> None:
        """Mark proxy as successful."""
        self.success_count += 1
        self.last_used = time.time()
        self.last_success = self.last_used
    
    def mark_failure(self, ban_duration: float = 300.0) -> None:
        """
        Mark proxy as failed.
        
        Args:
            ban_duration: Duration in seconds to ban the proxy
        """
        self.failure_count += 1
        self.last_used = time.time()
        self.last_failure = self.last_used
        self.banned_until = self.last_used + ban_duration
    
    def get_success_rate(self) -> float:
        """
        Get proxy success rate.
        
        Returns:
            Success rate (0.0-1.0)
        """
        total = self.success_count + self.failure_count
        return self.success_count / total if total > 0 else 0.0
    
    def __str__(self) -> str:
        """String representation of proxy."""
        return f"Proxy({self.url}, success_rate={self.get_success_rate():.2f})"

class ProxyManager:
    """
    Proxy manager for handling multiple proxies with rotation and validation.
    
    This class provides:
    - Proxy rotation based on success rate
    - Proxy validation and testing
    - Thread-safe operations
    """
    
    def __init__(self):
        """Initialize the proxy manager."""
        # Proxies
        self.proxies: List[Proxy] = []
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Default cooldown between proxy uses (seconds)
        self.default_cooldown = 1.0
        
        # Test URL for proxy validation
        self.test_url = "https://httpbin.org/ip"
    
    def add_proxy(self, proxy: Proxy) -> None:
        """
        Add a proxy to the manager.
        
        Args:
            proxy: Proxy to add
        """
        with self.lock:
            self.proxies.append(proxy)
    
    def add_proxies_from_list(self, proxy_list: List[str]) -> None:
        """
        Add proxies from a list of URLs.
        
        Args:
            proxy_list: List of proxy URLs
        """
        for url in proxy_list:
            self.add_proxy(Proxy(url))
    
    def add_proxies_from_file(self, file_path: str) -> None:
        """
        Add proxies from a file (one proxy per line).
        
        Args:
            file_path: Path to proxy list file
        """
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        self.add_proxy(Proxy(line))
        except Exception as e:
            log.error("Error loading proxies from file %s: %s", file_path, e)
    
    def get_proxy(self) -> Optional[Proxy]:
        """
        Get the best available proxy.
        
        Returns:
            Best available proxy or None if no proxies available
        """
        with self.lock:
            # Filter available proxies
            available = [p for p in self.proxies if p.is_available()]
            
            if not available:
                return None
                
            # Sort by success rate (highest first)
            available.sort(key=lambda p: p.get_success_rate(), reverse=True)
            
            # Return the best proxy
            return available[0]
    
    def get_random_proxy(self) -> Optional[Proxy]:
        """
        Get a random available proxy.
        
        Returns:
            Random available proxy or None if no proxies available
        """
        with self.lock:
            # Filter available proxies
            available = [p for p in self.proxies if p.is_available()]
            
            if not available:
                return None
                
            # Return a random proxy
            return random.choice(available)
    
    def test_proxy(self, proxy: Proxy) -> bool:
        """
        Test if a proxy is working.
        
        Args:
            proxy: Proxy to test
            
        Returns:
            True if proxy is working, False otherwise
        """
        try:
            # Create a new session for testing
            session = requests.Session()
            
            # Set proxy
            session.proxies.update(proxy.get_proxy_dict())
            
            # Set timeout and headers
            timeout = (5, 10)  # Connection timeout, read timeout
            headers = {"User-Agent": config.user_agent}
            
            # Make request
            response = session.get(self.test_url, timeout=timeout, headers=headers)
            
            # Check if successful
            if response.status_code == 200:
                log.debug("Proxy test successful: %s", proxy.url)
                return True
                
            log.warning("Proxy test failed with status %d: %s", 
                       response.status_code, proxy.url)
            return False
            
        except Exception as e:
            log.warning("Proxy test error for %s: %s", proxy.url, e)
            return False
    
    def test_all_proxies(self) -> Tuple[int, int]:
        """
        Test all proxies and update their status.
        
        Returns:
            Tuple of (working count, total count)
        """
        working = 0
        total = 0
        
        with self.lock:
            for proxy in self.proxies:
                total += 1
                if self.test_proxy(proxy):
                    proxy.mark_success()
                    working += 1
                else:
                    proxy.mark_failure()
        
        log.info("Proxy test results: %d/%d working", working, total)
        return working, total
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the proxy manager.
        
        Returns:
            Dictionary of statistics
        """
        with self.lock:
            total = len(self.proxies)
            available = sum(1 for p in self.proxies if p.is_available())
            success_rates = [p.get_success_rate() for p in self.proxies if p.success_count + p.failure_count > 0]
            avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0
            
            return {
                "total_proxies": total,
                "available_proxies": available,
                "unavailable_proxies": total - available,
                "average_success_rate": avg_success_rate
            }

# Create a global proxy manager instance
proxy_manager = ProxyManager()
