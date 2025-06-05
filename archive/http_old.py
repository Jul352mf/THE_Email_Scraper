"""
Enhanced HTTP client module with improved thread safety and security.

This module provides a thread-safe HTTP client with proper error handling,
retry mechanisms, and security features.
"""

import logging
import re
import os
import threading
import time
from typing import Optional, Dict, Any, Callable
from urllib.parse import urlparse, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, SSLError, Timeout

from scraper.config import config

# Initialize logger
log = logging.getLogger(__name__)

# Thread-local storage for tracking visited URLs and sessions
thread_local = threading.local()

class ThreadSafeSession:
    """Thread-safe session manager that creates a session per thread."""
    
    def __init__(self):
        """Initialize the thread-safe session manager."""
        self._session_lock = threading.Lock()
        self._insecure_hosts = set()
    
    def _create_session(self, verify: bool = True) -> requests.Session:
        """
        Create a requests session with retry and timeout configuration.
        
        Args:
            verify: Whether to verify SSL certificates
            
        Returns:
            Configured requests session
        """
        session = requests.Session()
        session.headers.update({"User-Agent": config.user_agent})
        session.verify = verify
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def get_session(self, url: str) -> requests.Session:
        """
        Get a session for a URL with appropriate SSL verification.
        
        Args:
            url: URL to get session for
            
        Returns:
            Configured requests.Session
        """
        # Always respect the global insecure_ssl setting
        verify_ssl = not config.insecure_ssl
        
        # Create session with appropriate SSL verification
        return self._create_session(verify=verify_ssl)
    
    def clear_visited(self) -> None:
        """Clear the set of visited URLs for the current thread."""
        if hasattr(thread_local, 'visited'):
            thread_local.visited.clear()

class HttpClient:
    """Enhanced HTTP client with improved error handling and security features."""
    
    def __init__(self):
        """Initialize the HTTP client."""
        self._session_manager = ThreadSafeSession()
        
        # Debug mode for saving HTML content
        self.debug_mode = os.getenv("DEBUG_MODE", "0").lower() in {"1", "true", "yes"}
        self.debug_dir = os.getenv("DEBUG_DIR", "debug_output")
        
        # Create debug directory if needed
        if self.debug_mode and not os.path.exists(self.debug_dir):
            try:
                os.makedirs(self.debug_dir)
                log.info("Created debug directory: %s", self.debug_dir)
            except Exception as e:
                log.warning("Failed to create debug directory: %s", e)
                self.debug_mode = False
    
    def safe_get(self, url: str, method: str = "GET", 
                 max_redirects: int = None, 
                 timeout: Optional[tuple] = None,
                 retry_count: int = 1,
                 retry_delay: float = 1.0,
                 headers: Optional[Dict[str, str]] = None,
                 callback: Optional[Callable[[requests.Response], Any]] = None) -> Optional[requests.Response]:
        """
        Safely make HTTP requests with proper error handling and redirect management.
        
        Args:
            url: URL to request
            method: HTTP method (GET or HEAD)
            max_redirects: Maximum number of redirects to follow
            timeout: Request timeout (connection_timeout, read_timeout)
            retry_count: Number of times to retry on failure
            retry_delay: Delay between retries in seconds
            headers: Additional headers to send
            callback: Optional callback function to process the response
            
        Returns:
            Response object or None if failed
        """
        if not validate_url(url):
            log.warning("Invalid URL: %s", url)
            return None
        
        # Use default timeout if not specified
        if timeout is None:
            timeout = config.request_timeout
        
        # Use default max_redirects if not specified
        if max_redirects is None:
            max_redirects = config.max_redirects
        
        # Initialize thread_local.visited if not already done
        if not hasattr(thread_local, 'visited'):
            thread_local.visited = set()
        
        # Check for redirect loops
        if url in thread_local.visited:
            log.warning("Redirect loop detected: %s", url)
        
        thread_local.visited.add(url)
        
        # Get the appropriate session
        session = self._session_manager.get_session(url)
        
        # Prepare request function
        request_fn = session.head if method == "HEAD" else session.get
        
        # Combine default headers with custom headers
        request_headers = {}
        if headers:
            request_headers.update(headers)
        
        # Try the request with retries
        response = None
        for attempt in range(retry_count):
            try:
                response = request_fn(
                    url, 
                    allow_redirects=True, 
                    timeout=timeout,
                    headers=request_headers if request_headers else None
                )
                
                # Save HTML content if in debug mode
                if self.debug_mode and response.status_code == 200:
                    self._save_debug_content(url, response)
                
                # Log response details at debug level
                log.debug("GET %s -> %d (%d bytes)", url, response.status_code, len(response.content))
                
                # Break if successful
                if response and response.ok:
                    break
                    
                # If we got a response but it's not OK, log and continue
                if response:
                    log.debug("Request failed with status %d: %s", response.status_code, url)

            except SSLError as e:
                log.debug("Request attempt %d/%d failed for %s: %s", attempt, retry_count, url, e)
                if attempt == retry_count:
                    # If we've exhausted retries and it's an SSL error, try one more time with verification disabled
                    if config.insecure_ssl:
                        try:
                            log.debug("Retrying with explicit SSL verification disabled: %s", url)
                            response = request_fn(
                                url,
                                timeout=timeout,
                                allow_redirects=True,
                                headers=request_headers if request_headers else None,
                                verify=False
                            )
                            
                            # Save HTML content if in debug mode
                            if self.debug_mode and response.status_code == 200:
                                self._save_debug_content(url, response)
                                
                            return response
                        except Exception as e2:
                            log.debug("Final attempt failed for %s: %s", url, e2)
                            return None
                    else:
                        return None
                
            except requests.RequestException as e:
                log.debug("Request attempt %d/%d failed for %s: %s", 
                         attempt + 1, retry_count, url, str(e))
                
                # Wait before retrying
                if attempt < retry_count - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
            
        # If all attempts failed
        if not response or not response.ok:
            return None
        
        # Handle redirects
        final_url = response.url
        if final_url and final_url != url and final_url not in thread_local.visited:
            thread_local.visited.add(final_url)
            try:
                # Don't follow redirects again to avoid loops
                redirect_response = session.get(
                    final_url, 
                    allow_redirects=False, 
                    timeout=timeout,
                    headers=request_headers if request_headers else None
                )
                
                if redirect_response and redirect_response.ok:
                    response = redirect_response
                    
            except requests.RequestException as e:
                log.debug("Redirect request failed for %s: %s", final_url, e)
        
        # Clean up visited set to prevent memory leaks
        if len(thread_local.visited) > 100:
            self._session_manager.clear_visited()
        
        # Process response with callback if provided
        if callback and response and response.ok:
            try:
                callback(response)
            except Exception as e:
                log.error("Callback error for %s: %s", url, e)
        
        return response if response and response.ok else None
    
    def _save_debug_content(self, url: str, response: requests.Response) -> None:
        """
        Save response content for debugging.
        
        Args:
            url: URL that was requested
            response: Response object
        """
        if not self.debug_mode:
            return
            
        try:
            # Create a safe filename from the URL
            parsed = urlparse(url)
            domain = parsed.netloc
            path = parsed.path.strip("/").replace("/", "_")
            if not path:
                path = "index"
                
            filename = f"{domain}_{path}.html"
            filepath = os.path.join(self.debug_dir, filename)
            
            # Save the content
            with open(filepath, "wb") as f:
                f.write(response.content)
                
            log.debug("Saved debug content for %s to %s", url, filepath)
            
        except Exception as e:
            log.warning("Failed to save debug content for %s: %s", url, e)




# URL utilities
def validate_url(url: str) -> bool:
    """
    Validate URL length and format with enhanced security checks.
    
    Args:
        url: URL to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not url:
        return False
    
    # Check URL length
    if len(url) > config.max_url_length:
        log.warning("URL exceeds maximum length (%d): %s", config.max_url_length, url)
        return False
    
    # Check URL format
    try:
        result = urlparse(url)
        if not all([result.scheme, result.netloc]):
            return False
        
        # Ensure scheme is http or https
        if result.scheme not in ('http', 'https'):
            log.warning("Invalid URL scheme: %s", url)
            return False
        
        # Check for potentially malicious patterns
        if re.search(r'(file|data|javascript):', url, re.IGNORECASE):
            log.warning("Potentially malicious URL scheme: %s", url)
            return False
        
        return True
        
    except Exception as e:
        log.warning("URL validation error for %s: %s", url, e)
        return False

def normalise_domain(url: str) -> str:
    """
    Normalize a domain by removing www prefix and converting to lowercase.
    
    Args:
        url: URL or domain string
        
    Returns:
        Normalized domain string
    """
    try:
        host = urlparse(url).netloc if url.startswith(("http://", "https://")) else url
        return host.lower().removeprefix("www.")
    except Exception as e:
        log.warning("Domain normalization error for %s: %s", url, e)
        return url.lower()

def join_url(base: str, path: str) -> str:
    """
    Join a base URL and a path, handling relative paths correctly.
    
    Args:
        base: Base URL or domain
        path: Path to join
        
    Returns:
        Complete URL
    """
    # If path is already a complete URL, return it
    if path.startswith(('http://', 'https://')):
        return path
    
    # If base is just a domain, add https://
    if not base.startswith(('http://', 'https://')):
        base = f"https://{base}"
    
    # Join the base and path
    joined_url = urljoin(base, path.lstrip('/'))
    
    # Validate the joined URL
    if not validate_url(joined_url):
        log.warning("Invalid joined URL: %s + %s", base, path)
        return ""
    
    return joined_url

# Create a global HTTP client instance
http_client = HttpClient()
