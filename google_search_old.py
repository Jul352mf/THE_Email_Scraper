import logging
import threading
import time
from typing import List, Dict, Any, Optional, Callable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from scraper.config import config, API_KEY, CX_ID

# Initialize logger
log = logging.getLogger(__name__)

# Rate limiting globals
_last_google_ts: float = 0.0
_google_lock = threading.Lock()

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
        Initialize the Google API service with error handling.
        """
        with self._init_lock:
            if self._service is not None:
                return
            try:
                self._service = build(
                    "customsearch", 
                    "v1", 
                    developerKey=API_KEY, 
                    cache_discovery=False
                )
            except Exception as e:
                msg = f"Failed to initialize Google API service: {e}"
                log.error(msg)
                raise GoogleApiError(msg)
    
    def _respect_rate(self) -> None:
        """
        Ensure Google API rate limits are respected with enhanced synchronization.
        """
        global _last_google_ts
        with _google_lock:
            now = time.time()
            wait = config.google_safe_interval - (now - _last_google_ts)
            if wait > 0:
                log.debug("Rate limiting: waiting %.2f seconds", wait)
                time.sleep(wait)
            _last_google_ts = time.time()
    
    def search(
        self,
        query: str,
        num_results: int = 10,
        site_restrict: Optional[str] = None,
        callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform a Google search with enhanced error handling and rate limiting.
        """
        if not query:
            log.warning("Empty search query")
            return []
        if num_results > 10:
            log.warning("Google API limits results to 10, adjusting num_results")
            num_results = 10
        full_q = f"{query} {site_restrict}" if site_restrict else query
        log.debug("Searching for: %s", full_q)
        if self._service is None:
            self._initialize_service()

        backoff = 1
        for attempt in range(config.google_max_retries):
            try:
                self._respect_rate()
                resp = (
                    self._service.cse()
                        .list(q=full_q, cx=CX_ID, num=num_results)
                        .execute()
                )
                items = resp.get("items", [])
                if callback and items:
                    try:
                        callback(items)
                    except Exception as cb_e:
                        log.error("Callback error: %s", cb_e)
                return items

            except HttpError as he:
                status = getattr(he.resp, 'status', None)
                # quota exceeded
                if status in (403, 429):
                    backoff = backoff * 2
                    log.warning(
                        "Google quota %s – sleeping %ds (attempt %d/%d)",
                        status, backoff, attempt+1, config.google_max_retries
                    )
                    time.sleep(backoff)
                    continue
                log.error("Google API error (status %s): %s", status, he)
                raise GoogleApiError(he)

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

# global instance
google_client = GoogleSearchClient()
