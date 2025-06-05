"""
Google fallback module with alternative search strategies.

This module provides fallback search mechanisms when the primary Google API fails,
including alternative search engines and retry strategies.
"""

import logging
import random
import re
import time
from typing import List, Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from scraper.cache import google_cache
from scraper.config import config
from scraper.http import http_client, validate_url

# Initialize logger
log = logging.getLogger(__name__)

class GoogleFallbackError(Exception):
    """Exception raised for Google fallback errors."""
    pass

class GoogleFallback:
    """
    Google fallback implementation with alternative search strategies.
    
    This class provides:
    - Alternative search methods when Google API fails
    - Caching of search results
    - Rate limiting and retry mechanisms
    """
    
    def __init__(self):
        """Initialize the Google fallback."""
        # User agents for scraping
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ]
        
        # Search engines for fallback
        self.search_engines = [
            {
                "name": "Bing",
                "url": "https://www.bing.com/search?q={query}",
                "result_selector": "li.b_algo h2 a",
                "link_attribute": "href"
            },
            {
                "name": "DuckDuckGo",
                "url": "https://html.duckduckgo.com/html/?q={query}",
                "result_selector": "a.result__a",
                "link_attribute": "href"
            }
        ]
    
    def _get_random_user_agent(self) -> str:
        """
        Get a random user agent.
        
        Returns:
            Random user agent string
        """
        return random.choice(self.user_agents)
    
    def _extract_search_results(self, html: str, selector: str, attribute: str) -> List[Dict[str, Any]]:
        """
        Extract search results from HTML.
        
        Args:
            html: HTML content
            selector: CSS selector for result elements
            attribute: Attribute containing the URL
            
        Returns:
            List of search result items
        """
        results = []
        try:
            soup = BeautifulSoup(html, "html.parser")
            for element in soup.select(selector):
                url = element.get(attribute, "")
                title = element.get_text(strip=True)
                
                # Validate URL
                if url and validate_url(url):
                    results.append({
                        "title": title,
                        "link": url,
                        "displayLink": url.split("/")[2] if url.startswith("http") else ""
                    })
        except Exception as e:
            log.warning("Error extracting search results: %s", e)
        
        return results[:10]  # Limit to 10 results like Google API
    
    def search_with_fallback_engine(self, query: str) -> List[Dict[str, Any]]:
        """
        Search using alternative search engines.
        
        Args:
            query: Search query
            
        Returns:
            List of search result items
            
        Raises:
            GoogleFallbackError: If all fallback engines fail
        """
        # Try each search engine
        for engine in self.search_engines:
            try:
                log.info("Trying fallback search with %s for query: %s", engine["name"], query)
                
                # Format URL with query
                url = engine["url"].format(query=query.replace(" ", "+"))
                
                # Set custom headers
                headers = {
                    "User-Agent": self._get_random_user_agent(),
                    "Accept": "text/html,application/xhtml+xml,application/xml",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.google.com/"
                }
                
                # Make request
                response = http_client.safe_get(
                    url, 
                    headers=headers,
                    retry_count=3,
                    retry_delay=2.0
                )
                
                if not response:
                    log.warning("Failed to get response from %s", engine["name"])
                    continue
                
                # Extract results
                results = self._extract_search_results(
                    response.text,
                    engine["result_selector"],
                    engine["link_attribute"]
                )
                
                if results:
                    log.info("Found %d results with %s fallback", len(results), engine["name"])
                    return results
                    
            except Exception as e:
                log.warning("Error with %s fallback: %s", engine["name"], e)
        
        # If all engines failed
        log.error("All fallback search engines failed for query: %s", query)
        raise GoogleFallbackError(f"All fallback search engines failed for query: {query}")
    
    def search_with_cache(self, query: str) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Search with cache support.
        
        Args:
            query: Search query
            
        Returns:
            Tuple of (search results, whether results came from cache)
        """
        # Normalize query for cache key
        cache_key = query.lower().strip()
        
        # Check cache
        cached_results = google_cache.get(cache_key)
        if cached_results:
            log.info("Using cached search results for query: %s", query)
            return cached_results, True
        
        # Try fallback search
        try:
            results = self.search_with_fallback_engine(query)
            
            # Cache results
            if results:
                google_cache.set(cache_key, results)
                
            return results, False
            
        except GoogleFallbackError:
            # Return empty results if all fallbacks fail
            return [], False

# Create a global Google fallback instance
google_fallback = GoogleFallback()
