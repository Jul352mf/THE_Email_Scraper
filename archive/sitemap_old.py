"""
Enhanced sitemap module with improved error handling and parsing.

This module provides robust sitemap discovery and parsing functionality with
proper error handling, validation, and security features.
"""

import gzip
import logging
import time
from typing import Generator, List, Tuple, Optional, Set
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scraper.config import config
from scraper.http import http_client, join_url, validate_url

# Initialize logger
log = logging.getLogger(__name__)

class SitemapError(Exception):
    """Exception raised for sitemap parsing errors."""
    pass

class SitemapParser:
    """Enhanced sitemap parser with improved error handling and validation."""
    
    def __init__(self):
        """Initialize the sitemap parser."""
        # Track processed sitemaps to avoid duplicates
        self._processed_sitemaps: Set[str] = set()
        
        # Maximum sitemap size to process (50MB)
        self.max_sitemap_size = 50 * 1024 * 1024
        
        # Maximum number of URLs to extract from a single sitemap
        self.max_urls_per_sitemap = 10000
    
    def discover_sitemaps(self, domain: str) -> Generator[str, None, None]:
        """
        Discover sitemap URLs for a domain with enhanced error handling.
        
        Args:
            domain: Domain to search for sitemaps
            
        Yields:
            Sitemap URLs
        """
        if not domain:
            log.warning("Empty domain provided to discover_sitemaps")
            return
            
        any_found = False
        start_time = time.time()
        
        # Check standard sitemap locations
        for host in (domain, f"www.{domain}" if not domain.startswith("www.") else domain):
            for name in config.sitemap_filenames:
                url = f"https://{host}/{name}"
                
                # Validate URL
                if not validate_url(url):
                    log.warning("Invalid sitemap URL: %s", url)
                    continue
                    
                log.debug("Checking sitemap candidate: %s", url)
                
                # Check if sitemap exists
                head = http_client.safe_get(url, "HEAD", retry_count=2)
                status = head.status_code if head else 'ERR'
                log.debug(" HEAD %s → %s", url, status)
                
                if head:
                    # Check content type
                    content_type = head.headers.get("Content-Type", "")
                    if not any(ct in content_type.lower() for ct in ["xml", "text", "application"]):
                        log.debug("Skipping non-XML content type: %s", content_type)
                        continue
                        
                    # Check content size
                    content_length = int(head.headers.get("Content-Length", "0"))
                    if content_length > self.max_sitemap_size:
                        log.warning("Sitemap too large (%d bytes): %s", content_length, url)
                        continue
                    
                    # Get sitemap content
                    body = http_client.safe_get(url, retry_count=2)
                    if not body:
                        continue
                        
                    size = len(body.content) if body and body.ok else 'FAIL'
                    log.debug(" GET %s → %s bytes", url, size)
                    
                    # Validate sitemap content
                    if body and body.ok:
                        # Check for XML content
                        is_xml = (
                            body.content.startswith(b"<?xml") or
                            body.content.startswith(b"\x1f\x8b")  # gzip magic number
                        )
                        
                        if is_xml:
                            any_found = True
                            if url not in self._processed_sitemaps:
                                self._processed_sitemaps.add(url)
                                yield url
        
        # If no standard sitemaps found, check robots.txt
        if not any_found:
            robots_url = f"https://{domain}/robots.txt"
            response = http_client.safe_get(robots_url, retry_count=2)
            
            if response:
                for line in response.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_url = line.split(":", 1)[1].strip()
                        
                        # Fix relative paths from robots.txt
                        if not sitemap_url.startswith(('http://', 'https://')):
                            sitemap_url = join_url(domain, sitemap_url)
                        
                        # Validate URL
                        if not validate_url(sitemap_url):
                            log.warning("Invalid sitemap URL from robots.txt: %s", sitemap_url)
                            continue
                            
                        if sitemap_url not in self._processed_sitemaps:
                            self._processed_sitemaps.add(sitemap_url)
                            yield sitemap_url
        
        elapsed = time.time() - start_time
        log.debug("Sitemap discovery for %s completed in %.2f seconds", domain, elapsed)
    
    def parse_sitemap(self, xml_content: bytes) -> Generator[str, None, None]:
        """
        Parse sitemap XML to extract URLs with enhanced error handling.
        
        Args:
            xml_content: Sitemap XML content
            
        Yields:
            URLs from sitemap
            
        Raises:
            SitemapError: If sitemap parsing fails
        """
        if not xml_content:
            log.warning("Empty sitemap content")
            return
            
        # Check content size
        if len(xml_content) > self.max_sitemap_size:
            log.warning("Sitemap content too large: %d bytes", len(xml_content))
            raise SitemapError(f"Sitemap content too large: {len(xml_content)} bytes")
        
        # Check if sitemap is gzipped
        try:
            if xml_content.startswith(b"\x1f\x8b"):
                try:
                    xml_content = gzip.decompress(xml_content)
                except Exception as e:
                    log.error("Failed to decompress gzipped sitemap: %s", e)
                    raise SitemapError(f"Failed to decompress gzipped sitemap: {e}")
        except Exception as e:
            log.error("Error checking sitemap compression: %s", e)
            return
            
        # Parse XML with BeautifulSoup
        try:
            soup = BeautifulSoup(xml_content, "xml")
            
            # Check for sitemap index
            sitemapindex = soup.find("sitemapindex")
            if sitemapindex:
                log.debug("Found sitemap index with %d sitemaps", len(sitemapindex.find_all("sitemap")))
                
                # Process nested sitemaps
                for sitemap in sitemapindex.find_all("sitemap"):
                    loc = sitemap.find("loc")
                    if loc:
                        nested_url = loc.get_text(strip=True)
                        if validate_url(nested_url) and nested_url not in self._processed_sitemaps:
                            self._processed_sitemaps.add(nested_url)
                            log.debug("Processing nested sitemap: %s", nested_url)
                            
                            # Get nested sitemap content
                            response = http_client.safe_get(nested_url, retry_count=2)
                            if response and response.ok:
                                # Process nested sitemap content
                                for url in self.parse_sitemap(response.content):
                                    yield url
            
            # Extract URLs from loc elements
            url_count = 0
            for loc in soup.find_all("loc"):
                url = loc.get_text(strip=True)
                
                # Validate URL
                if url and validate_url(url):
                    url_count += 1
                    
                    # Check if we've reached the maximum URLs per sitemap
                    if url_count > self.max_urls_per_sitemap:
                        log.warning("Reached maximum URLs per sitemap (%d)", self.max_urls_per_sitemap)
                        break
                        
                    yield url
                    
        except Exception as e:
            log.error("Error parsing sitemap: %s", e)
            raise SitemapError(f"Error parsing sitemap: {e}")
    
    def get_priority_urls(self, domain: str) -> Tuple[List[str], bool]:
        """
        Get priority URLs from sitemaps based on configured priority path parts.
        
        Args:
            domain: Domain to search for sitemaps
            
        Returns:
            Tuple of (list of priority URLs, whether sitemap was used)
        """
        priority_urls = []
        used_sitemap = False
        start_time = time.time()
        
        try:
            # Discover and parse sitemaps
            for sitemap_url in self.discover_sitemaps(domain):
                used_sitemap = True
                log.debug("Processing sitemap: %s", sitemap_url)
                
                response = http_client.safe_get(sitemap_url, retry_count=2)
                if not response:
                    continue
                    
                try:
                    url_count = 0
                    priority_count = 0
                    
                    for url in self.parse_sitemap(response.content):
                        url_count += 1
                        
                        # Check if URL contains any priority path parts
                        url_lower = url.lower()
                        if any(part in url_lower for part in config.priority_parts):
                            priority_urls.append(url)
                            priority_count += 1
                            
                    log.debug("Extracted %d URLs (%d priority) from sitemap %s", 
                             url_count, priority_count, sitemap_url)
                    
                except SitemapError as e:
                    log.warning("Error processing sitemap %s: %s", sitemap_url, e)
                    
        except Exception as e:
            log.error("Error getting priority URLs for %s: %s", domain, e)
        
        elapsed = time.time() - start_time
        log.debug("Priority URL extraction for %s completed in %.2f seconds: %d URLs", 
                 domain, elapsed, len(priority_urls))
        
        return priority_urls, used_sitemap
    
    def clear_cache(self) -> None:
        """Clear the processed sitemaps cache."""
        self._processed_sitemaps.clear()

# Create a global sitemap parser instance
sitemap_parser = SitemapParser()
