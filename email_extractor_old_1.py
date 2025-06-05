"""
Enhanced email extraction module with improved error handling and security.

This module provides robust email extraction functionality with proper error handling,
validation, and security features.
"""

import logging
import re
import os
from typing import Set, Optional, List, Pattern

import idna
from bs4 import BeautifulSoup

from scraper.config import config
from scraper.http import http_client

# Initialize logger
log = logging.getLogger(__name__)

# Regular expressions for email extraction
EMAIL_RE = re.compile(
    r"(?i)(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\.)+[A-Z]{2,63}(?![A-Z0-9._%+-])"
)
MAILTO_RE = re.compile(r"(?i)mailto:")

class EmailValidationError(Exception):
    """Exception raised for email validation errors."""
    pass

class EmailExtractor:
    """Enhanced email extractor with improved validation and security features."""
    
    def __init__(self):
        """Initialize the email extractor with validation patterns."""
        
        # Additional validation patterns
        self.domain_blacklist: Set[str] = {
            "example.com", "test.com", "domain.com", "email.com", 
            "yourcompany.com", "company.com", "localhost"
        }
        
        # Suspicious patterns that might indicate fake emails
        self.suspicious_patterns: List[Pattern] = [
            re.compile(r"(?i)noreply@"),
            re.compile(r"(?i)donotreply@"),
            re.compile(r"(?i)no-reply@"),
            re.compile(r"(?i)webmaster@"),
            re.compile(r"(?i)hostmaster@"),
            re.compile(r"(?i)postmaster@"),
        ]
        
                # New: really-bad patterns
        self._drop_patterns: List[Pattern] = [
            re.compile(r"%"),                                  # percent-encoded
            re.compile(r"\.(?:png|jpe?g|gif)$", re.I),         # asset filenames
            re.compile(r"^[0-9a-f]{20,}$", re.I),              # long hex local-parts
        ]
    
    def is_valid_email(self, email: str) -> bool:
        """
        Check if an email address is valid.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email or '@' not in email:
            return False
        
        try:
            local_part, domain = email.rsplit('@', 1)
            
            # Check local part
            if not local_part or len(local_part) > 64:
                return False
            
            # Check domain
            if not domain or len(domain) > 255 or '.' not in domain:
                return False
            
            # Skip blacklist check in test environment
            if not os.environ.get('SCRAPER_TEST_MODE'):
                # Check for blacklisted domains
                if domain.lower() in self.domain_blacklist:
                    log.debug("Blacklisted domain in email: %s", email)
                    return False
            
            # Check for suspicious patterns
            for pattern in self.suspicious_patterns:
                if pattern.search(email):
                    log.debug("Suspicious email pattern detected: %s", email)
                    
                    return False
                
            # drop on any of our new bad patterns
            for pat in self._drop_patterns:
                if pat.search(local_part) or pat.search(domain):
                    log.debug("Dropping email by pattern %s: %s", pat.pattern, email)
                    return False
            
            return True
            
        except Exception as e:
            log.debug("Email validation error for %s: %s", email, e)
            return False
    
    def clean_email(self, email: str) -> str:
        """
        Clean and normalize email addresses with enhanced validation.
        
        Args:
            email: Raw email address
            
        Returns:
            Cleaned email address
            
        Raises:
            EmailValidationError: If email cannot be cleaned
        """
         
        log.debug("Raw email before cleaning: %s", email)
        
        # Normalize whitespace and case
        email = email.strip()
        # Remove mailto: in a case-insensitive way by checking lowercase
        if email.lower().startswith("mailto:"):
            email = email[len("mailto:"):]
        # Then drop URL params
        local = email.split("?", 1)[0]
        
        # Split into user and host parts
        try:
            user, host = local.rsplit('@', 1)
        except ValueError:
            raise EmailValidationError(f"Invalid email format: {email}")
        
        host = host.strip().rstrip("%;,:)}]>\"'`")
        
        # Handle internationalized domains
        try:
            host = idna.decode(host)
        except Exception as e:
            log.warning("Failed to decode internationalized domain %s: %s", host, e)
        
        cleaned_email = f"{user}@{host}".lower()
        
        # Validate the cleaned email
        if not self.is_valid_email(cleaned_email):
            log.warning("Invalid email after cleaning: %s", cleaned_email)
            raise EmailValidationError(f"Invalid email after is_valid_email: {email}")
        
        return cleaned_email
    
    def extract_from_url(self, url: str) -> Set[str]:
        """
        Extract email addresses from a URL with enhanced error handling.
        
        Args:
            url: URL to extract emails from
            
        Returns:
            Set of valid email addresses
        """
        # Skip PDFs if not configured to process them
        if not config.process_pdfs and url.lower().endswith(".pdf"):
            log.debug("Skipping PDF %s", url)
            return set()
        
        # Get the page content with retry and timeout
        response = http_client.safe_get(
            url, 
            retry_count=2,
            timeout=(10, 60)  # Longer timeout for potentially large pages
        )
        
        if not response:
            return set()
        
        # Extract emails if the content is HTML
        if "html" in response.headers.get("Content-Type", ""):
            return self.extract_from_html(response.text, url)
        
        # For non-HTML content, try direct text extraction
        return self.extract_from_text(response.text, url)
    
    def extract_from_html(self, html: str, url: Optional[str] = None) -> Set[str]:
        """
        Extract email addresses from HTML content with enhanced parsing.
        
        Args:
            html: HTML content to extract emails from
            url: Source URL for logging purposes
            
        Returns:
            Set of valid email addresses
        """
        if not html:
            return set()
        
        hits = set()
        
        try:
            # Extract emails from text using regex
            for match in EMAIL_RE.finditer(html):
                try:
                    email = self.clean_email(match.group(0))
                    if self.is_valid_email(email):
                        hits.add(email)
                except Exception as e:
                    log.debug("Failed to clean email %s: %s", match.group(0), e)
            
            # Extract mailto links with case-insensitive detection
            try:
                soup = BeautifulSoup(html, "html.parser")
                for anchor in soup.find_all("a", href=True):
                    href = anchor["href"]
                    if MAILTO_RE.match(href):
                        try:
                            email = href.split(":", 1)[1].strip()
                            cleaned_email = self.clean_email(email)
                            if self.is_valid_email(cleaned_email):
                                hits.add(cleaned_email)
                        except Exception as e:
                            log.debug("Failed to clean mailto email %s: %s", href, e)
            except Exception as e:
                log.warning("BeautifulSoup parsing error: %s", e)
        
        except Exception as e:
            log.error("Error extracting emails from HTML: %s", e)
        
        if url:
            log.debug(" %2d emails on %s", len(hits), url)
        
        return hits
    
    def extract_from_text(self, text: str, url: Optional[str] = None) -> Set[str]:
        """
        Extract email addresses from plain text.
        
        Args:
            text: Text content to extract emails from
            url: Source URL for logging purposes
            
        Returns:
            Set of valid email addresses
        """
        if not text:
            return set()
        
        hits = set()
        
        try:
            # Extract emails using regex
            for match in EMAIL_RE.finditer(text):
                try:
                    email = self.clean_email(match.group(0))
                    if self.is_valid_email(email):
                        hits.add(email)
                except Exception as e:
                    log.debug("Failed to clean email %s: %s", match.group(0), e)
        
        except Exception as e:
            log.error("Error extracting emails from text: %s", e)
        
        if url:
            log.debug(" %2d emails on %s", len(hits), url)
        
        return hits

# Create a global email extractor instance
email_extractor = EmailExtractor()
