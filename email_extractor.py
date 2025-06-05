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


_OBF_EMAIL = re.compile(
    r"""
    (?P<user>[A-Za-z0-9._%+-]+)              # local-part
    \s*(?:\[\s*at\s*\]|\(\s*at\s*\)|\bat\b)\s*  # obfuscated “at”
    (?P<host>(?:[A-Za-z0-9-]+                  # domain labels
        (?:\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\bdot\b)\s*[A-Za-z0-9-]+)+))  # one or more obf-dot + label
    """,
    re.IGNORECASE | re.VERBOSE,
)

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
            # re.compile(r"%"),                                  # percent-encoded
            re.compile(r"\.(?:png|jpe?g|gif)$", re.I),         # asset filenames
            re.compile(r"^[0-9a-f]{20,}$", re.I),              # long hex local-parts
        ]
    
    def is_valid_email(self, email: str) -> bool:
        try:
            if not email or '@' not in email:
                log.debug("Rejecting %r: empty or missing @", email)
                return False

            # Split local and domain, catch missing/@-only cases here
            local_part, domain = email.rsplit('@', 1)

            # Local‐part checks
            if not local_part:
                log.debug("Rejecting %r: empty local-part", email)
                return False
            if len(local_part) > 64:
                log.debug("Rejecting %r: local-part too long (%d > 64)", email, len(local_part))
                return False

            # Domain checks
            if not domain:
                log.debug("Rejecting %r: empty domain", email)
                return False
            if len(domain) > 255:
                log.debug("Rejecting %r: domain too long (%d > 255)", email, len(domain))
                return False
            if '.' not in domain:
                log.debug("Rejecting %r: domain has no dot", email)
                return False

            # Blacklist
            if not os.environ.get('SCRAPER_TEST_MODE') and domain.lower() in self.domain_blacklist:
                log.debug("Rejecting %r: blacklisted domain %r", email, domain.lower())
                return False

            # Suspicious patterns
            for pat in self.suspicious_patterns:
                if pat.search(email):
                    log.debug("Rejecting %r: matched suspicious pattern %r", email, pat.pattern)
                    return False

            # Drop‐patterns
            for pat in self._drop_patterns:
                if pat.search(local_part) or pat.search(domain):
                    log.debug("Rejecting %r: matched drop-pattern %r", email, pat.pattern)
                    return False

            return True

        except ValueError:
            log.debug("Rejecting %r: cannot split local@domain", email)
            return False

        except Exception as e:
            log.debug("Email validation error for %r: %s", email, e)
            return False
    
    def clean_email(self, email: str) -> str:
        log.debug("Attempting to clean %r", email)
        email = email.strip()
        if email.lower().startswith("mailto:"):
            log.debug("Stripping mailto: prefix from %r", email)
            email = email[len("mailto:"):]
        email = email.split("?", 1)[0]

        try:
            user, host = email.rsplit('@', 1)
        except ValueError:
            log.warning("Failed to clean %r: invalid format (no single @)", email)
            raise EmailValidationError(f"Invalid email format: {email}")

        host = host.strip().rstrip("%;,:)}]>\"'`")
        try:
            host = idna.decode(host)
        except Exception as e:
            log.warning("IDNA decode failed for %r: %s", host, e)

        cleaned = f"{user}@{host}".lower()
        if not self.is_valid_email(cleaned):
            log.warning("Validation failed after cleaning: %r → %r", email, cleaned)
            raise EmailValidationError(f"Invalid email after cleaning: {cleaned}")

        log.debug("Successfully cleaned %r → %r", email, cleaned)
        return cleaned
    
    @staticmethod
    def deobfuscate_emails(text: str) -> str:
        def _repl(m):
            user = m.group("user")
            host = m.group("host")
            # turn any [dot]/(dot)/dot in the host into real dots
            host = re.sub(r'(\[\s*dot\s*\]|\(\s*dot\s*\)|\bdot\b)', '.', host, flags=re.IGNORECASE)
            # collapse any stray spaces around dots
            host = re.sub(r'\s*\.\s*', '.', host)
            return f"{user}@{host}"
        # only replace where the full pattern matches
        return _OBF_EMAIL.sub(_repl, text)
    
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
        hits: Set[str] = set()
        seen_raw: Set[str] = set()
        seen_clean: Set[str] = set()
        try:
            soup = BeautifulSoup(html, "html.parser")

            # 1) Extract from visible text only
            page_text = soup.get_text(separator=" ")
            page_text = self.deobfuscate_emails(page_text)
            for match in EMAIL_RE.finditer(page_text):
                raw = match.group(0)
                if raw in seen_raw:
                    continue
                seen_raw.add(raw)
                try:
                    cleaned = self.clean_email(raw)
                    # only add each cleaned address once
                    if cleaned not in seen_clean:
                        seen_clean.add(cleaned)
                        hits.add(cleaned)
                except EmailValidationError as e:
                    log.debug("Failed to clean email %s: %s", raw, e)

            # 2) Still handle mailto: links explicitly
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if href.lower().startswith("mailto:"):
                    raw = href.split(":", 1)[1].split("?", 1)[0]
                    if raw in seen_raw:
                        continue
                    seen_raw.add(raw)
                    try:
                        cleaned = self.clean_email(raw)
                        if cleaned not in seen_clean:
                            seen_clean.add(cleaned)
                            hits.add(cleaned)
                    except EmailValidationError as e:
                        log.debug("Failed to clean mailto email %s: %s", raw, e)

        except Exception as e:
            log.error("Error extracting emails from HTML: %s", e)

        if url:
            log.debug(" %2d emails on %s (def from html)", len(hits), url)
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
        
        text = self.deobfuscate_emails(text) 
        
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
            log.info(" %2d emails on %s (def from text)", len(hits), url) # to check
        
        return hits

# Create a global email extractor instance
email_extractor = EmailExtractor()
