"""
Smart email discovery module with page prioritization and early stopping.

This module implements intelligent email discovery strategies including:
- Priority-based page ordering (Contact > About > Team > etc.)
- Early stopping when sufficient emails are found
- Email pattern caching per domain
- Adaptive discovery based on success rate
"""

import logging
import re
from typing import List, Dict, Set, Tuple, Optional
from urllib.parse import urlparse, unquote
from collections import defaultdict

from scraper.config import config, HIGH_PRIORITY_PARTS, MEDIUM_PRIORITY_PARTS

log = logging.getLogger(__name__)


class EmailPatternCache:
    """Cache for email patterns discovered per domain."""
    
    def __init__(self):
        self._domain_patterns: Dict[str, Set[str]] = defaultdict(set)
        self._domain_suffixes: Dict[str, Set[str]] = defaultdict(set)
    
    def add_emails(self, domain: str, emails: Set[str]) -> None:
        """Add emails to domain cache and extract patterns."""
        if not emails:
            return
            
        domain = domain.lower().removeprefix("www.")
        
        for email in emails:
            self._domain_patterns[domain].add(email)
            # Extract domain suffix from email
            if '@' in email:
                email_domain = email.split('@')[1].lower()
                self._domain_suffixes[domain].add(email_domain)
    
    def get_patterns(self, domain: str) -> Tuple[Set[str], Set[str]]:
        """Get cached email patterns and suffixes for a domain."""
        domain = domain.lower().removeprefix("www.")
        return (
            self._domain_patterns.get(domain, set()).copy(),
            self._domain_suffixes.get(domain, set()).copy()
        )
    
    def has_patterns(self, domain: str) -> bool:
        """Check if domain has cached patterns."""
        domain = domain.lower().removeprefix("www.")
        return len(self._domain_patterns.get(domain, set())) > 0


class SmartEmailDiscovery:
    """Smart email discovery with prioritization and early stopping."""
    
    def __init__(self):
        self.pattern_cache = EmailPatternCache()
        self._stats = defaultdict(int)
    
    def prioritize_urls(self, urls: List[str]) -> List[str]:
        """
        Prioritize URLs based on likelihood of containing emails.
        
        Args:
            urls: List of URLs to prioritize
            
        Returns:
            Sorted list of URLs with highest priority first
        """
        if not config.enable_smart_discovery:
            return urls
        
        def get_priority_score(url: str) -> Tuple[int, str]:
            """Get priority score for URL (lower is higher priority)."""
            url_lower = url.lower()
            path = urlparse(url).path.lower()
            
            # Extract meaningful parts from path
            path_parts = [p for p in path.split('/') if p]
            url_text = unquote(' '.join(path_parts))
            
            # Check for high-priority keywords (score 0-99)
            for i, keyword in enumerate(HIGH_PRIORITY_PARTS):
                if keyword in url_text or keyword in url_lower:
                    return (i, url)  # Lower index = higher priority
            
            # Check for medium-priority keywords (score 100-199)
            for i, keyword in enumerate(MEDIUM_PRIORITY_PARTS):
                if keyword in url_text or keyword in url_lower:
                    return (100 + i, url)
            
            # Check for other contact-related patterns (score 200-299)
            contact_patterns = [
                r'\bcontact\b', r'\babout\b', r'\bteam\b', r'\bstaff\b',
                r'\bpeople\b', r'\boffice\b', r'\blocation\b', r'\baddress\b'
            ]
            for i, pattern in enumerate(contact_patterns):
                if re.search(pattern, url_text, re.IGNORECASE):
                    return (200 + i, url)
            
            # Prefer shorter, simpler paths (score 300+)
            path_complexity = len(path_parts) + len(url) // 50
            return (300 + path_complexity, url)
        
        # Sort by priority score
        prioritized = sorted(urls, key=get_priority_score)
        
        # Limit to max priority pages if configured
        if config.max_priority_pages > 0:
            prioritized = prioritized[:config.max_priority_pages]
        
        log.debug("Prioritized %d URLs, processing top %d", len(urls), len(prioritized))
        return prioritized
    
    def should_stop_early(self, current_emails: Set[str], total_processed: int) -> bool:
        """
        Determine if we should stop processing more pages.
        
        Args:
            current_emails: Emails found so far
            total_processed: Number of pages processed so far
            
        Returns:
            True if should stop, False to continue
        """
        if not config.enable_early_stopping:
            return False
        
        # Stop if we've found enough emails
        if len(current_emails) >= config.early_stop_threshold:
            log.info("Early stopping: found %d emails (threshold: %d)", 
                    len(current_emails), config.early_stop_threshold)
            return True
        
        # Only stop if we've processed MANY pages without finding emails
        # Changed from 5 to 15 to be less aggressive
        if total_processed >= 15 and len(current_emails) == 0:
            log.info("Early stopping: no emails found after %d pages",
                     total_processed)
            return True
        
        return False
    
    def filter_relevant_emails(self, emails: Set[str], domain: str) -> Set[str]:
        """
        Filter emails to only include those relevant to the domain.
        
        Args:
            emails: Set of email addresses found
            domain: Domain being processed
            
        Returns:
            Filtered set of relevant emails
        """
        if not emails:
            return set()
        
        domain = domain.lower().removeprefix("www.")
        relevant_emails = set()
        
        # Get cached patterns for this domain
        cached_emails, cached_suffixes = self.pattern_cache.get_patterns(domain)
        
        for email in emails:
            email_lower = email.lower()
            
            # Always include emails from the same domain
            if '@' in email_lower:
                email_domain = email_lower.split('@')[1]
                # Remove www. and common subdomains for comparison
                email_domain_clean = email_domain.removeprefix("www.").removeprefix("mail.")
                domain_clean = domain.removeprefix("www.").removeprefix("mail.")
                
                if email_domain_clean == domain_clean:
                    relevant_emails.add(email)
                    continue
                
                # Include if matches cached domain suffixes
                if email_domain in cached_suffixes:
                    relevant_emails.add(email)
                    continue
            
            # Include emails that match cached patterns
            if email_lower in {e.lower() for e in cached_emails}:
                relevant_emails.add(email)
                continue
            
            # Include professional-looking emails (not personal services)
            personal_domains = {
                'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
                'aol.com', 'icloud.com', 'protonmail.com'
            }
            
            if '@' in email_lower:
                email_domain = email_lower.split('@')[1]
                if email_domain not in personal_domains:
                    relevant_emails.add(email)
        
        # Update cache with new emails
        if config.enable_email_pattern_cache:
            self.pattern_cache.add_emails(domain, relevant_emails)
        
        log.debug("Filtered %d emails to %d relevant for domain %s", 
                 len(emails), len(relevant_emails), domain)
        
        return relevant_emails
    
    def get_discovery_stats(self) -> Dict[str, int]:
        """Get discovery statistics."""
        return dict(self._stats)
    
    def reset_stats(self) -> None:
        """Reset discovery statistics."""
        self._stats.clear()


# Global instance
smart_discovery = SmartEmailDiscovery()