"""
Enhanced domain scoring module with improved validation and error handling.

This module provides robust domain scoring functionality with proper error handling,
validation, and security features.
"""

import logging
import re
from typing import Tuple, List, Dict, Any, Optional, Set

from rapidfuzz import fuzz
import tldextract

from scraper.config import config
from scraper.http import normalise_domain

# Initialize logger
log = logging.getLogger(__name__)

class DomainScoringError(Exception):
    """Exception raised for domain scoring errors."""
    pass

class DomainScorer:
    """Enhanced domain scorer with improved validation and error handling."""
    
    def __init__(self):
        """Initialize the domain scorer with validation settings."""
        # Social media and generic domains that should be penalized
        self.penalty_domains: Set[str] = {
            "linkedin.com", "facebook.com", "instagram.com", "twitter.com",
            "youtube.com", "medium.com", "github.com", "glassdoor.com",
            "indeed.com", "crunchbase.com", "bloomberg.com", "wikipedia.org"
        }
        
        # Default penalty for social media and generic domains
        self.social_penalty = 25
        
        # Minimum company name length for reliable scoring
        self.min_company_length = 3
    
    def clean_company_name(self, company: str) -> str:
        """
        Clean company name for comparison.
        
        Args:
            company: Company name to clean
            
        Returns:
            Cleaned company name
        """
        if not company:
            return ""
            
        # Remove common legal suffixes
        suffixes = [
            " inc", " inc.", " incorporated", " llc", " ltd", " ltd.", " limited",
            " gmbh", " ag", " corp", " corp.", " corporation", " co", " co."
        ]
        
        cleaned = company.lower()
        for suffix in suffixes:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)]
                break
        
        # Remove special characters
        return re.sub(r"[^a-z0-9]", "", cleaned)
    
    def score_domain(self, company: str, url: str) -> int:
        """
        Score the relevance of a domain to a company name with enhanced validation.
        
        Args:
            company: Company name to match
            url: URL to score
            
        Returns:
            Relevance score (0-100)
            
        Raises:
            DomainScoringError: If scoring fails
        """
        if not company or not url:
            return 0
            
        try:
            # Extract domain from URL
            host = normalise_domain(url)
            
            # Clean company name for comparison
            base = self.clean_company_name(company)
            
            # Skip scoring if company name is too short
            if len(base) < self.min_company_length:
                log.warning("Company name too short for reliable scoring: %s", company)
                return 50  # Return neutral score
            
            # Apply penalty for social media and generic domains
            penalty = 0
            for penalty_domain in self.penalty_domains:
                if penalty_domain in host:
                    penalty = self.social_penalty
                    log.debug("Applied penalty to %s (contains %s)", host, penalty_domain)
                    break
            
            # Extract domain parts
            ext = tldextract.extract(host)
            
            # Calculate similarity scores for domain and subdomain
            domain_score = fuzz.partial_ratio(base, ext.domain or "")
            subdomain_score = fuzz.partial_ratio(base, ext.subdomain or "")
            
            # Take the maximum score
            s = max(domain_score, subdomain_score)
            
            # Apply penalty and ensure non-negative score
            final_score = max(0, s - penalty)
            
            log.debug("Domain score for %s and %s: %d (base: %d, penalty: %d)", 
                     company, host, final_score, s, penalty)
            
            return final_score
            
        except Exception as e:
            log.error("Error scoring domain %s for company %s: %s", url, company, e)
            raise DomainScoringError(f"Error scoring domain: {e}")
    
    def find_best_domain(self, company: str, search_results: List[Dict[str, Any]]) -> Tuple[int, str]:
        """
        Find the best matching domain from search results with enhanced validation.
        
        Args:
            company: Company name to match
            search_results: List of search result items
            
        Returns:
            Tuple of (score, domain URL)
            
        Raises:
            DomainScoringError: If domain finding fails
        """
        if not search_results:
            return 0, ""
        
        try:
            # Score each domain and find the best match
            scored_domains = []
            
            for result in search_results:
                link = result.get("link", "")
                if not link:
                    continue
                    
                try:
                    score = self.score_domain(company, link)
                    scored_domains.append((score, link))
                except Exception as e:
                    log.warning("Error scoring domain %s: %s", link, e)
            
            # If no valid domains found
            if not scored_domains:
                return 0, ""
                
            # Find the best match
            best_domain = max(scored_domains, key=lambda t: t[0])
            
            log.debug("Best domain for %s: %s (score: %d)", 
                     company, best_domain[1], best_domain[0])
            
            return best_domain
            
        except Exception as e:
            log.error("Error finding best domain for %s: %s", company, e)
            raise DomainScoringError(f"Error finding best domain: {e}")
    
    def is_domain_relevant(self, company: str, url: str) -> bool:
        """
        Determine if a domain is relevant to a company name with enhanced validation.
        
        Args:
            company: Company name to match
            url: URL to check
            
        Returns:
            True if domain is relevant, False otherwise
        """
        try:
            score = self.score_domain(company, url)
            
            # Log rejected domains
            if score < config.domain_score_threshold:
                log.info("Domain score too low (%d < %d): %s for company %s", 
                        score, config.domain_score_threshold, url, company)
                return False
                
            return True
            
        except Exception as e:
            log.error("Error checking domain relevance for %s and %s: %s", 
                     company, url, e)
            return False

# Create a global domain scorer instance
domain_scorer = DomainScorer()
