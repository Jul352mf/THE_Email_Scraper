"""
Enhanced configuration module with improved validation and security.

This module provides a robust configuration system with validation,
environment variable handling, and security features.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Set

from dotenv import load_dotenv

# Initialize logger
log = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Default configuration values
DEFAULT_PARTS = (
    "contact,about,impress,impressum,kontakt,privacy,sales,"
    "investor,procurement,suppliers,urea,adblue,europe,switzerland"
)

class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass

class Config:
    """Enhanced configuration class with validation and security features."""
    
    def __init__(self, env_file: Optional[str] = None):
        """
        Initialize configuration with default values and environment overrides.
        
        Args:
            env_file: Optional path to .env file to load
        """
            
        # API credentials
        self.api_key = os.getenv("GOOGLE_API_KEY", "")
        self.cx_id = os.getenv("GOOGLE_CX_ID", "")
        
        # Priority path parts for sitemap filtering
        self.priority_parts = [
            p.strip().lower() 
            for p in os.getenv("PRIORITY_PATH_PARTS", DEFAULT_PARTS).split(",") 
            if p.strip()
        ]
        
        # Page limits and quotas
        self.max_fallback_pages = self._parse_int("MAX_FALLBACK_PAGES", 12, 1, 500)
        
        # PDF processing
        self.process_pdfs = self._parse_bool("PROCESS_PDFS", False)
        
        # SSL verification
        self.insecure_ssl = self._parse_bool("ALLOW_INSECURE_SSL", False)
        
        # Threading and concurrency
        self.max_workers = self._parse_int("MAX_WORKERS", 4, 1, 64)
        
        # Google API settings
        self.google_safe_interval = self._parse_float("GOOGLE_SAFE_INTERVAL", 0.8, 0.1, 10.0)
        self.google_max_retries = self._parse_int("GOOGLE_MAX_RETRIES", 5, 1, 10)
        
        # Domain scoring
        self.domain_score_threshold = self._parse_int("DOMAIN_SCORE_THRESHOLD", 60, 0, 100)
        
        # HTTP settings
        self.max_redirects = self._parse_int("MAX_REDIRECTS", 5, 0, 100)
        self.max_url_length = self._parse_int("MAX_URL_LENGTH", 2000, 100, 10000)
        self.request_timeout = (
            self._parse_int("CONNECTION_TIMEOUT", 10, 1, 120),
            self._parse_int("READ_TIMEOUT", 20, 1, 120)
        )
        
        # Crawl throttling (seconds)
        self.min_crawl_delay = self._parse_float("MIN_CRAWL_DELAY", 0.5, 0.0, 60.0)
        self.max_crawl_delay = self._parse_float("MAX_CRAWL_DELAY", 2.0, 0.0, 60.0)
        
        # User agent
        self.user_agents = [
            # Chrome Desktop (Windows 10)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            # Chrome Desktop (macOS)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            # Firefox Desktop (Windows 10)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
            "Gecko/20100101 Firefox/124.0",
            # Safari Desktop (macOS)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
            # Edge Desktop (Windows 10)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            # Opera Desktop (Windows 10)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 OPR/100.0.0.0",
            # Chrome on Android
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
            # Firefox on Android
            "Mozilla/5.0 (Android 14; Mobile; rv:124.0) Gecko/124.0 Firefox/124.0",
            # Safari on iOS
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 "
            "Mobile/15E148 Safari/604.1",
            # Samsung Internet on Android
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 "
            "(KHTML, like Gecko) SamsungBrowser/24.0 Chrome/124.0.0.0 "
            "Mobile Safari/537.36",
            # Edge on iOS
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/124.0.0.0 "
            "Mobile/15E148 Safari/605.1.15",
        ]
        
        p_list = os.getenv("PROXIES", "")
        self.proxies = [p.strip() for p in p_list.split(",") if p.strip()]
        
        # Sitemap file names
        self.sitemap_filenames = (
            "sitemap.xml", 
            "sitemap_index.xml", 
            "sitemap-index.xml", 
            "sitemap1.xml"
        )
        # per‐sitemap URL parse limit
        # controls how many <loc> entries we consume from each sitemap
        self.max_urls_per_sitemap = self._parse_int(
            "MAX_URLS_PER_SITEMAP", 10_000, 1, 100_000
        )
        
        # Security settings
        self.allowed_schemes: Set[str] = {"http", "https"}
        self.blocked_domains: Set[str] = set()
        
        # Load blocked domains if provided
        blocked_domains_str = os.getenv("BLOCKED_DOMAINS", "")
        if blocked_domains_str:
            self.blocked_domains = {d.strip().lower() for d in blocked_domains_str.split(",") if d.strip()}
    
    def _parse_int(self, env_var: str, default: int, min_val: int, max_val: int) -> int:
        """
        Parse an integer environment variable with range validation.
        
        Args:
            env_var: Environment variable name
            default: Default value if not set
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            Parsed integer value
        """
        try:
            value = int(os.getenv(env_var, str(default)))
            if value < min_val:
                log.warning("%s value %d below minimum %d, using minimum", env_var, value, min_val)
                return min_val
            if value > max_val:
                log.warning("%s value %d above maximum %d, using maximum", env_var, value, max_val)
                return max_val
            return value
        except ValueError:
            log.warning("Invalid %s value, using default %d", env_var, default)
            return default
    
    def _parse_float(self, env_var: str, default: float, min_val: float, max_val: float) -> float:
        """
        Parse a float environment variable with range validation.
        
        Args:
            env_var: Environment variable name
            default: Default value if not set
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            Parsed float value
        """
        try:
            value = float(os.getenv(env_var, str(default)))
            if value < min_val:
                log.warning("%s value %f below minimum %f, using minimum", env_var, value, min_val)
                return min_val
            if value > max_val:
                log.warning("%s value %f above maximum %f, using maximum", env_var, value, max_val)
                return max_val
            return value
        except ValueError:
            log.warning("Invalid %s value, using default %f", env_var, default)
            return default
    
    def _parse_bool(self, env_var: str, default: bool) -> bool:
        """
        Parse a boolean environment variable.
        
        Args:
            env_var: Environment variable name
            default: Default value if not set
            
        Returns:
            Parsed boolean value
        """
        value = os.getenv(env_var, "")
        if not value:
            return default
        return value.lower() in {"1", "true", "yes", "y", "on"}
    
    def as_dict(self) -> Dict[str, Any]:
        """
        Return configuration as a dictionary.
        
        Returns:
            Dictionary of configuration values
        """
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return a list of error messages.
        
        Returns:
            List of error messages, empty if valid
        """
        errors = []
        
        if not self.api_key:
            errors.append("GOOGLE_API_KEY is missing")
        
        if not self.cx_id:
            errors.append("GOOGLE_CX_ID is missing")
        
        if self.max_workers < 1:
            errors.append("MAX_WORKERS must be at least 1")
        
        if self.max_fallback_pages < 1:
            errors.append("MAX_FALLBACK_PAGES must be at least 1")
        
        return errors
    
    def validate_or_raise(self) -> None:
        """
        Validate configuration and raise an exception if invalid.
        
        Raises:
            ConfigurationError: If configuration is invalid
        """
        errors = self.validate()
        if errors:
            error_msg = "Configuration errors: " + ", ".join(errors)
            log.error(error_msg)
            raise ConfigurationError(error_msg)
    
    def is_domain_blocked(self, domain: str) -> bool:
        """
        Check if a domain is in the blocked domains list.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if domain is blocked, False otherwise
        """
        domain = domain.lower()
        # Remove www. prefix for comparison
        if domain.startswith("www."):
            domain = domain[4:]
        return domain in self.blocked_domains
    
    def update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """
        Update configuration from a dictionary.
        
        Args:
            config_dict: Dictionary of configuration values
        """
        for key, value in config_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)

# Create a global configuration instance
config = Config()

# Export API credentials for backward compatibility
API_KEY = config.api_key
CX_ID = config.cx_id
