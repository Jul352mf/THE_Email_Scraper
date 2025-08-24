"""
Enhanced configuration module with improved validation and security.

This module provides a robust configuration system with validation,
environment variable handling, security features, and centralized worker
management to prevent thread pool nesting.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Set
from threading import Lock

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

# High-priority parts for smart email discovery (ordered by priority)
HIGH_PRIORITY_PARTS = [
    "contact", "contacts", "kontakt", "contacto", "contato",
    "about", "about-us", "aboutus", "uber-uns", "quienes-somos",
    "team", "teams", "staff", "people", "leadership", "management",
    "careers", "jobs", "employment", "work-with-us"
]

# Medium-priority parts
MEDIUM_PRIORITY_PARTS = [
    "impress", "impressum", "legal", "privacy", "terms",
    "sales", "support", "help", "service", "customer-service"
]

class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass


class WorkerManager:
    """Centralized worker count management to prevent thread pool nesting."""
    
    def __init__(self, base_workers: int = 4):
        """Initialize with base worker count."""
        self._base_workers = max(1, base_workers)
        self._allocation_lock = Lock()
        self._active_pools: Dict[str, int] = {}
        
    @property
    def base_workers(self) -> int:
        """Get base worker count."""
        return self._base_workers
        
    def set_base_workers(self, count: int):
        """Set base worker count."""
        self._base_workers = max(1, count)
        
    def get_workers_for_task(self, task_name: str,
                             total_items: int = None) -> int:
        """
        Get optimal worker count for a specific task.
        
        Args:
            task_name: Name of the task (for debugging/logging)
            total_items: Number of items to process (optional)
            
        Returns:
            Optimal worker count for the task
        """
        with self._allocation_lock:
            # Different allocation strategies based on task
            if task_name == "domain_probe":
                # Light probing, use fewer workers
                workers = min(self._base_workers // 2, 4)
            elif task_name == "sitemap_download":
                # Sitemap downloads, moderate workers
                workers = min(4, total_items or self._base_workers)
            elif task_name == "main_processing":
                # Main processing gets most workers
                workers = self._base_workers
            elif task_name == "url_crawling":
                # URL crawling within a domain, use fewer to avoid overwhelming
                workers = min(self._base_workers // 3, 3)
            else:
                # Default allocation
                workers = max(1, self._base_workers // 2)
                
            # Limit by available items if provided
            if total_items is not None:
                workers = min(workers, total_items)
                
            # Ensure minimum of 1 worker
            workers = max(1, workers)
            
            # Track active allocations (for debugging)
            self._active_pools[task_name] = workers
            log.debug(f"Allocated {workers} workers for task: {task_name}")
            
            return workers
    
    def release_workers_for_task(self, task_name: str):
        """Release workers allocated for a task."""
        with self._allocation_lock:
            if task_name in self._active_pools:
                del self._active_pools[task_name]
                log.debug(f"Released workers for task: {task_name}")
    
    def get_active_allocations(self) -> Dict[str, int]:
        """Get current worker allocations for debugging."""
        with self._allocation_lock:
            return self._active_pools.copy()


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
        self.max_fallback_pages = self._parse_int("MAX_FALLBACK_PAGES", 50, 1, 500)
        
        # PDF processing
        self.process_pdfs = self._parse_bool("PROCESS_PDFS", False)
        
        # SSL verification
        self.insecure_ssl = self._parse_bool("ALLOW_INSECURE_SSL", False)
        
        # Threading and concurrency
        self.max_workers = self._parse_int("MAX_WORKERS", 4, 1, 64)
        
        # Initialize centralized worker manager
        self.worker_manager = WorkerManager(self.max_workers)
        
        # Google API settings
        self.google_safe_interval = self._parse_float("GOOGLE_SAFE_INTERVAL", 0.8, 0.1, 10.0)
        self.google_max_retries = self._parse_int("GOOGLE_MAX_RETRIES", 5, 1, 10)
        
        # Domain scoring
        self.domain_score_threshold = self._parse_int("DOMAIN_SCORE_THRESHOLD", 60, 0, 100)
        
        # Smart email discovery settings
        self.enable_smart_discovery = self._parse_bool(
            "ENABLE_SMART_DISCOVERY", True)
        # Disabled to find ALL emails per company
        self.enable_early_stopping = self._parse_bool(
            "ENABLE_EARLY_STOPPING", False)
        # Much higher threshold
        self.early_stop_threshold = self._parse_int(
            "EARLY_STOP_THRESHOLD", 100, 1, 1000)
        # Increased for more thorough search
        self.max_priority_pages = self._parse_int(
            "MAX_PRIORITY_PAGES", 25, 1, 100)
        self.enable_email_pattern_cache = self._parse_bool(
            "ENABLE_EMAIL_PATTERN_CACHE", True)
        
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

        # Async orchestration tuning (new)
    # Controls how many companies processed concurrently
    # (can override orchestrator init)
        self.async_company_concurrency = self._parse_int(
            "ASYNC_COMPANY_CONCURRENCY", 50, 1, 500
        )
        # Max parallel page fetches within a single domain
        self.async_per_domain_page_concurrency = self._parse_int(
            "ASYNC_PER_DOMAIN_PAGE_CONCURRENCY", 3, 1, 20
        )
        # Whether to skip generic discovery when sitemap produced priority URLs
        self.enable_sitemap_first = self._parse_bool(
            "ENABLE_SITEMAP_FIRST", True
        )
        # Enable Google dorking (site:domain "@domain")
        # to harvest leaked emails
        self.enable_google_dork = self._parse_bool(
            "ENABLE_GOOGLE_DORK", True
        )
        # Limit of dork result pages / queries
        self.google_dork_max_results = self._parse_int(
            "GOOGLE_DORK_MAX_RESULTS", 5, 1, 20
        )
        # Early stop absolute cap (extra guard besides EARLY_STOP_THRESHOLD)
        self.max_emails_per_domain = self._parse_int(
            "MAX_EMAILS_PER_DOMAIN", 150, 1, 5000
        )
        # Allow generic role emails (info@, support@, sales@) to be kept
        self.allow_generic_role_emails = self._parse_bool(
            "ALLOW_GENERIC_ROLE_EMAILS", True
        )
        # Debug: log raw vs filtered email candidates per domain extraction
        self.debug_email_candidates = self._parse_bool(
            "DEBUG_EMAIL_CANDIDATES", True
        )
    # Email candidate threshold: when reached we can skip
    # sitemap/extra phases or early stop (config dependent)
        self.email_candidate_threshold = self._parse_int(
            "EMAIL_CANDIDATE_THRESHOLD", 25, 1, 10_000
        )
    # Minimal email validation: keep almost everything that
    # syntactically looks like an email
        self.minimal_email_validation = self._parse_bool(
            "MINIMAL_EMAIL_VALIDATION", True
        )
        # Use simplified fuzzy domain match instead of complex scorer penalties
        self.simple_domain_match = self._parse_bool(
            "SIMPLE_DOMAIN_MATCH", False
        )
        # Fallback: guess domain if Google returns nothing
        self.enable_domain_guessing = self._parse_bool(
            "ENABLE_DOMAIN_GUESSING", True
        )
    # Deprecated: previously enabled harvesting emails from
    # Google snippets. Logic removed from async orchestrator for
    # precision parity; flag kept only to avoid attribute errors
    # if env var still set.
        self.enable_snippet_email_harvest = self._parse_bool(
            "ENABLE_SNIPPET_EMAIL_HARVEST", False
        )
        
        # Security settings
        self.allowed_schemes: Set[str] = {"http", "https"}
        self.blocked_domains: Set[str] = set()
        
        # Load blocked domains if provided
        blocked_domains_str = os.getenv("BLOCKED_DOMAINS", "")
        if blocked_domains_str:
            self.blocked_domains = {
                d.strip().lower()
                for d in blocked_domains_str.split(",")
                if d.strip()
            }
    
    def _parse_int(
        self, env_var: str, default: int, min_val: int, max_val: int
    ) -> int:
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
                log.warning(
                    "%s value %d below minimum %d, using minimum",
                    env_var,
                    value,
                    min_val,
                )
                return min_val
            if value > max_val:
                log.warning(
                    "%s value %d above maximum %d, using maximum",
                    env_var,
                    value,
                    max_val,
                )
                return max_val
            return value
        except ValueError:
            log.warning("Invalid %s value, using default %d", env_var, default)
            return default
    
    def _parse_float(
        self, env_var: str, default: float, min_val: float, max_val: float
    ) -> float:
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
                log.warning(
                    "%s value %f below minimum %f, using minimum",
                    env_var,
                    value,
                    min_val,
                )
                return min_val
            if value > max_val:
                log.warning(
                    "%s value %f above maximum %f, using maximum",
                    env_var,
                    value,
                    max_val,
                )
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
        return {
            k: v for k, v in self.__dict__.items() if not k.startswith('_')
        }
    
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
                # Update worker manager if max_workers changed
                if key == 'max_workers':
                    self.worker_manager.set_base_workers(value)
    
    def update_max_workers(self, count: int) -> None:
        """
        Update max workers and sync with worker manager.
        
        Args:
            count: New worker count
        """
        self.max_workers = max(1, count)
        self.worker_manager.set_base_workers(self.max_workers)
        log.info(f"Updated max workers to {self.max_workers}")

# Create a global configuration instance
config = Config()
# Backward-compat alias (legacy code may import settings)
settings = config  # deprecated alias
__all__ = ["Config", "ConfigurationError", "config", "settings"]


# Export API credentials for backward compatibility
API_KEY = config.api_key
CX_ID = config.cx_id
