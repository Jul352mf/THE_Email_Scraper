"""
Centralized regex pattern caching for performance optimization.

This module provides a thread-safe cache for compiled regex patterns to avoid 
repeated compilation overhead in email extraction and other text processing operations.
"""

import logging
import re
import threading
from typing import Dict, Pattern, Any, Optional
from functools import lru_cache
from collections import OrderedDict

# Initialize logger
log = logging.getLogger(__name__)

class RegexPatternCache:
    """
    Thread-safe cache for compiled regex patterns with LRU eviction and statistics.
    """
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize regex pattern cache.
        
        Args:
            max_size: Maximum number of patterns to cache
        """
        self.max_size = max_size
        self._cache: OrderedDict[str, Pattern] = OrderedDict()
        self._lock = threading.RLock()
        
        # Statistics
        self._hit_count = 0
        self._miss_count = 0
        self._compilation_time = 0.0
    
    def get_pattern(self, pattern: str, flags: int = 0) -> Pattern:
        """
        Get or compile a regex pattern with caching.
        
        Args:
            pattern: Regular expression pattern string
            flags: Regex flags (re.IGNORECASE, etc.)
            
        Returns:
            Compiled regex pattern object
        """
        # Create cache key including flags
        cache_key = f"{pattern}:{flags}"
        
        # Fast path: check cache with read lock
        with self._lock:
            if cache_key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(cache_key)
                self._hit_count += 1
                return self._cache[cache_key]
        
        # Cache miss - compile pattern
        import time
        start_time = time.time()
        
        try:
            compiled_pattern = re.compile(pattern, flags)
        except re.error as e:
            log.error("Failed to compile regex pattern '%s': %s", pattern, e)
            raise
        
        compilation_time = time.time() - start_time
        
        # Store in cache
        with self._lock:
            self._cache[cache_key] = compiled_pattern
            self._miss_count += 1
            self._compilation_time += compilation_time
            
            # LRU eviction if cache is full
            while len(self._cache) > self.max_size:
                oldest_key, _ = self._cache.popitem(last=False)
                log.debug("Evicted regex pattern from cache: %s", oldest_key[:50])
        
        log.debug("Compiled and cached regex pattern: %s (%.3fms)", 
                  pattern[:50], compilation_time * 1000)
        
        return compiled_pattern
    
    def precompile_patterns(self, patterns: Dict[str, Dict[str, Any]]) -> None:
        """
        Precompile a set of patterns for better performance.
        
        Args:
            patterns: Dict of {name: {'pattern': str, 'flags': int}} 
        """
        log.info("Precompiling %d regex patterns", len(patterns))
        
        for name, pattern_info in patterns.items():
            try:
                pattern = pattern_info['pattern']
                flags = pattern_info.get('flags', 0)
                self.get_pattern(pattern, flags)
                log.debug("Precompiled pattern '%s'", name)
            except Exception as e:
                log.warning("Failed to precompile pattern '%s': %s", name, e)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        with self._lock:
            total_requests = self._hit_count + self._miss_count
            hit_rate = (self._hit_count / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'cache_size': len(self._cache),
                'max_size': self.max_size,
                'hit_count': self._hit_count,
                'miss_count': self._miss_count,
                'total_requests': total_requests,
                'hit_rate_percent': round(hit_rate, 2),
                'total_compilation_time_ms': round(self._compilation_time * 1000, 2),
                'average_compilation_time_ms': round(
                    (self._compilation_time / max(self._miss_count, 1)) * 1000, 3
                )
            }
    
    def clear(self) -> None:
        """Clear all cached patterns and reset statistics."""
        with self._lock:
            cleared_count = len(self._cache)
            self._cache.clear()
            self._hit_count = 0
            self._miss_count = 0
            self._compilation_time = 0.0
            
        log.info("Cleared %d cached regex patterns", cleared_count)
    
    def get_most_used_patterns(self, limit: int = 10) -> list:
        """Get list of most frequently requested patterns (by cache order)."""
        with self._lock:
            # Get the most recently used patterns (end of OrderedDict)
            recent_patterns = list(self._cache.items())[-limit:]
            return [(key.split(':', 1)[0], key.split(':', 1)[1]) for key, _ in recent_patterns]

# Global cache instance
_regex_cache = RegexPatternCache(max_size=1000)

# Convenience functions for easy usage
def get_compiled_pattern(pattern: str, flags: int = 0) -> Pattern:
    """
    Get a compiled regex pattern from cache.
    
    Args:
        pattern: Regular expression pattern string
        flags: Regex compilation flags
        
    Returns:
        Compiled Pattern object
    """
    return _regex_cache.get_pattern(pattern, flags)

def precompile_common_patterns() -> None:
    """Precompile commonly used email extraction patterns."""
    
    common_patterns = {
        'email_basic': {
            'pattern': r"(?i)(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\.)+[A-Z]{2,63}(?![A-Z0-9._%+-])",
            'flags': 0
        },
        'mailto': {
            'pattern': r"mailto:",
            'flags': re.IGNORECASE
        },
        'obfuscated_email': {
            'pattern': r"""
            (?P<user>[A-Za-z0-9._%+-]+)              # local-part
            \s*(?:\[\s*at\s*\]|\(\s*at\s*\)|\bat\b)\s*  # obfuscated "at"
            (?P<host>(?:[A-Za-z0-9-]+                  # domain labels
                (?:\s*(?:\[\s*dot\s*\]|\(\s*dot\s*\)|\bdot\b)\s*[A-Za-z0-9-]+)+))  # one or more obf-dot + label
            """,
            'flags': re.IGNORECASE | re.VERBOSE
        },
        'noreply': {
            'pattern': r"noreply@",
            'flags': re.IGNORECASE
        },
        'donotreply': {
            'pattern': r"donotreply@",
            'flags': re.IGNORECASE
        },
        'no_reply': {
            'pattern': r"no-reply@",
            'flags': re.IGNORECASE
        },
        'webmaster': {
            'pattern': r"webmaster@",
            'flags': re.IGNORECASE
        },
        'hostmaster': {
            'pattern': r"hostmaster@",
            'flags': re.IGNORECASE
        },
        'postmaster': {
            'pattern': r"postmaster@",
            'flags': re.IGNORECASE
        },
        'image_extensions': {
            'pattern': r"\.(?:png|jpe?g|gif)$",
            'flags': re.IGNORECASE
        },
        'hex_local_parts': {
            'pattern': r"^[0-9a-f]{20,}$",
            'flags': re.IGNORECASE
        }
    }
    
    _regex_cache.precompile_patterns(common_patterns)
    log.info("Precompiled %d common email extraction patterns", len(common_patterns))

def get_cache_stats() -> Dict[str, Any]:
    """Get regex cache performance statistics."""
    return _regex_cache.get_stats()

def clear_cache() -> None:
    """Clear regex pattern cache."""
    _regex_cache.clear()

def get_cache_instance() -> RegexPatternCache:
    """Get the global regex cache instance for advanced usage."""
    return _regex_cache

# Initialize common patterns on import
precompile_common_patterns()