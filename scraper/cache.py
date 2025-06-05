"""
Cache module for persistent storage of results.

This module provides functionality for caching search results, domain scores,
and other data to avoid redundant API calls and improve performance.
"""

import json
import logging
import os
import pickle
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, Set, Tuple, Generic, TypeVar, Union, List

# Initialize logger
log = logging.getLogger(__name__)

# Type variable for generic cache value type
T = TypeVar('T')

class CacheError(Exception):
    """Exception raised for cache errors."""
    pass

class Cache(Generic[T]):
    """
    Generic cache implementation with disk persistence.
    
    This class provides:
    - In-memory caching with configurable TTL
    - Disk persistence for cache durability
    - Thread-safe operations
    - Automatic cache pruning
    """
    
    def __init__(self, name: str, 
                 cache_dir: Optional[str] = None,
                 ttl: int = 86400,  # 24 hours default TTL
                 max_size: int = 10000):
        """
        Initialize the cache.
        
        Args:
            name: Cache name (used for file naming)
            cache_dir: Directory to store cache files (defaults to ~/.scraper_cache)
            ttl: Time-to-live in seconds for cache entries
            max_size: Maximum number of entries in memory cache
        """
        self.name = name
        self.ttl = ttl
        self.max_size = max_size
        
        # Set up cache directory
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".scraper_cache"
            
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache file path
        self.cache_file = self.cache_dir / f"{name}_cache.json"
        
        # In-memory cache
        self.cache: Dict[str, Tuple[T, float]] = {}  # key -> (value, timestamp)
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Load cache from disk
        self._load_cache()
    
    def _load_cache(self) -> None:
        """Load cache from disk."""
        if not self.cache_file.exists():
            return
            
        try:
            with self.lock:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    
                # Convert loaded data to cache format
                for key, entry in data.items():
                    if isinstance(entry, list) and len(entry) == 2:
                        value, timestamp = entry
                        # Only load non-expired entries
                        if time.time() - timestamp <= self.ttl:
                            self.cache[key] = (value, timestamp)
                            
                log.debug("[%s] Loaded %d entries from cache file", self.name, len(self.cache))
                
        except Exception as e:
            log.warning("[%s] Failed to load cache from %s: %s", 
                       self.name, self.cache_file, e)
    
    def _save_cache(self) -> None:
        """Save cache to disk."""
        try:
            with self.lock:
                # Convert cache to serializable format
                data = {key: [value, ts] for key, (value, ts) in self.cache.items()}
                
                with open(self.cache_file, 'w') as f:
                    json.dump(data, f)
                    
                log.debug("[%s] Saved %d entries to cache file", self.name, len(self.cache))
                
        except Exception as e:
            log.warning("[%s] Failed to save cache to %s: %s", 
                       self.name, self.cache_file, e)
    
    def _prune_cache(self) -> None:
        """Remove expired entries and limit cache size."""
        with self.lock:
            # Current time
            now = time.time()
            
            # Remove expired entries
            expired_keys = [
                key for key, (_, timestamp) in self.cache.items()
                if now - timestamp > self.ttl
            ]
            
            for key in expired_keys:
                del self.cache[key]
                
            # If still too large, remove oldest entries
            if len(self.cache) > self.max_size:
                # Sort by timestamp (oldest first)
                sorted_items = sorted(self.cache.items(), key=lambda x: x[1][1])
                
                # Remove oldest entries
                to_remove = len(self.cache) - self.max_size
                for key, _ in sorted_items[:to_remove]:
                    del self.cache[key]
                    
            log.debug("[%s] Pruned cache: removed %d expired entries, %d entries remaining", 
                     self.name, len(expired_keys), len(self.cache))
    
    def get(self, key: str) -> Optional[T]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found or expired
        """
        with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                
                # Check if expired
                if time.time() - timestamp > self.ttl:
                    del self.cache[key]
                    return None
                    
                return value
                
            return None
    
    def set(self, key: str, value: T) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        with self.lock:
            # Add to cache with current timestamp
            self.cache[key] = (value, time.time())
            
            # Prune cache if it's getting too large
            if len(self.cache) >= self.max_size:
                self._prune_cache()
                
            # Periodically save to disk (10% chance)
            if hash(key) % 10 == 0:
                self._save_cache()
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was found and deleted, False otherwise
        """
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear the entire cache."""
        with self.lock:
            self.cache.clear()
            self._save_cache()
    
    def keys(self) -> Set[str]:
        """
        Get all keys in the cache.
        
        Returns:
            Set of cache keys
        """
        with self.lock:
            return set(self.cache.keys())
    
    def size(self) -> int:
        """
        Get the number of entries in the cache.
        
        Returns:
            Number of cache entries
        """
        with self.lock:
            return len(self.cache)
    
    def save(self) -> None:
        """Explicitly save the cache to disk."""
        self._save_cache()
    
    def __enter__(self) -> 'Cache[T]':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.save()

class BinaryCache(Generic[T]):
    """
    Binary cache implementation for larger objects using pickle.
    
    This class provides:
    - In-memory caching with configurable TTL
    - Disk persistence using pickle for complex objects
    - Thread-safe operations
    - Automatic cache pruning
    """
    
    def __init__(self, name: str, 
                 cache_dir: Optional[str] = None,
                 ttl: int = 86400,  # 24 hours default TTL
                 max_size: int = 1000):
        """
        Initialize the binary cache.
        
        Args:
            name: Cache name (used for file naming)
            cache_dir: Directory to store cache files (defaults to ~/.scraper_cache)
            ttl: Time-to-live in seconds for cache entries
            max_size: Maximum number of entries in memory cache
        """
        self.name = name
        self.ttl = ttl
        self.max_size = max_size
        
        # Set up cache directory
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".scraper_cache" / name
            
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Index file path
        self.index_file = self.cache_dir / "index.json"
        
        # In-memory cache
        self.cache: Dict[str, Tuple[T, float]] = {}  # key -> (value, timestamp)
        
        # Index of all keys and timestamps
        self.index: Dict[str, float] = {}  # key -> timestamp
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Load index from disk
        self._load_index()
    
    def _get_cache_path(self, key: str) -> Path:
        """
        Get the file path for a cache key.
        
        Args:
            key: Cache key
            
        Returns:
            Path to cache file
        """
        # Use hash of key for filename to avoid invalid characters
        hashed = str(abs(hash(key)))
        return self.cache_dir / f"{hashed}.pickle"
    
    def _load_index(self) -> None:
        """Load index from disk."""
        if not self.index_file.exists():
            return
            
        try:
            with self.lock:
                with open(self.index_file, 'r') as f:
                    self.index = json.load(f)
                    
                log.debug("[%s] Loaded index with %d entries", self.name, len(self.index))
                
        except Exception as e:
            log.warning("[%s] Failed to load index from %s: %s", 
                       self.name, self.index_file, e)
            self.index = {}
    
    def _save_index(self) -> None:
        """Save index to disk."""
        try:
            with self.lock:
                with open(self.index_file, 'w') as f:
                    json.dump(self.index, f)
                    
                log.debug("[%s] Saved index with %d entries", self.name, len(self.index))
                
        except Exception as e:
            log.warning("[%s] Failed to save index to %s: %s", 
                       self.name, self.index_file, e)
    
    def _prune_cache(self) -> None:
        """Remove expired entries and limit cache size."""
        with self.lock:
            # Current time
            now = time.time()
            
            # Find expired entries
            expired_keys = [
                key for key, timestamp in self.index.items()
                if now - timestamp > self.ttl
            ]
            
            # Remove expired entries
            for key in expired_keys:
                self._remove_entry(key)
                
            # If still too many entries, remove oldest
            if len(self.index) > self.max_size:
                # Sort by timestamp (oldest first)
                sorted_items = sorted(self.index.items(), key=lambda x: x[1])
                
                # Remove oldest entries
                to_remove = len(self.index) - self.max_size
                for key, _ in sorted_items[:to_remove]:
                    self._remove_entry(key)
                    
            log.debug("[%s] Pruned cache: removed %d expired entries, %d entries remaining", 
                     self.name, len(expired_keys), len(self.index))
            
            # Save updated index
            self._save_index()
    
    def _remove_entry(self, key: str) -> None:
        """
        Remove an entry from cache and disk.
        
        Args:
            key: Cache key to remove
        """
        # Remove from in-memory cache
        if key in self.cache:
            del self.cache[key]
            
        # Remove from index
        if key in self.index:
            del self.index[key]
            
        # Remove from disk
        cache_path = self._get_cache_path(key)
        if cache_path.exists():
            try:
                os.remove(cache_path)
            except Exception as e:
                log.warning("[%s] Failed to remove cache file %s: %s", 
                           self.name, cache_path, e)
    
    def get(self, key: str) -> Optional[T]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found or expired
        """
        with self.lock:
            # Check in-memory cache first
            if key in self.cache:
                value, timestamp = self.cache[key]
                
                # Check if expired
                if time.time() - timestamp > self.ttl:
                    self._remove_entry(key)
                    return None
                    
                return value
                
            # Check if key exists in index
            if key in self.index:
                timestamp = self.index[key]
                
                # Check if expired
                if time.time() - timestamp > self.ttl:
                    self._remove_entry(key)
                    return None
                    
                # Load from disk
                cache_path = self._get_cache_path(key)
                if cache_path.exists():
                    try:
                        with open(cache_path, 'rb') as f:
                            value = pickle.load(f)
                            
                        # Add to in-memory cache
                        self.cache[key] = (value, timestamp)
                        
                        return value
                        
                    except Exception as e:
                        log.warning("[%s] Failed to load cache entry %s: %s", 
                                   self.name, key, e)
                        self._remove_entry(key)
                
            return None
    
    def set(self, key: str, value: T) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        with self.lock:
            # Current timestamp
            timestamp = time.time()
            
            # Add to in-memory cache
            self.cache[key] = (value, timestamp)
            
            # Update index
            self.index[key] = timestamp
            
            # Save to disk
            cache_path = self._get_cache_path(key)
            try:
                with open(cache_path, 'wb') as f:
                    pickle.dump(value, f)
            except Exception as e:
                log.warning("[%s] Failed to save cache entry %s: %s", 
                           self.name, key, e)
                return
                
            # Periodically save index and prune cache
            if len(self.index) % 10 == 0:
                self._save_index()
                
            # Prune cache if it's getting too large
            if len(self.index) >= self.max_size:
                self._prune_cache()
    
    def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key was found and deleted, False otherwise
        """
        with self.lock:
            if key in self.index:
                self._remove_entry(key)
                self._save_index()
                return True
            return False
    
    def clear(self) -> None:
        """Clear the entire cache."""
        with self.lock:
            # Clear in-memory cache
            self.cache.clear()
            
            # Remove all cache files
            for key in list(self.index.keys()):
                self._remove_entry(key)
                
            # Clear index
            self.index.clear()
            self._save_index()
    
    def keys(self) -> Set[str]:
        """
        Get all keys in the cache.
        
        Returns:
            Set of cache keys
        """
        with self.lock:
            return set(self.index.keys())
    
    def size(self) -> int:
        """
        Get the number of entries in the cache.
        
        Returns:
            Number of cache entries
        """
        with self.lock:
            return len(self.index)
    
    def save(self) -> None:
        """Explicitly save the index to disk."""
        self._save_index()
    
    def __enter__(self) -> 'BinaryCache[T]':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.save()

# Create global cache instances
google_cache = Cache[List[Dict[str, Any]]](name="google_search")
domain_score_cache = Cache[int](name="domain_score")
email_cache = BinaryCache[Set[str]](name="email_extraction")
