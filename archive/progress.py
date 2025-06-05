"""
Progress reporting module for tracking and displaying task progress.

This module provides functionality for tracking and displaying progress
of long-running operations with various output formats.
"""

import logging
import sys
import threading
import time
from typing import Optional, Callable, Dict, Any, List

# Initialize logger
log = logging.getLogger(__name__)

class ProgressTracker:
    """
    Progress tracker for monitoring and reporting task progress.
    
    This class provides:
    - Real-time progress tracking
    - Multiple output formats (console, log, callback)
    - ETA calculation
    - Thread-safe operations
    """
    
    def __init__(self, total: int, 
                 description: str = "Progress", 
                 unit: str = "items",
                 console: bool = True,
                 log_interval: int = 10):
        """
        Initialize the progress tracker.
        
        Args:
            total: Total number of items to process
            description: Description of the task
            unit: Unit name for items (e.g., "items", "companies", "emails")
            console: Whether to display progress on console
            log_interval: Interval in seconds for logging progress
        """
        self.total = max(1, total)  # Ensure total is at least 1
        self.description = description
        self.unit = unit
        self.console = console
        self.log_interval = log_interval
        
        # Progress state
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_log_time = self.start_time
        self.completed = False
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Callback function
        self.callback: Optional[Callable[[Dict[str, Any]], None]] = None
        
        # Initial display
        if self.console:
            self._display_progress()
    
    def update(self, increment: int = 1) -> None:
        """
        Update progress by the specified increment.
        
        Args:
            increment: Amount to increment progress by
        """
        with self.lock:
            self.current += increment
            self.current = min(self.current, self.total)
            
            now = time.time()
            
            # Update display if enough time has passed or we're done
            if (now - self.last_update_time >= 0.1 or 
                self.current >= self.total):
                self.last_update_time = now
                
                if self.console:
                    self._display_progress()
                    
                # Call callback if set
                if self.callback:
                    self.callback(self.get_stats())
            
            # Log progress at specified interval
            if now - self.last_log_time >= self.log_interval:
                self.last_log_time = now
                self._log_progress()
            
            # Mark as completed if done
            if self.current >= self.total and not self.completed:
                self.completed = True
                self._on_complete()
    
    def set_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Set a callback function for progress updates.
        
        Args:
            callback: Function to call with progress stats
        """
        self.callback = callback
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current progress statistics.
        
        Returns:
            Dictionary of progress statistics
        """
        with self.lock:
            elapsed = time.time() - self.start_time
            percent = min(100.0, self.current / self.total * 100)
            
            # Calculate items per second
            items_per_sec = self.current / elapsed if elapsed > 0 else 0
            
            # Calculate ETA
            if items_per_sec > 0 and self.current < self.total:
                eta_seconds = (self.total - self.current) / items_per_sec
            else:
                eta_seconds = 0
                
            return {
                "description": self.description,
                "current": self.current,
                "total": self.total,
                "percent": percent,
                "elapsed": elapsed,
                "elapsed_str": self._format_time(elapsed),
                "eta": eta_seconds,
                "eta_str": self._format_time(eta_seconds),
                "items_per_sec": items_per_sec,
                "unit": self.unit,
                "completed": self.completed
            }
    
    def _format_time(self, seconds: float) -> str:
        """
        Format time in seconds to a human-readable string.
        
        Args:
            seconds: Time in seconds
            
        Returns:
            Formatted time string
        """
        if seconds < 0:
            return "Unknown"
            
        if seconds < 60:
            return f"{seconds:.1f}s"
            
        minutes, seconds = divmod(seconds, 60)
        if minutes < 60:
            return f"{int(minutes)}m {int(seconds)}s"
            
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours)}h {int(minutes)}m"
    
    def _display_progress(self) -> None:
        """Display progress on the console."""
        stats = self.get_stats()
        
        # Create progress bar
        bar_length = 30
        filled_length = int(bar_length * stats["current"] / stats["total"])
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Format progress line
        line = (
            f"\r{stats['description']}: [{bar}] "
            f"{stats['current']}/{stats['total']} {stats['unit']} "
            f"({stats['percent']:.1f}%) "
            f"[{stats['elapsed_str']} < {stats['eta_str']}] "
            f"({stats['items_per_sec']:.1f} {stats['unit']}/s)"
        )
        
        # Print progress
        sys.stdout.write(line)
        sys.stdout.flush()
        
        # Add newline if completed
        if stats["completed"]:
            sys.stdout.write("\n")
            sys.stdout.flush()
    
    def _log_progress(self) -> None:
        """Log progress to the logger."""
        stats = self.get_stats()
        log.info(
            "%s: %d/%d %s (%.1f%%) [%s elapsed, %s remaining]",
            stats["description"],
            stats["current"],
            stats["total"],
            stats["unit"],
            stats["percent"],
            stats["elapsed_str"],
            stats["eta_str"]
        )
    
    def _on_complete(self) -> None:
        """Handle completion of progress."""
        stats = self.get_stats()
        log.info(
            "%s completed: %d %s in %s (%.1f %s/s)",
            stats["description"],
            stats["total"],
            stats["unit"],
            stats["elapsed_str"],
            stats["items_per_sec"],
            stats["unit"]
        )
    
    def __enter__(self) -> 'ProgressTracker':
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        if not self.completed:
            self.completed = True
            if self.console:
                self._display_progress()

class MultiProgressTracker:
    """
    Tracker for multiple concurrent progress indicators.
    
    This class provides:
    - Tracking of multiple progress indicators
    - Aggregated statistics
    - Thread-safe operations
    """
    
    def __init__(self):
        """Initialize the multi-progress tracker."""
        self.trackers: Dict[str, ProgressTracker] = {}
        self.lock = threading.RLock()
    
    def add_tracker(self, name: str, tracker: ProgressTracker) -> None:
        """
        Add a progress tracker.
        
        Args:
            name: Unique name for the tracker
            tracker: ProgressTracker instance
        """
        with self.lock:
            self.trackers[name] = tracker
    
    def remove_tracker(self, name: str) -> None:
        """
        Remove a progress tracker.
        
        Args:
            name: Name of the tracker to remove
        """
        with self.lock:
            if name in self.trackers:
                del self.trackers[name]
    
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all trackers.
        
        Returns:
            Dictionary of tracker statistics by name
        """
        with self.lock:
            return {name: tracker.get_stats() for name, tracker in self.trackers.items()}
    
    def get_aggregate_stats(self) -> Dict[str, Any]:
        """
        Get aggregated statistics across all trackers.
        
        Returns:
            Dictionary of aggregated statistics
        """
        with self.lock:
            if not self.trackers:
                return {
                    "total": 0,
                    "current": 0,
                    "percent": 0.0,
                    "completed": True
                }
                
            stats = [tracker.get_stats() for tracker in self.trackers.values()]
            
            total_items = sum(s["total"] for s in stats)
            current_items = sum(s["current"] for s in stats)
            percent = (current_items / total_items * 100) if total_items > 0 else 0
            completed = all(s["completed"] for s in stats)
            
            return {
                "total": total_items,
                "current": current_items,
                "percent": percent,
                "completed": completed,
                "trackers_count": len(self.trackers)
            }
    
    def is_completed(self) -> bool:
        """
        Check if all trackers are completed.
        
        Returns:
            True if all trackers are completed, False otherwise
        """
        with self.lock:
            return all(tracker.completed for tracker in self.trackers.values())

# Create a global multi-progress tracker
multi_progress = MultiProgressTracker()
