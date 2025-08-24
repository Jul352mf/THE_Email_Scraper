"""
Progress tracking for email scraping operations.

This module provides real-time progress tracking with live statistics display
using the Rich library for enhanced terminal UI.
"""

import threading
import time
from typing import Optional, Dict, Any
from collections import Counter
from rich.progress import (
    Progress,
    TaskID,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text
from rich.console import Console
import logging

log = logging.getLogger(__name__)


class ProgressTracker:
    """Thread-safe progress tracker with live statistics display."""
    
    def __init__(self, total_companies: int, show_progress: bool = True):
        """
        Initialize progress tracker.
        
        Args:
            total_companies: Total number of companies to process
            show_progress: Whether to show progress bar (False for testing)
        """
        self.total_companies = total_companies
        self.show_progress = show_progress
        self.start_time = time.time()
        
        # Thread-safe counters
        self._lock = threading.Lock()
        self._stats = Counter()
        self._current_company = 0
        
        # Rich components
        self.console = Console()
        self._progress: Optional[Progress] = None
        self._task_id: Optional[TaskID] = None
        self._live: Optional[Live] = None
        
        self._stop_event = threading.Event()
        self._auto_thread: Optional[threading.Thread] = None
        if self.show_progress:
            self._setup_progress_display()
    
    def _setup_progress_display(self) -> None:
        """Setup Rich progress display components."""
        self._progress = Progress(
            TextColumn("[bold blue]Processing Companies", justify="right"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            TextColumn("{task.completed}/{task.total}"),
            "•",
            TimeElapsedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console
        )
        
        self._task_id = self._progress.add_task(
            "companies",
            total=self.total_companies,
            completed=0
        )
    
    def start(self) -> None:
        """Start the progress display."""
        if not self.show_progress or not self._progress:
            return
            
        try:
            self._live = Live(
                self._generate_display(),
                console=self.console,
                refresh_per_second=2,  # Update twice per second
                transient=False
            )
            self._live.start()
            log.debug("Progress display started")
            # Start lightweight auto-refresh / heartbeat thread so UI updates
            # even if no explicit updates have been triggered yet.
            self._auto_thread = threading.Thread(
                target=self._auto_refresh_loop,
                name="progress-auto-refresh",
                daemon=True,
            )
            self._auto_thread.start()
        except Exception as e:
            log.warning("Failed to start progress display: %s", e)
            self.show_progress = False
    
    def stop(self) -> None:
        """Stop the progress display."""
        if self._live:
            try:
                self._stop_event.set()
                if self._auto_thread and self._auto_thread.is_alive():
                    self._auto_thread.join(timeout=1.5)
                self._live.stop()
                log.debug("Progress display stopped")
            except Exception as e:
                log.warning("Error stopping progress display: %s", e)
    
    def update_company_started(self, company: str) -> None:
        """Mark that processing started for a company."""
        with self._lock:
            self._current_company += 1
            if (
                self.show_progress
                and self._progress
                and self._task_id is not None
            ):
                self._progress.update(
                    self._task_id, completed=self._current_company
                )
                if self._live:
                    self._live.update(self._generate_display())
    
    def update_stats(self, **kwargs) -> None:
        """Update statistics counters."""
        with self._lock:
            for key, value in kwargs.items():
                self._stats[key] += value
            
            if self.show_progress and self._live:
                self._live.update(self._generate_display())
    
    def get_stats_copy(self) -> Dict[str, Any]:
        """Get a thread-safe copy of current statistics."""
        with self._lock:
            elapsed = time.time() - self.start_time
            stats = dict(self._stats)
            stats.update({
                'current_company': self._current_company,
                'total_companies': self.total_companies,
                'elapsed_time': elapsed,
                'companies_per_second': (
                    self._current_company / elapsed if elapsed > 0 else 0.0
                )
            })
            return stats
    
    def _generate_display(self) -> Panel:
        """Generate the live display panel."""
        if not self._progress:
            return Panel("Progress tracking disabled")
        
        # Get current stats
        stats = self.get_stats_copy()
        
        # Create stats columns
        left_stats = Text()
        left_stats.append("📊 LIVE STATISTICS\n\n", style="bold cyan")
        left_stats.append("Companies Processed: ", style="white")
        left_stats.append(
            f"{stats['current_company']}/{stats['total_companies']}\n",
            style="bold green",
        )
        left_stats.append("✅ With Emails: ", style="white")
        left_stats.append(
            f"{stats.get('with_email', 0)}\n", style="bold green"
        )
        left_stats.append("❌ Without Emails: ", style="white")
        left_stats.append(
            f"{stats.get('without_email', 0)}\n", style="yellow"
        )
        
        right_stats = Text()
        right_stats.append("🚨 ERROR TRACKING\n\n", style="bold red")
        right_stats.append("Process Errors: ", style="white")
        right_stats.append(
            f"{stats.get('processing_error', 0)}\n", style="bold red"
        )
        right_stats.append("HTTP Errors: ", style="white")
        right_stats.append(f"{stats.get('http_error', 0)}\n", style="red")
        right_stats.append("Google Errors: ", style="white")
        right_stats.append(f"{stats.get('google_error', 0)}\n", style="red")
        
        perf_stats = Text()
        perf_stats.append("⚡ PERFORMANCE\n\n", style="bold yellow")
        perf_stats.append("Companies/sec: ", style="white")
        perf_stats.append(
            f"{stats['companies_per_second']:.2f}\n", style="bold yellow"
        )
        perf_stats.append("Domains Found: ", style="white")
        perf_stats.append(f"{stats.get('domain', 0)}\n", style="cyan")
        perf_stats.append("Sitemaps Used: ", style="white")
        perf_stats.append(f"{stats.get('sitemap', 0)}\n", style="cyan")
        
        # Combine stats in columns
        stats_columns = Columns(
            [left_stats, right_stats, perf_stats],
            equal=True,
            expand=True,
        )
        
        # Create the main display
        display_content = Columns(
            [
                Panel(
                    self._progress,
                    title="[bold blue]Progress",
                    border_style="blue",
                ),
                Panel(
                    stats_columns,
                    title="[bold cyan]Live Statistics",
                    border_style="cyan",
                ),
            ],
            equal=False,
            expand=True,
        )
        
        return Panel(
            display_content,
            title="[bold green]📧 Email Scraper Progress",
            border_style="green"
        )
    
    def print_final_summary(self) -> None:
        """Print final summary when processing is complete."""
        if not self.show_progress:
            return
            
        stats = self.get_stats_copy()
        
        self.console.print("\n")
        self.console.print(
            "🎉 [bold green]PROCESSING COMPLETE![/bold green] 🎉",
            justify="center",
        )
        self.console.print("\n")
        
        # Final summary table
        summary_text = Text()
        summary_text.append("📊 FINAL RESULTS\n\n", style="bold cyan")
        summary_text.append(
            f"Total Companies: {stats['total_companies']}\n",
            style="white",
        )
        summary_text.append(
            f"✅ With Emails: {stats.get('with_email', 0)}\n",
            style="bold green",
        )
        summary_text.append(
            f"❌ Without Emails: {stats.get('without_email', 0)}\n",
            style="yellow",
        )
        summary_text.append(
            f"🚨 Process Errors: {stats.get('processing_error', 0)}\n",
            style="red",
        )
        summary_text.append(
            f"⏱️  Total Time: {stats['elapsed_time']:.1f}s\n",
            style="cyan",
        )
        summary_text.append(
            f"⚡ Avg Speed: {stats['companies_per_second']:.2f} companies/sec",
            style="yellow",
        )
        
        self.console.print(Panel(
            summary_text,
            title="[bold green]Final Summary",
            border_style="green"
        ))

    # ---------------- internal helpers -----------------
    def _auto_refresh_loop(self) -> None:
        """Periodic refresh & idle warning to prevent apparent stall.

        Runs in a daemon thread; avoids blocking event loop. Every second
        it refreshes the display. If after 15s no companies have started,
        emits a warning suggesting to check Google API credentials or
        network connectivity.
        """
        idle_warned = False
        while not self._stop_event.is_set():
            try:
                if self._live:
                    self._live.update(self._generate_display())
                if (
                    not idle_warned
                    and (time.time() - self.start_time) > 15
                    and self._current_company == 0
                ):
                    log.warning(
                        "No company progress after 15s – check API key/network"
                    )
                    idle_warned = True
            except Exception:
                pass
            self._stop_event.wait(1.0)


# Global progress tracker instance
_global_progress_tracker: Optional[ProgressTracker] = None
_tracker_lock = threading.Lock()


def initialize_progress_tracker(
    total_companies: int, show_progress: bool = True
) -> None:
    """Initialize the global progress tracker."""
    global _global_progress_tracker
    with _tracker_lock:
        if _global_progress_tracker:
            _global_progress_tracker.stop()
        _global_progress_tracker = ProgressTracker(
            total_companies, show_progress
        )
        _global_progress_tracker.start()
 

def get_progress_tracker() -> Optional[ProgressTracker]:
    """Get the global progress tracker instance."""
    return _global_progress_tracker


def update_progress_company_started(company: str) -> None:
    """Update progress for company processing start."""
    if _global_progress_tracker:
        _global_progress_tracker.update_company_started(company)


def update_progress_stats(**kwargs) -> None:
    """Update progress statistics."""
    if _global_progress_tracker:
        _global_progress_tracker.update_stats(**kwargs)


def finalize_progress_tracker() -> None:
    """Finalize and cleanup the progress tracker."""
    global _global_progress_tracker
    with _tracker_lock:
        if _global_progress_tracker:
            _global_progress_tracker.print_final_summary()
            _global_progress_tracker.stop()
            _global_progress_tracker = None
