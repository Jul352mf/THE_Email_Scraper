"""
Output formatting utilities for clean CLI display.

This module provides formatting utilities for the CLI to display
information in a clean, user-friendly format.
"""

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

class OutputFormatter:
    """Clean output formatter for CLI display."""
    
    def __init__(self, verbose: bool = False):
        """Initialize the output formatter."""
        self.verbose = verbose
        self.enabled = True
    
    def show_info(self, message: str) -> None:
        """Show an informational message."""
        if self.enabled:
            print(f"INFO: {message}")
    
    def show_success(self, message: str) -> None:
        """Show a success message."""
        if self.enabled:
            print(f"SUCCESS: {message}")
    
    def show_warning(self, message: str) -> None:
        """Show a warning message."""
        if self.enabled:
            print(f"WARNING: {message}")
    
    def show_error(self, message: str) -> None:
        """Show an error message."""
        if self.enabled:
            print(f"ERROR: {message}")
    
    def show_progress(self, message: str) -> None:
        """Show a progress message."""
        if self.enabled:
            print(f"PROGRESS: {message}")
    
    def show_stats(self, stats_dict: dict) -> None:
        """Show statistics in a formatted way."""
        if not self.enabled:
            return
            
        print("\nStatistics:")
        for key, value in stats_dict.items():
            print(f"   {key}: {value}")

# Global formatter instance
_formatter: Optional[OutputFormatter] = None

def initialize_formatter(verbose: bool = False) -> None:
    """Initialize the global output formatter."""
    global _formatter
    _formatter = OutputFormatter(verbose=verbose)
    log.debug("Output formatter initialized (verbose=%s)", verbose)

def get_formatter() -> OutputFormatter:
    """Get the global output formatter instance."""
    global _formatter
    if _formatter is None:
        _formatter = OutputFormatter()
        log.debug("Auto-initialized output formatter")
    return _formatter

def show_final_summary(output_path: str) -> None:
    """Show final summary of batch processing results."""
    formatter = get_formatter()
    
    try:
        output_dir = Path(output_path)
        if output_dir.exists() and output_dir.is_dir():
            # Count result files
            result_files = list(output_dir.glob("*.xlsx"))
            total_files = len(result_files)
            
            if total_files > 0:
                formatter.show_success(f"Batch processing completed successfully!")
                formatter.show_info(f"Generated {total_files} result files in: {output_dir}")
                
                # Show file details if verbose
                if formatter.verbose and total_files <= 10:
                    print("\nResult files:")
                    for file in result_files:
                        file_size = file.stat().st_size if file.exists() else 0
                        print(f"   {file.name} ({file_size:,} bytes)")
            else:
                formatter.show_warning("Batch processing completed but no result files were generated")
        else:
            formatter.show_warning(f"Output directory not found: {output_path}")
            
    except Exception as e:
        formatter.show_error(f"Error showing final summary: {e}")
        log.error("Error in show_final_summary: %s", e)

def show_performance_summary(stats: dict) -> None:
    """Show performance statistics summary."""
    formatter = get_formatter()
    
    if not stats:
        return
    
    print("\n" + "=" * 50)
    print("PERFORMANCE SUMMARY")
    print("=" * 50)
    
    # Basic stats
    if 'processing_time' in stats:
        print(f"Total processing time: {stats['processing_time']:.2f}s")
    
    if 'companies_processed' in stats:
        print(f"Companies processed: {stats['companies_processed']}")
    
    if 'emails_found' in stats:
        print(f"Emails found: {stats['emails_found']}")
    
    # Performance metrics
    if 'avg_time_per_company' in stats:
        print(f"Average time per company: {stats['avg_time_per_company']:.2f}s")
    
    if 'success_rate' in stats:
        print(f"Success rate: {stats['success_rate']:.1f}%")
    
    # Memory and resource usage
    if 'memory_usage' in stats:
        print(f"Peak memory usage: {stats['memory_usage']}")
    
    print("=" * 50)

def disable_formatter() -> None:
    """Disable formatter output (for testing)."""
    formatter = get_formatter()
    formatter.enabled = False

def enable_formatter() -> None:
    """Enable formatter output."""
    formatter = get_formatter()
    formatter.enabled = True