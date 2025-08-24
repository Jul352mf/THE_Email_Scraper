"""
Enhanced CLI module with improved error handling and user interface.

This module provides a robust command-line interface with proper
error handling, input validation, and clean output formatting.
"""  # flake8: noqa
import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from scraper.config import config, ConfigurationError
from scraper.browser_service import get_browser_service
# batch_processor imported lazily where needed to avoid cost when unused
from scraper.progress_tracker import (
    initialize_progress_tracker,
    finalize_progress_tracker,
    update_progress_company_started,
    update_progress_stats,
)

# Import process manager for zombie prevention
try:
    from scraper.process_manager import (
        setup_signal_handlers,
        check_for_zombies,
        cleanup_all_processes,
    )
    PROCESS_MANAGER_AVAILABLE = True
except ImportError:
    PROCESS_MANAGER_AVAILABLE = False


# Initialize logger
log = logging.getLogger(__name__)


class CLIError(Exception):
    """Exception raised for CLI errors."""
    pass


class CLI:
    """Enhanced command-line interface with improved error handling."""
    
    def __init__(self):
        """Initialize the CLI."""
        self.parser = self._create_parser()
        
        # Set up process management for zombie prevention
        if PROCESS_MANAGER_AVAILABLE:
            setup_signal_handlers()
            log.debug("Process management initialized")
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """
        Create command-line argument parser.
        
        Returns:
            Configured argument parser
        """
        parser = argparse.ArgumentParser(
            description="Email scraper for finding company email addresses",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        
        parser.add_argument(
            "input_file",
            nargs='?',  # Make optional for batch mode
            help="Input Excel/CSV file with 'Company' column"
        )
        
        parser.add_argument(
            "output_file",
            nargs='?',  # Make optional for batch mode
            help="Output Excel file for results"
        )
        
        parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            help="Enable verbose logging"
        )
        
        parser.add_argument(
            "--workers",
            type=int,
            default=config.max_workers,
            help="Number of worker threads"
        )
        
        parser.add_argument(
            "--save-domain-only",
            action="store_true",
            help="Save domain even if no emails found"
        )
        
        parser.add_argument(
            "--process-pdfs",
            action="store_true",
            help="Process PDF files"
        )
        
        parser.add_argument(
            "--domain-threshold",
            type=int,
            default=config.domain_score_threshold,
            help="Domain score threshold (0-100)"
        )
        
        parser.add_argument(
            "--max-pages",
            type=int,
            default=config.max_fallback_pages,
            help="Maximum pages to crawl per domain"
        )
        
        parser.add_argument(
            "--config",
            help="Path to custom .env configuration file"
        )
        
        parser.add_argument(
            "--batch",
            action="store_true",
            help="Process all files in input directory"
        )

    # Async pipeline is now the only supported mode (legacy sync removed)
        
        parser.add_argument(
            "--input-dir",
            default=os.getenv("INPUT_DIR", "input"),
            help="Input directory for batch processing"
        )
        
        parser.add_argument(
            "--output-dir",
            default=os.getenv("OUTPUT_DIR", "output"),
            help="Output directory for results"
        )
        
        parser.add_argument(
            "--log-dir",
            default=os.getenv("LOG_DIR", "logs"),
            help="Log directory for processing logs"
        )
        
        return parser
    
    def validate_environment(self) -> bool:
        """
        Validate that all required environment variables are set.
        
        Returns:
            True if valid, False otherwise
        """
        try:
            config.validate_or_raise()
            return True
        except ConfigurationError as e:
            log.error("Configuration error: %s", e)
            return False
    
    def validate_input_file(
        self, file_path: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that the input file exists and has the required format.
        
        Args:
            file_path: Path to input file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check if file exists
        if not os.path.isfile(file_path):
            return False, f"Input file not found: {file_path}"
            
        # Check file extension
        if not file_path.lower().endswith(('.xlsx', '.xls', '.csv')):
            return (
                False,
                (
                    "Input file must be Excel or CSV format "
                    "(.xlsx, .xls, or .csv): "
                    f"{file_path}"
                ),
            )
            
        # Try to load the file
        try:
            if file_path.lower().endswith('.csv'):
                # Try different encodings for CSV
                encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    return (
                        False,
                        "Could not read CSV file with any supported encoding: "
                        f"{file_path}",
                    )
            else:
                df = pd.read_excel(file_path)
            
            # Check for required columns
            if "Company" not in df.columns:
                return (
                    False,
                    f"Input file must have 'Company' column: {file_path}",
                )
                
            # Check if there's data
            if len(df) == 0:
                return False, f"Input file has no data: {file_path}"
                
            return True, None
            
        except Exception as e:
            return False, f"Error reading input file: {e}"
    
    def validate_output_file(
        self, file_path: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate that the output file can be written.
        
        Args:
            file_path: Path to output file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check file extension
        if not file_path.lower().endswith(('.xlsx', '.xls')):
            return (
                False,
                (
                    f"Output file must be Excel format (.xlsx or .xls): "
                    f"{file_path}"
                ),
            )
            
        # Check if directory exists
        output_dir = os.path.dirname(file_path)
        if output_dir and not os.path.isdir(output_dir):
            return False, f"Output directory does not exist: {output_dir}"
            
        # Check if file is writable
        try:
            if os.path.exists(file_path):
                # Check if we can write to existing file
                if not os.access(file_path, os.W_OK):
                    return False, f"Output file is not writable: {file_path}"
            else:
                # Check if we can write to directory
                test_dir = output_dir if output_dir else "."
                if not os.access(test_dir, os.W_OK):
                    return (
                        False,
                        f"Cannot write to output directory: {test_dir}",
                    )
                    
            return True, None
            
        except Exception as e:
            return False, f"Error checking output file: {e}"
    
    def setup_logging(self, verbose: bool, log_dir: str = ".") -> str:
        """
        Set up logging configuration.
        
        Args:
            verbose: Whether to enable verbose logging
            log_dir: Directory where log files should be saved
            
        Returns:
            Path to log file
        """
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Create log file name in the specified directory
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        logfile = os.path.join(log_dir, f"scraper_{timestamp}.log")
        
        # Set log level for file logging
        file_level = logging.DEBUG if verbose else logging.INFO
        
        # Configure file logging only - console output handled by formatter
        console_handler = (logging.StreamHandler(sys.stdout) if verbose
                           else logging.NullHandler())
        logging.basicConfig(
            level=file_level,
            format="%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s",
            handlers=[
                logging.FileHandler(logfile, encoding="utf-8"),
                console_handler
            ]
        )
        
        # Set lower level for external libraries
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("googleapiclient").setLevel(logging.WARNING)
        
        # Initialize the clean output formatter
        from scraper.output_formatter import initialize_formatter
        initialize_formatter(verbose=verbose)
        
        return logfile
    
    def process_batch(self, args: argparse.Namespace) -> bool:
        """
        Process all files in batch mode.
        
        Args:
            args: Command-line arguments
            
        Returns:
            True if successful, False otherwise
        """
        # Set up logging with log directory
        self.setup_logging(args.verbose, args.log_dir)
        
        # Get clean output formatter
        from scraper.output_formatter import get_formatter
        formatter = get_formatter()
        
        # Show startup info with clean formatting
        config_info = {
            'max_workers': args.workers,
            'js_enabled': False,  # Batch mode typically doesn't use JS
            'output_file': f"{args.output_dir}/*"
        }
        
        # Count total files to process
        import glob
        input_files = glob.glob(os.path.join(args.input_dir, "*.csv"))
        input_files.extend(glob.glob(os.path.join(args.input_dir, "*.xlsx")))
        total_files = len(input_files)
        
        formatter.show_startup_info(total_files, config_info)
        
        # Log detailed info to file only
        log.info("Email scraper starting in batch mode")
        log.info("Input directory: %s", args.input_dir)
        log.info("Output directory: %s", args.output_dir)
        log.info("Log directory: %s", args.log_dir)
        log.info("Workers: %d", args.workers)
        
        # Update configuration with centralized worker management
        config.domain_score_threshold = args.domain_threshold
        config.max_fallback_pages = args.max_pages
        config.process_pdfs = args.process_pdfs
        config.update_max_workers(args.workers)
        
    # Legacy orchestrator removed – option retained for forward compatibility
        
        # Validate environment
        if not self.validate_environment():
            formatter.show_error("Environment validation failed")
            return False
        
        # Set up batch processor
        global batch_processor
        from scraper.batch_processor import BatchProcessor
        batch_processor = BatchProcessor(
            args.input_dir, args.output_dir, args.log_dir)
        
        browser_service = get_browser_service()
        
        try:
            # Process all files with progress updates
            results = batch_processor.process_all_files(verbose=args.verbose)
            
            if not results:
                formatter.show_error("No files were processed")
                return False
            
            # Collect results for summary
            successful = sum(1 for r in results if r.get("success", False))
            
            # Show final summary instead of log.info messages
            from scraper.output_formatter import show_final_summary
            show_final_summary(f"{args.output_dir}/batch_results")
            
            return successful > 0
            
        except KeyboardInterrupt:
            formatter.show_error("Processing interrupted by user")
            # Clean up any zombie processes on interruption
            if PROCESS_MANAGER_AVAILABLE:
                cleanup_all_processes()
            return False
        except Exception as e:
            formatter.show_error(f"Batch processing failed: {e}")
            return False
        finally:
            browser_service.shutdown()
            # BrowserService is thread-backed; no join() API.
            # Shutdown handled separately.
            log.info("BrowserService: shutdown complete")
            
            # Check for any remaining zombie processes
            if PROCESS_MANAGER_AVAILABLE:
                zombies = check_for_zombies()
                if zombies:
                    log.warning(
                        "Found and cleaned up %d zombie processes",
                        len(zombies)
                    )
    
    def scrape_companies(self, args: argparse.Namespace) -> bool:
        """
        Main function to scrape companies from an Excel file.
        
        Args:
            args: Command-line arguments
            
        Returns:
            True if successful, False otherwise
        """
        # Set up logging with log directory
        logfile = self.setup_logging(args.verbose, args.log_dir)
        
        browser_service = get_browser_service()
        
    # Log startup information
        log.info("Email scraper starting")
        log.info("Input file: %s", args.input_file)
        log.info("Output file: %s", args.output_file)
        log.info("Workers: %d", args.workers)
        log.info("Domain threshold: %d", args.domain_threshold)
        log.info("Max pages: %d", args.max_pages)
        log.info("Process PDFs: %s", args.process_pdfs)
        log.info("Save domain only: %s", args.save_domain_only)
        
        # Update configuration from command-line arguments
        config.domain_score_threshold = args.domain_threshold
        config.max_fallback_pages = args.max_pages
        config.process_pdfs = args.process_pdfs
        config.update_max_workers(args.workers)
        
    # Legacy orchestrator removed; save_domain_only currently unused
    # (reserved for async pipeline future use)
        
        # Validate environment
        if not self.validate_environment():
            log.error("Environment validation failed")
            return False
            
        # Validate input file
        valid_input, input_error = self.validate_input_file(args.input_file)
        if not valid_input:
            log.error("Input validation failed: %s", input_error)
            return False
            
        # Validate output file
        valid_output, output_error = self.validate_output_file(
            args.output_file
        )
        if not valid_output:
            log.error("Output validation failed: %s", output_error)
            return False
            
        # Load input file
        try:
            if args.input_file.lower().endswith('.csv'):
                # Try different encodings for CSV
                encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
                df = None
                for encoding in encodings:
                    try:
                        df = pd.read_csv(args.input_file, encoding=encoding)
                        log.debug(
                            "Successfully read CSV with encoding %s",
                            encoding,
                        )
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    log.error(
                        "Could not read CSV file with any supported encoding"
                    )
                    return False
            else:
                df = pd.read_excel(args.input_file)
                
            companies = [c for c in df["Company"].astype(str) if c.strip()]
            log.info(
                "Loaded %d companies from %s", len(companies), args.input_file
            )
        except Exception as e:
            log.error("Failed to load input file: %s", e)
            return False
            
        # Initialize tracking
        start_time = time.time()
        # Always use async pipeline now
        return self._scrape_companies_async(
            args, companies, browser_service, start_time, logfile
        )

    # ----------------------------- async path -----------------------------
    def _scrape_companies_async(
        self,
        args: argparse.Namespace,
        companies: List[str],
        browser_service,
        start_time: float,
        logfile: str,
    ) -> bool:
        """Async scraping pipeline using AsyncOrchestrator.

        Reuses validation & setup from sync path; runs event loop to process
        companies concurrently while updating the progress tracker live.
        """
        import asyncio
        from scraper.async_orchestrator import AsyncOrchestrator

        async def run_async() -> bool:
            initialize_progress_tracker(
                len(companies), show_progress=not args.verbose
            )
            tmp_csv = Path(args.output_file).with_suffix('.partial.csv')
            written = 0
            header_written = False
            import csv

            fieldnames = ["Company", "Domain", "Email", "Source"]

            from scraper.config import config as _cfg
            async with AsyncOrchestrator(
                max_concurrent_companies=min(
                    getattr(
                        _cfg,
                        'async_company_concurrency',
                        args.workers,
                    ),
                    200,
                ),
                per_domain_page_concurrency=getattr(
                    _cfg, 'async_per_domain_page_concurrency', 3
                ),
            ) as async_orch:
                async def on_start(name: str):  # company begins
                    update_progress_company_started(name)

                def on_rows(rows):  # domain results ready
                    nonlocal header_written, written
                    if not rows:
                        return
                    mode = 'a' if header_written else 'w'
                    with open(
                        tmp_csv, mode, newline='', encoding='utf-8'
                    ) as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if not header_written:
                            writer.writeheader()
                            header_written = True
                        for r in rows:
                            writer.writerow(
                                {
                                    "Company": r.get("company"),
                                    "Domain": r.get("domain"),
                                    "Email": r.get("email"),
                                    "Source": r.get("source", "unknown"),
                                }
                            )
                        written += len(rows)

                stats_counter, _ = await async_orch.\
                    process_companies_streaming(
                        companies,
                        on_company_start=on_start,
                        on_rows=on_rows,
                    )

                update_progress_stats(
                    with_email=stats_counter.get("success", 0),
                    without_email=stats_counter.get("no_email", 0),
                    processing_error=stats_counter.get("processing_errors", 0)
                    + stats_counter.get("domain_processing_errors", 0),
                    google_error=stats_counter.get("no_google", 0),
                    domain=stats_counter.get("success", 0)
                    + stats_counter.get("no_email", 0),
                )

            finalize_progress_tracker()

            # Consolidate to Excel
            try:
                import pandas as pd
                import os
                import tempfile

                def _write_excel_atomic(df, path: Path):
                    """Write DataFrame to XLSX atomically.

                    Ensures we never leave a half-written workbook.
                    """
                    path.parent.mkdir(parents=True, exist_ok=True)
                    # Always ensure columns exist even if empty
                    base_cols = ["Company", "Domain", "Email", "Source"]
                    for c in base_cols:
                        if c not in df.columns:
                            df[c] = []
                    df = df[base_cols]
                    with tempfile.NamedTemporaryFile(
                        suffix=".xlsx", delete=False
                    ) as tmpf:
                        tmp_name = tmpf.name
                    try:
                        with pd.ExcelWriter(tmp_name, engine="openpyxl") as w:
                            df.to_excel(w, index=False)
                        os.replace(tmp_name, path)
                    finally:  # cleanup on failure
                        if os.path.exists(tmp_name) and not os.path.samefile(
                            tmp_name, path
                        ):
                            try:
                                os.remove(tmp_name)
                            except Exception:
                                pass

                if tmp_csv.exists():
                    df_out = pd.read_csv(tmp_csv)
                    df_out = df_out.drop_duplicates()
                else:
                    df_out = pd.DataFrame(
                        columns=["Company", "Domain", "Email", "Source"]
                    )

                _write_excel_atomic(df_out, Path(args.output_file))
            except Exception as e:
                log.error("Failed final Excel write: %s", e)
                # Best-effort fallback: write CSV instead next to desired path
                try:
                    fallback_csv = Path(args.output_file).with_suffix('.csv')
                    if 'df_out' in locals():
                        df_out.to_csv(fallback_csv, index=False)
                        log.warning(
                            "Wrote fallback CSV instead: %s", fallback_csv
                        )
                except Exception as e2:  # pragma: no cover
                    log.error("Fallback CSV write also failed: %s", e2)
                return False

            elapsed = time.time() - start_time
            log.info(
                "Async streaming run complete in %.1fs (%d rows)",
                elapsed,
                written,
            )
            log.info("Saved output -> %s", args.output_file)
            if tmp_csv.exists():
                try:
                    tmp_csv.unlink()
                except Exception:
                    pass
            return True

        try:
            return asyncio.run(run_async())
        finally:
            browser_service.shutdown()
            # BrowserService is thread-backed; no join() API
            log.info("BrowserService: shutdown complete")
            if PROCESS_MANAGER_AVAILABLE:
                zombies = check_for_zombies()
                if zombies:
                    log.warning(
                        "Found and cleaned up %d zombie processes",
                        len(zombies),
                    )
    
    def run(self, args: Optional[List[str]] = None) -> int:
        """
        Run the CLI with the given arguments.
        
        Args:
            args: Command-line arguments (defaults to sys.argv[1:])
            
        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        try:
            # Parse arguments
            parsed_args = self.parser.parse_args(args)
            
            # Check if batch processing or single file
            if parsed_args.batch:
                # TODO: migrate batch mode to async (legacy removed)
                log.error(
                    "Batch mode is temporarily unsupported in async-only build"
                )
                return 1
            # Single file mode validation
            if (
                not hasattr(parsed_args, 'input_file')
                or not parsed_args.input_file
            ):
                log.error("Input file is required for single file mode")
                return 1
            if (
                not hasattr(parsed_args, 'output_file')
                or not parsed_args.output_file
            ):
                log.error("Output file is required for single file mode")
                return 1
            success = self.scrape_companies(parsed_args)
            
            return 0 if success else 1
            
        except KeyboardInterrupt:
            log.warning("Execution interrupted by user (Ctrl+C)")
            return 1
        except Exception as e:
            log.error("Unhandled exception: %s", e, exc_info=True)
            return 1

 
def main() -> int:
    """
    Main entry point for the email scraper.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    
    cli = CLI()
    
    try:
        return cli.run()
    
    except KeyboardInterrupt:
        log.warning("Execution interrupted by user")
        return 1
    
    except Exception as e:
        log.error("Execution failed: %s", e, exc_info=True)
        return 1
