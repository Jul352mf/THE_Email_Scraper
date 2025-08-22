"""
Enhanced CLI module with improved error handling and user interface.

This module provides a robust command-line interface with proper error handling,
input validation, and logging features.
"""
import argparse
import logging
import os
import signal
import sys, traceback
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from scraper.http import http_client
import asyncio
from scraper.async_scraper import main as async_main

import pandas as pd

from scraper.config import config, ConfigurationError
from scraper.orchestrator import orchestrator
from scraper.browser_service import get_browser_service
from scraper.batch_processor import batch_processor


# Initialize logger
log = logging.getLogger(__name__)


class CLIError(Exception):
    """Exception raised for CLI errors."""
    pass

class CLI:
    """Enhanced command-line interface with improved error handling and validation."""
    
    def __init__(self):
        """Initialize the CLI."""
        self.parser = self._create_parser()
    
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
        
        parser.add_argument(
            "--input-dir",
            default="input",
            help="Input directory for batch processing"
        )
        
        parser.add_argument(
            "--output-dir", 
            default="output",
            help="Output directory for results"
        )
        
        parser.add_argument(
            "--log-dir",
            default="logs",
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
    
    def validate_input_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
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
            return False, f"Input file must be Excel or CSV format (.xlsx, .xls, or .csv): {file_path}"
            
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
                    return False, f"Could not read CSV file with any supported encoding: {file_path}"
            else:
                df = pd.read_excel(file_path)
            
            # Check for required columns
            if "Company" not in df.columns:
                return False, f"Input file must have 'Company' column: {file_path}"
                
            # Check if there's data
            if len(df) == 0:
                return False, f"Input file has no data: {file_path}"
                
            return True, None
            
        except Exception as e:
            return False, f"Error reading input file: {e}"
    
    def validate_output_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Validate that the output file can be written.
        
        Args:
            file_path: Path to output file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check file extension
        if not file_path.lower().endswith(('.xlsx', '.xls')):
            return False, f"Output file must be Excel format (.xlsx or .xls): {file_path}"
            
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
                    return False, f"Cannot write to output directory: {test_dir}"
                    
            return True, None
            
        except Exception as e:
            return False, f"Error checking output file: {e}"
    
    def setup_logging(self, verbose: bool) -> str:
        """
        Set up logging configuration.
        
        Args:
            verbose: Whether to enable verbose logging
            
        Returns:
            Path to log file
        """
        # Create log file name
        logfile = f"scraper_{time.strftime('%Y%m%d_%H%M%S')}.log"
        
        # Set log level
        level = logging.DEBUG if verbose else logging.INFO
        
        # Configure logging
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s",
            handlers=[
                logging.FileHandler(logfile, encoding="utf-8"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Set lower level for external libraries
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("googleapiclient").setLevel(logging.WARNING)
        
        return logfile
    
    def process_batch(self, args: argparse.Namespace) -> bool:
        """
        Process all files in batch mode.
        
        Args:
            args: Command-line arguments
            
        Returns:
            True if successful, False otherwise
        """
        # Set up logging
        logfile = self.setup_logging(args.verbose)
        
        log.info("Email scraper starting in batch mode")
        log.info("Input directory: %s", args.input_dir)
        log.info("Output directory: %s", args.output_dir) 
        log.info("Log directory: %s", args.log_dir)
        log.info("Workers: %d", args.workers)
        
        # Update configuration
        config.domain_score_threshold = args.domain_threshold
        config.max_fallback_pages = args.max_pages
        config.process_pdfs = args.process_pdfs
        config.max_workers = args.workers
        
        # Set orchestrator options
        orchestrator.set_options(save_domain_only=args.save_domain_only)
        
        # Validate environment
        if not self.validate_environment():
            log.error("Environment validation failed")
            return False
        
        # Set up batch processor
        global batch_processor
        from scraper.batch_processor import BatchProcessor
        batch_processor = BatchProcessor(args.input_dir, args.output_dir, args.log_dir)
        
        browser_service = get_browser_service()
        
        try:
            # Process all files
            results = batch_processor.process_all_files(verbose=args.verbose)
            
            if not results:
                log.warning("No files were processed")
                return False
            
            # Print summary
            successful = sum(1 for r in results if r.get("success", False))
            total_companies = sum(r.get("companies_processed", 0) for r in results)
            total_results = sum(r.get("results_found", 0) for r in results)
            
            log.info(f"\nBatch processing summary:")
            log.info(f"Files processed: {successful}/{len(results)}")
            log.info(f"Total companies: {total_companies}")
            log.info(f"Total results: {total_results}")
            
            return successful > 0
            
        except KeyboardInterrupt:
            log.warning("Batch processing interrupted by user")
            return False
        except Exception as e:
            log.error("Batch processing failed: %s", e)
            return False
        finally:
            browser_service.shutdown()
            browser_service.join()
            log.info("BrowserService: shutdown complete")
    
    def scrape_companies(self, args: argparse.Namespace) -> bool:
        """
        Main function to scrape companies from an Excel file.
        
        Args:
            args: Command-line arguments
            
        Returns:
            True if successful, False otherwise
        """
        # Set up logging
        logfile = self.setup_logging(args.verbose)
        
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
        config.max_workers = args.workers
        
        # Set orchestrator options
        orchestrator.set_options(save_domain_only=args.save_domain_only)
        
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
        valid_output, output_error = self.validate_output_file(args.output_file)
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
                        log.debug(f"Successfully read CSV with encoding {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                if df is None:
                    log.error("Could not read CSV file with any supported encoding")
                    return False
            else:
                df = pd.read_excel(args.input_file)
                
            companies = [c for c in df["Company"].astype(str) if c.strip()]
            log.info("Loaded %d companies from %s", len(companies), args.input_file)
        except Exception as e:
            log.error("Failed to load input file: %s", e)
            return False
            
        # Initialize tracking
        start_time = time.time()
        orchestrator.reset_stats()
        all_rows: List[Dict[str, str]] = []

        # Process companies with concurrent domain processing
        try:
            stats, rows = orchestrator.process_companies_concurrent(companies)
            orchestrator.global_stats.update(stats)
            all_rows.extend(rows)
        except KeyboardInterrupt:
            log.warning("Interrupted by user; shutting down")
            return False
        except Exception as e:
            log.error("Error in concurrent processing: %s", e)
            return False
        
        finally:
            browser_service.shutdown()
            browser_service.join()
            log.info("BrowserService: shutdown complete")

        # Create output DataFrame
        df_out = pd.DataFrame(all_rows, columns=["Company", "Domain", "Email"]).drop_duplicates()
        
        # Save output
        try:
            df_out.to_excel(args.output_file, index=False)
        except Exception as e:
            log.error("Failed to save output file: %s", e)
            return False

        # Print summary
        elapsed = time.time() - start_time
        stats = orchestrator.global_stats
        
        http_stats = http_client.stats
        
        # collect all HTTP statuses ≥400
        error_stats = {
            k: v
            for k, v in http_stats.items()
            if k.startswith("status_")
               and k.split("_", 1)[1].isdigit()
               and 400 <= int(k.split("_", 1)[1]) < 600
        }
        
        # grab any “no-response” count (defaulting to zero)
        no_response_count = http_stats.get("status_no-response", 0)    
        
        total_http_errors = sum(error_stats.values())

        # now print the box
        log.info(
            "\n+--------------------------------------------------+\n"
            "| RUN SUMMARY                                      |\n"
            "+--------------------------------------------------+\n"
            f"| Leads           : {stats['leads']:>3}\n"
            f"| Domain found    : {stats['domain']:>3}\n"
            f"| No Google hits  : {stats['no_google']:>3}\n"
            f"| Domain unclear  : {stats['domain_unclear']:>3}\n"
            f"| Sitemap used    : {stats['sitemap']:>3}\n"
            f"| With e-mail     : {stats['with_email']:>3}\n"
            f"| Without e-mail  : {stats['without_email']:>3}\n"
            f"| Google errors   : {stats['google_error']:>3}\n"
            f"| Processing errors: {stats['processing_error']:>3}\n"
            f"| Unique e-mails  : {df_out['Email'].nunique():>3}\n"
            f"| Runtime         : {elapsed:6.1f} s\n"
            f"| HTTP Requests   : {http_stats['total_requests']:>3}\n"
            f"| HTTP errors     : {total_http_errors:>3}\n"
            f"| No-response     : {no_response_count:>3}\n"
            "+--------------------------------------------------+"
        )
        log.info("Saved %d rows -> %s", len(df_out), args.output_file)
        log.info("Verbose log -> %s", Path(logfile).resolve())
        
        return True
    
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
                success = self.process_batch(parsed_args)
            else:
                # Validate required arguments for single file mode
                if not hasattr(parsed_args, 'input_file') or not parsed_args.input_file:
                    log.error("Input file is required for single file mode")
                    return 1
                if not hasattr(parsed_args, 'output_file') or not parsed_args.output_file:
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
