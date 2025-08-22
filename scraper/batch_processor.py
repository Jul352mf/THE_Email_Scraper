"""
Batch processor for handling multiple input files and automated processing.

This module provides functionality to:
- Process all files in an input directory
- Support both Excel and CSV input formats
- Generate timestamped output files
- Organize logs in a dedicated directory
"""

import os
import logging
import time
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import pandas as pd
from datetime import datetime

from scraper.config import config
from scraper.orchestrator import orchestrator

log = logging.getLogger(__name__)

class BatchProcessor:
    """Batch processor for automated file processing."""
    
    def __init__(self, input_dir: str = "input", output_dir: str = "output", log_dir: str = "logs"):
        """
        Initialize batch processor.
        
        Args:
            input_dir: Directory containing input files
            output_dir: Directory for output files
            log_dir: Directory for log files
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        
        # Create directories if they don't exist
        self.input_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)
        
        self.supported_formats = ['.xlsx', '.xls', '.csv']
        
    def find_input_files(self) -> List[Path]:
        """Find all supported input files in the input directory."""
        input_files = []
        
        for file_path in self.input_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in self.supported_formats:
                input_files.append(file_path)
        
        # Sort by modification time (newest first)
        input_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        log.info(f"Found {len(input_files)} input files: {[f.name for f in input_files]}")
        return input_files
    
    def read_input_file(self, file_path: Path) -> pd.DataFrame:
        """
        Read input file (Excel or CSV) and return DataFrame.
        
        Args:
            file_path: Path to input file
            
        Returns:
            DataFrame with company data
            
        Raises:
            ValueError: If file format is not supported or file is invalid
        """
        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif file_path.suffix.lower() == '.csv':
                # Try different encodings for CSV
                encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        log.debug(f"Successfully read {file_path} with encoding {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    raise ValueError(f"Could not read CSV file {file_path} with any supported encoding")
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            # Validate required columns
            if "Company" not in df.columns:
                # Try common variations
                company_columns = [col for col in df.columns if 'company' in col.lower()]
                if company_columns:
                    df = df.rename(columns={company_columns[0]: "Company"})
                    log.info(f"Renamed column '{company_columns[0]}' to 'Company'")
                else:
                    raise ValueError(f"No 'Company' column found in {file_path}")
            
            # Clean and validate data
            df = df.dropna(subset=['Company'])  # Remove rows with empty company names
            df['Company'] = df['Company'].astype(str).str.strip()  # Clean company names
            df = df[df['Company'] != '']  # Remove empty strings
            
            log.info(f"Loaded {len(df)} companies from {file_path}")
            return df
            
        except Exception as e:
            log.error(f"Error reading {file_path}: {e}")
            raise
    
    def generate_output_filename(self, input_file: Path) -> Path:
        """Generate timestamped output filename."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = input_file.stem
        output_name = f"{base_name}_results_{timestamp}.xlsx"
        return self.output_dir / output_name
    
    def generate_log_filename(self, input_file: Path) -> Path:
        """Generate timestamped log filename."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = input_file.stem
        log_name = f"{base_name}_scraper_{timestamp}.log"
        return self.log_dir / log_name
    
    def setup_file_logging(self, log_file: Path, verbose: bool = False) -> logging.FileHandler:
        """Set up file logging for a specific processing session."""
        handler = logging.FileHandler(log_file, encoding='utf-8')
        handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"
        )
        handler.setFormatter(formatter)
        
        # Add to root logger
        logging.getLogger().addHandler(handler)
        
        return handler
    
    def process_single_file(self, input_file: Path, verbose: bool = False) -> Tuple[bool, Dict]:
        """
        Process a single input file.
        
        Args:
            input_file: Path to input file
            verbose: Enable verbose logging
            
        Returns:
            Tuple of (success, results_summary)
        """
        log.info(f"Processing file: {input_file}")
        
        # Generate output and log filenames
        output_file = self.generate_output_filename(input_file)
        log_file = self.generate_log_filename(input_file)
        
        # Set up file logging
        file_handler = self.setup_file_logging(log_file, verbose)
        
        try:
            # Load input data
            df = self.read_input_file(input_file)
            companies = df["Company"].tolist()
            
            if not companies:
                log.warning(f"No companies found in {input_file}")
                return False, {"error": "No companies found"}
            
            # Reset orchestrator for clean run
            orchestrator.reset_stats()
            
            # Process companies
            start_time = time.time()
            log.info(f"Starting processing of {len(companies)} companies")
            
            stats, rows = orchestrator.process_companies_concurrent(companies)
            
            elapsed = time.time() - start_time
            
            # Create output DataFrame
            if rows:
                df_out = pd.DataFrame(rows, columns=["Company", "Domain", "Email"]).drop_duplicates()
                df_out.to_excel(output_file, index=False)
                log.info(f"Saved {len(df_out)} results to {output_file}")
            else:
                # Create empty output file for consistency
                pd.DataFrame(columns=["Company", "Domain", "Email"]).to_excel(output_file, index=False)
                log.warning(f"No results found, created empty output file: {output_file}")
            
            # Generate summary
            summary = {
                "input_file": str(input_file),
                "output_file": str(output_file),
                "log_file": str(log_file),
                "companies_processed": len(companies),
                "results_found": len(rows) if rows else 0,
                "processing_time": elapsed,
                "stats": dict(stats)
            }
            
            log.info(f"Processing completed in {elapsed:.2f}s")
            log.info(f"Results: {summary['results_found']} emails from {summary['companies_processed']} companies")
            
            return True, summary
            
        except Exception as e:
            log.error(f"Error processing {input_file}: {e}")
            summary = {
                "input_file": str(input_file),
                "error": str(e),
                "processing_time": 0,
                "results_found": 0
            }
            return False, summary
            
        finally:
            # Remove file handler
            logging.getLogger().removeHandler(file_handler)
            file_handler.close()
    
    def process_all_files(self, verbose: bool = False) -> List[Dict]:
        """
        Process all files in the input directory.
        
        Args:
            verbose: Enable verbose logging
            
        Returns:
            List of processing results for each file
        """
        input_files = self.find_input_files()
        
        if not input_files:
            log.warning(f"No input files found in {self.input_dir}")
            return []
        
        results = []
        total_start = time.time()
        
        log.info(f"Starting batch processing of {len(input_files)} files")
        
        for i, input_file in enumerate(input_files, 1):
            log.info(f"Processing file {i}/{len(input_files)}: {input_file.name}")
            
            success, summary = self.process_single_file(input_file, verbose)
            summary["file_index"] = i
            summary["success"] = success
            results.append(summary)
            
            if not success:
                log.error(f"Failed to process {input_file.name}")
            
            # Brief pause between files to avoid overwhelming servers
            if i < len(input_files):
                time.sleep(1)
        
        total_elapsed = time.time() - total_start
        
        # Print final summary
        successful = sum(1 for r in results if r.get("success", False))
        total_companies = sum(r.get("companies_processed", 0) for r in results)
        total_results = sum(r.get("results_found", 0) for r in results)
        
        log.info(f"\nBatch processing completed in {total_elapsed:.2f}s")
        log.info(f"Files processed: {successful}/{len(input_files)}")
        log.info(f"Total companies: {total_companies}")
        log.info(f"Total results: {total_results}")
        
        return results


# Global batch processor instance
batch_processor = BatchProcessor()