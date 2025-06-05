"""
File updater module for updating input files with results.

This module provides functionality for updating input Excel files with
found emails and domains, supporting both append and merge modes.
"""

import logging
import os
from typing import List, Dict, Any, Optional, Set, Tuple

import pandas as pd

# Initialize logger
log = logging.getLogger(__name__)

class FileUpdaterError(Exception):
    """Exception raised for file updater errors."""
    pass

class FileUpdater:
    """
    File updater for updating input files with results.
    
    This class provides:
    - Updating input Excel files with found emails
    - Merging new results with existing data
    - Preserving original data while adding new information
    """
    
    def __init__(self):
        """Initialize the file updater."""
        pass
    
    def update_file(self, input_file: str, output_file: str, 
                   results: List[Dict[str, str]], 
                   merge: bool = True) -> Tuple[int, int]:
        """
        Update an Excel file with results.
        
        Args:
            input_file: Path to input Excel file
            output_file: Path to output Excel file
            results: List of result dictionaries (Company, Domain, Email)
            merge: Whether to merge with existing data
            
        Returns:
            Tuple of (updated rows, total rows)
            
        Raises:
            FileUpdaterError: If file update fails
        """
        try:
            # Load input file
            df_input = pd.read_excel(input_file)
            
            # Check if required columns exist
            if "Company" not in df_input.columns:
                raise FileUpdaterError(f"Input file {input_file} must have 'Company' column")
                
            # Create results DataFrame
            df_results = pd.DataFrame(results)
            
            # Perform update based on mode
            if merge:
                df_output = self._merge_data(df_input, df_results)
            else:
                df_output = self._append_data(df_input, df_results)
                
            # Save output file
            df_output.to_excel(output_file, index=False)
            
            # Count updated rows
            updated_rows = len(df_output) - len(df_input)
            
            log.info("Updated file %s -> %s: %d rows updated, %d total rows", 
                    input_file, output_file, updated_rows, len(df_output))
            
            return updated_rows, len(df_output)
            
        except Exception as e:
            log.error("Error updating file %s: %s", input_file, e)
            raise FileUpdaterError(f"Error updating file {input_file}: {e}")
    
    def _merge_data(self, df_input: pd.DataFrame, df_results: pd.DataFrame) -> pd.DataFrame:
        """
        Merge input data with results.
        
        Args:
            df_input: Input DataFrame
            df_results: Results DataFrame
            
        Returns:
            Merged DataFrame
        """
        # Create a copy of input DataFrame
        df_output = df_input.copy()
        
        # Add Domain column if it doesn't exist
        if "Domain" not in df_output.columns:
            df_output["Domain"] = ""
            
        # Add Email column if it doesn't exist
        if "Email" not in df_output.columns:
            df_output["Email"] = ""
            
        # Create a mapping of companies to results
        company_results = {}
        for _, row in df_results.iterrows():
            company = row["Company"]
            if company not in company_results:
                company_results[company] = []
            company_results[company].append(row)
            
        # Update each row in the input DataFrame
        updated_rows = 0
        for i, row in df_output.iterrows():
            company = row["Company"]
            if company in company_results:
                # Get the first result for this company
                result = company_results[company][0]
                
                # Update domain if empty
                if not row["Domain"] and "Domain" in result:
                    df_output.at[i, "Domain"] = result["Domain"]
                    updated_rows += 1
                    
                # Update email if empty
                if not row["Email"] and "Email" in result:
                    df_output.at[i, "Email"] = result["Email"]
                    updated_rows += 1
                    
                # Remove the used result
                company_results[company].pop(0)
                
                # If no more results for this company, remove it from the mapping
                if not company_results[company]:
                    del company_results[company]
        
        # Add remaining results as new rows
        new_rows = []
        for company, results in company_results.items():
            for result in results:
                new_rows.append(result)
                
        if new_rows:
            df_new = pd.DataFrame(new_rows)
            df_output = pd.concat([df_output, df_new], ignore_index=True)
            
        return df_output
    
    def _append_data(self, df_input: pd.DataFrame, df_results: pd.DataFrame) -> pd.DataFrame:
        """
        Append results to input data.
        
        Args:
            df_input: Input DataFrame
            df_results: Results DataFrame
            
        Returns:
            Combined DataFrame
        """
        # Simply concatenate the DataFrames
        return pd.concat([df_input, df_results], ignore_index=True)
    
    def update_in_place(self, file_path: str, results: List[Dict[str, str]], 
                       merge: bool = True) -> Tuple[int, int]:
        """
        Update a file in place.
        
        Args:
            file_path: Path to Excel file
            results: List of result dictionaries
            merge: Whether to merge with existing data
            
        Returns:
            Tuple of (updated rows, total rows)
            
        Raises:
            FileUpdaterError: If file update fails
        """
        try:
            # Create a temporary file
            temp_file = f"{file_path}.temp"
            
            # Update the file
            updated_rows, total_rows = self.update_file(file_path, temp_file, results, merge)
            
            # Replace the original file
            os.replace(temp_file, file_path)
            
            return updated_rows, total_rows
            
        except Exception as e:
            log.error("Error updating file in place %s: %s", file_path, e)
            raise FileUpdaterError(f"Error updating file in place {file_path}: {e}")

# Create a global file updater instance
file_updater = FileUpdater()
