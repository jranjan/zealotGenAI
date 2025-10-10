"""
Scanner CLI - Command line interface for source data scanning
"""

import sys
from pathlib import Path
from scanner.source import SourceScanner
from .sdk.base import BaseCLI


class ScannerCLI(BaseCLI):
    """CLI class for source data scanning operations"""
    
    def __init__(self):
        super().__init__()
        self.processor = SourceScanner()
    
    def run_operation(self, source_folder: str) -> None:
        """
        Run complete source directory analysis.
        
        Args:
            source_folder: Path to source directory containing JSON files
        """
        try:
            self.print_header(f"🔍 Analyzing source directory: {source_folder}")
            
            # Get basic directory info first
            dir_info = self.processor.get_directory_info(source_folder)
            if not dir_info['valid']:
                self.display_error(dir_info['error'])
            
            print(f"✅ Found {self.format_number(dir_info['total_files'])} JSON files")
            if dir_info['json_files']:
                print("Sample files:")
                for file_name in dir_info['json_files']:
                    print(f"  - {file_name}")
            
            print("\n🔍 Running detailed analysis...")
            
            # Run full analysis
            results = self.processor.scan_source_directory(source_folder)
            
            self.display_results(results)
            
        except Exception as e:
            self.display_error(str(e))
    
    def display_results(self, results: dict) -> None:
        """
        Display analysis results in a formatted way.
        
        Args:
            results: Analysis results dictionary
        """
        self.print_section("📊 Analysis Results")
        print(f"📁 Total Files: {self.format_number(results['total_files'])}")
        print(f"📈 Estimated Assets: {self.format_number(results['estimated_assets'])}")
        print(f"📂 Source Directory: {results['source_folder']}")
        
        if results['file_details']:
            self.print_section("📋 File Details (Sample)")
            self.print_table_row("File", "Size (MB)", "Assets", widths=[30, 12, 12])
            for file_info in results['file_details']:
                self.print_table_row(
                    file_info['File'][:28] + ".." if len(file_info['File']) > 30 else file_info['File'],
                    f"{file_info['Size (MB)']:.2f}",
                    self.format_number(file_info['Assets']),
                    widths=[30, 12, 12]
                )
        
        print("\n✅ Analysis complete!")
    
    def validate_input(self, source_folder: str) -> bool:
        """
        Validate input parameters.
        
        Args:
            source_folder: Path to source directory
            
        Returns:
            True if valid, False otherwise
        """
        return self.validate_directory_exists(source_folder, "source directory")
