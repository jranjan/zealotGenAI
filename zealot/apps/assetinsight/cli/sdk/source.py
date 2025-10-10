#!/usr/bin/env python3
"""
Source CLI - Command line interface for source data analysis
"""

import sys
from pathlib import Path

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from cli.scanner import ScannerCLI


def main():
    """Main CLI function - handles input and delegates to ScannerCLI"""
    if len(sys.argv) != 2:
        print("Usage: python source.py <source_directory>")
        print("Example: python source.py /path/to/json/files")
        sys.exit(1)
    
    source_folder = sys.argv[1]
    
    # Create CLI instance and run analysis
    cli = ScannerCLI()
    
    if not cli.validate_input(source_folder):
        sys.exit(1)
    
    cli.run_operation(source_folder)


if __name__ == "__main__":
    main()