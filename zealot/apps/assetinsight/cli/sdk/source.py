#!/usr/bin/env python3
"""
Source CLI - Command line interface for source data analysis
"""

import sys
import argparse
from pathlib import Path

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from cli.scanner import ScannerCLI


def main():
    """Main CLI function - handles input and delegates to ScannerCLI"""
    parser = argparse.ArgumentParser(description="Analyze source directory containing JSON files")
    parser.add_argument("source_directory", help="Path to source directory containing JSON files")
    parser.add_argument("--scanner", choices=["source", "supersonic_source_scanner"], 
                       default="supersonic_source_scanner", help="Type of scanner to use")
    parser.add_argument("--max-workers", type=int, help="Maximum number of worker processes (for supersonic_source_scanner)")
    parser.add_argument("--chunk-size", type=int, default=50, help="Number of files to process per chunk (for supersonic_source_scanner)")
    
    args = parser.parse_args()
    source_folder = args.source_directory
    
    # Prepare kwargs for scanner
    kwargs = {}
    if args.max_workers:
        kwargs['max_workers'] = args.max_workers
    if args.chunk_size:
        kwargs['chunk_size'] = args.chunk_size
    
    # Create CLI instance and run analysis
    cli = ScannerCLI(scanner_type=args.scanner, **kwargs)
    
    if not cli.validate_input(source_folder):
        sys.exit(1)
    
    cli.run_operation(source_folder)


if __name__ == "__main__":
    main()