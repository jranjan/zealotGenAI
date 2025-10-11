#!/usr/bin/env python3
"""
Transformer CLI - Command line interface for data transformation
"""

import sys
import argparse
from pathlib import Path

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from cli.transformer import TransformerCLI


def main():
    """Main CLI function - handles input and delegates to TransformerCLI"""
    parser = argparse.ArgumentParser(description="Transform JSON data by flattening nested structures")
    parser.add_argument("source_folder", help="Path to source directory containing JSON files")
    parser.add_argument("target_folder", help="Path to target directory for transformed files")
    parser.add_argument("--transformer", choices=["flattener", "sonic_flattener", "supersonic_flattener"], 
                       default="supersonic_flattener", help="Type of transformer to use")
    parser.add_argument("--max-workers", type=int, help="Maximum number of worker processes (for sonic_flattener and supersonic_flattener)")
    parser.add_argument("--chunk-size", type=int, default=100, help="Number of files to process per chunk (for supersonic_flattener)")
    parser.add_argument("--analyze-keys", action="store_true", help="Analyze and display flattened keys")
    
    args = parser.parse_args()
    
    # Create CLI instance and run transformation
    cli = TransformerCLI()
    
    if not cli.validate_input(args.source_folder, args.target_folder):
        sys.exit(1)
    
    # Prepare kwargs for transformer
    kwargs = {}
    if args.max_workers:
        kwargs['max_workers'] = args.max_workers
    if args.chunk_size:
        kwargs['chunk_size'] = args.chunk_size
    if args.analyze_keys:
        kwargs['analyze_keys'] = True
    
    cli.run_transformation(
        args.source_folder, 
        args.target_folder, 
        args.transformer, 
        **kwargs
    )


if __name__ == "__main__":
    main()