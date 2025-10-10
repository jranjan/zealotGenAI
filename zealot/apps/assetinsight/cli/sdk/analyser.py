#!/usr/bin/env python3
"""
Analyser CLI - Command line interface for asset analysis
"""

import sys
import argparse
from pathlib import Path

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from cli.analyser import AnalyserCLI


def main():
    """Main CLI function - handles input and delegates to AnalyserCLI"""
    parser = argparse.ArgumentParser(description="Analyze asset data using configuration")
    parser.add_argument("config_path", help="Path to configuration YAML file")
    parser.add_argument("source_folder", help="Path to source directory containing JSON files")
    parser.add_argument("output_folder", help="Path to output directory for analysis results")
    parser.add_argument("--analyser", choices=["owner", "security"], 
                       default="owner", help="Type of analyser to use")
    
    args = parser.parse_args()
    
    # Create CLI instance and run analysis
    cli = AnalyserCLI()
    
    if not cli.validate_input(args.config_path, args.source_folder, args.output_folder):
        sys.exit(1)
    
    cli.run_analysis(
        args.config_path,
        args.source_folder, 
        args.output_folder,
        args.analyser
    )


if __name__ == "__main__":
    main()
