"""
Analyser CLI - Command line interface for asset analysis
"""

import sys
import json
from pathlib import Path
from analyser import AssetAnalyser
from configreader import AssetFieldConfig
from common import AnalyserFactory


class AnalyserCLI:
    """CLI class for asset analysis operations"""
    
    def __init__(self):
        self.analyser = None
        self.config = None
    
    def run_analysis(self, config_path: str, source_folder: str, output_folder: str, analyser_type: str = "owner") -> None:
        """
        Run asset analysis.
        
        Args:
            config_path: Path to configuration YAML file
            source_folder: Path to source directory containing JSON files
            output_folder: Path to output directory for analysis results
            analyser_type: Type of analyser to use ("owner" or "security")
        """
        try:
            # Load configuration
            self.config = AssetFieldConfig(config_path)
            print(f"📋 Loaded configuration from: {config_path}")
            
            # Create analyser based on type
            if not AnalyserFactory.is_valid_type(analyser_type):
                print(f"❌ Error: Unknown analyser type: {analyser_type}")
                print(f"Available types: {AnalyserFactory.get_available_types()}")
                sys.exit(1)
            
            self.analyser = AnalyserFactory.create_analyser(analyser_type)
            
            print(f"🔍 Running {analyser_type} analysis...")
            print(f"📂 Source: {source_folder}")
            print(f"📁 Output: {output_folder}")
            print()
            
            # Run analysis
            results = self.analyser.analyse_with_config(
                config=self.config,
                source_directory=source_folder,
                result_directory=output_folder
            )
            
            self._display_results(results)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            sys.exit(1)
    
    def _display_results(self, results: dict) -> None:
        """
        Display analysis results.
        
        Args:
            results: Analysis results dictionary
        """
        print("📊 Analysis Results:")
        print(f"  Total Clouds: {results.get('total_clouds', 0)}")
        print(f"  Total Assets: {results.get('total_assets', 0)}")
        print(f"  Processing Time: {results.get('processing_time_seconds', 0):.2f} seconds")
        
        if results.get('clouds'):
            print(f"\n📋 Cloud Details:")
            for cloud_name, cloud_data in results['clouds'].items():
                print(f"  {cloud_name}:")
                print(f"    Teams: {cloud_data.get('total_cloud_team', 0)}")
                print(f"    Assets: {cloud_data.get('total_cloud_assets', 0)}")
        
        print("\n✅ Analysis complete!")
    
    def validate_input(self, config_path: str, source_folder: str, output_folder: str) -> bool:
        """
        Validate input parameters.
        
        Args:
            config_path: Path to configuration file
            source_folder: Path to source directory
            output_folder: Path to output directory
            
        Returns:
            True if valid, False otherwise
        """
        if not config_path:
            print("❌ Error: Configuration file path is required")
            return False
        
        if not source_folder:
            print("❌ Error: Source directory path is required")
            return False
        
        if not output_folder:
            print("❌ Error: Output directory path is required")
            return False
        
        if not Path(config_path).exists():
            print(f"❌ Error: Configuration file does not exist: {config_path}")
            return False
        
        if not Path(source_folder).exists():
            print(f"❌ Error: Source directory does not exist: {source_folder}")
            return False
        
        if not Path(source_folder).is_dir():
            print(f"❌ Error: Source path is not a directory: {source_folder}")
            return False
        
        return True
