#!/usr/bin/env python3
"""
Asset Insight CLI Application

This module provides a command-line interface for the Asset Insight application.
It uses YAML configuration files to define asset classes and their processing parameters.
"""

import sys
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Union

# Add the current directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import from local modules
from analyser.asset import AssetAnalyser, AssetClassResult
from config.yaml import AssetConfig


class AssetAnalyserApp:
    """
    Application wrapper for AssetAnalyser that handles configuration-based analysis.
    
    This class provides a higher-level interface for using AssetAnalyser with
    YAML configuration files and manages multiple asset class processing.
    """
    
    def __init__(self, config_path: Union[str, Path]):
        """
        Initialize the asset analyser app.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        self.analyser = AssetAnalyser()
        self.results: List[AssetClassResult] = []
        
        # Load configuration
        self.config = AssetConfig(config_path)
        
        if not self.config.is_valid():
            raise ValueError(f"Invalid configuration file: {config_path}")
    
    def analyse_all_assets(self) -> List[AssetClassResult]:
        """
        Analyse all asset classes defined in the configuration.
        
        Returns:
            List of AssetClassResult objects for each asset class
        """
        print("🚀 Starting Asset Analysis")
        print("=" * 50)
        
        self.results = []
        assets = self.config.get_assets()
        
        if not assets:
            print("⚠️  No assets found in configuration")
            return []
        
        print(f"📋 Found {len(assets)} asset classes to process")
        
        for asset_config in assets:
            try:
                # Create a fresh analyser for each asset class to ensure clean processing
                fresh_analyser = AssetAnalyser()
                
                # Get field configurations for this asset
                asset_fields = self.config.get_asset_fields(asset_config.name)
                cloud_fields = self.config.get_cloud_fields(asset_config.name)
                
                result = fresh_analyser.analyse_asset_class(
                    asset_class_name=asset_config.name,
                    source_path=asset_config.source_id,
                    result_path=asset_config.result_id,
                    asset_fields=asset_fields,
                    cloud_fields=cloud_fields
                )
                self.results.append(result)
                
                print(f"✅ {result.asset_class}: {result.total_assets} assets, {result.processing_stats['parent_clouds_count']} clouds")
                
            except Exception as e:
                print(f"❌ Error processing {asset_config.name}: {e}")
                continue
        
        return self.results
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the analysis results.
        
        Returns:
            Dictionary with analysis summary
        """
        if not self.results:
            return {
                'total_asset_classes': 0,
                'total_assets_processed': 0,
                'total_files_created': 0,
                'results_by_asset_class': {}
            }
        
        total_assets = sum(result.total_assets for result in self.results)
        total_files = sum(result.processing_stats['files_created'] for result in self.results)
        
        results_by_class = {}
        for result in self.results:
            results_by_class[result.asset_class] = {
                'total_assets': result.total_assets,
                'parent_clouds': result.processing_stats['parent_clouds_count'],
                'files_created': result.processing_stats['files_created'],
                'source_path': result.source_path,
                'result_path': result.result_path
            }
        
        return {
            'total_asset_classes': len(self.results),
            'total_assets_processed': total_assets,
            'total_files_created': total_files,
            'results_by_asset_class': results_by_class
        }
    
    def save_analysis_report(self, output_path: Union[str, Path] = "analysis_report.json") -> Path:
        """
        Save a detailed analysis report.
        
        Args:
            output_path: Path to save the analysis report
            
        Returns:
            Path to the saved report file
        """
        report_path = Path(output_path)
        
        # Get analysis summary
        summary = self.get_analysis_summary()
        
        # Add detailed results
        report = {
            'analysis_summary': summary,
            'configuration': {
                'config_file': str(self.config_path),
                'total_asset_classes_configured': len(self.config.get_assets())
            },
            'detailed_results': [
                {
                    'asset_class': result.asset_class,
                    'source_path': result.source_path,
                    'result_path': result.result_path,
                    'total_assets': result.total_assets,
                    'parent_clouds': result.parent_clouds,
                    'processing_stats': result.processing_stats
                }
                for result in self.results
            ]
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Analysis report saved: {report_path}")
        return report_path
    
    def run_analysis(self) -> List[AssetClassResult]:
        """
        Run complete analysis with summary and report generation.
        
        Returns:
            List of AssetClassResult objects for each asset class
        """
        print(f"📋 Loading configuration from: {self.config_path}")
        
        # Analyse all assets
        print("\n🔄 Starting asset analysis...")
        results = self.analyse_all_assets()
        
        # Get summary
        summary = self.get_analysis_summary()
        print(f"\n📊 Analysis Summary:")
        print(f"  Total asset classes: {summary['total_asset_classes']}")
        print(f"  Total assets processed: {summary['total_assets_processed']}")
        print(f"  Total files created: {summary['total_files_created']}")
        
        # Show results by asset class
        print(f"\n📋 Results by Asset Class:")
        for asset_class, stats in summary['results_by_asset_class'].items():
            print(f"  {asset_class}:")
            print(f"    Assets: {stats['total_assets']}")
            print(f"    Parent clouds: {stats['parent_clouds']}")
            print(f"    Files created: {stats['files_created']}")
            print(f"    Source: {stats['source_path']}")
            print(f"    Result: {stats['result_path']}")
        
        # Save detailed report
        report_path = self.save_analysis_report("analysis_report.json")
        print(f"\n📄 Detailed report saved to: {report_path}")
        
        return results


def main():
    """Main CLI function for Asset Insight."""
    parser = argparse.ArgumentParser(description="Asset Insight CLI - Analyze asset data using YAML configuration")
    parser.add_argument("--config", "-c", default="assets.yaml", help="YAML configuration file path (default: assets.yaml)")
    
    args = parser.parse_args()
    
    print("🚀 Asset Insight CLI")
    print("=" * 50)
    
    try:
        # Configuration-based analysis
        app = AssetAnalyserApp(args.config)
        app.run_analysis()
        print(f"\n✅ Asset analysis completed successfully!")
        
    except FileNotFoundError as e:
        print(f"❌ Configuration file not found: {e}")
        print("   Make sure the YAML configuration file exists")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
