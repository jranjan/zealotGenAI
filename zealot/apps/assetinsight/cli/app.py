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

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

# Import from local modules
from analyser import AssetAnalyser
from configreader import AssetFieldConfig
from common import AnalyserFactory


class AssetAnalyserApp:
    """
    Application wrapper for AssetAnalyser that handles configuration-based analysis.
    
    This class provides a higher-level interface for using AssetAnalyser with
    YAML configuration files and manages multiple asset class processing.
    """
    
    def __init__(self, config_path: Union[str, Path], analyser_type: str = "owner"):
        """
        Initialize the asset analyser app.
        
        Args:
            config_path: Path to the YAML configuration file
            analyser_type: Type of analyser to use ('owner', 'security', 'network')
        """
        self.config_path = Path(config_path)
        self.analyser_type = analyser_type
        self.analyser = self._create_analyser(analyser_type)
        self.results: List[Dict[str, Any]] = []
        
        # Load configuration
        self.config = AssetFieldConfig(config_path)
    
    def _create_analyser(self, analyser_type: str) -> AssetAnalyser:
        """Create the appropriate analyser based on type."""
        return AnalyserFactory.create_analyser(analyser_type)
    
    def analyse_all_assets(self) -> List[Dict[str, Any]]:
        """
        Analyse all asset classes defined in the configuration.
        
        Returns:
            List of analysis result dictionaries for each asset class
        """
        print(f"🚀 Starting {self.analyser_type.title()} Analysis")
        print("=" * 50)
        
        self.results = []
        asset_names = self.config.get_asset_names()
        
        if not asset_names:
            print("⚠️  No assets found in configuration")
            return []
        
        print(f"📋 Found {len(asset_names)} asset classes to process")
        
        for asset_name in asset_names:
            try:
                # Use the configured analyser
                result = self.analyser.analyse(
                    source_directory="data/source",  # This should come from config
                    result_directory="data/results"  # This should come from config
                )
                self.results.append(result)
                
                print(f"✅ {asset_name}: Analysis completed using {self.analyser_type} analyser")
                
            except Exception as e:
                print(f"❌ Error processing {asset_name}: {e}")
                continue
        
        print("=" * 50)
        print("🎉 Analysis Complete!")
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
        
        # Since results are now dictionaries, we need to handle them differently
        total_assets = 0
        total_files = 0
        
        results_by_class = {}
        for i, result in enumerate(self.results):
            # Handle both old AssetClassResult format and new dictionary format
            if isinstance(result, dict):
                asset_class = result.get('asset_class', f'asset_{i}')
                total_assets += result.get('total_assets', 0)
                processing_stats = result.get('processing_stats', {})
                total_files += processing_stats.get('files_created', 0)
                
                results_by_class[asset_class] = {
                    'total_assets': result.get('total_assets', 0),
                    'parent_clouds': processing_stats.get('parent_clouds_count', 0),
                    'files_created': processing_stats.get('files_created', 0),
                    'source_path': result.get('source_path', 'N/A'),
                    'result_path': result.get('result_path', 'N/A')
                }
            else:
                # Fallback for old format
                total_assets += getattr(result, 'total_assets', 0)
                processing_stats = getattr(result, 'processing_stats', {})
                total_files += processing_stats.get('files_created', 0)
                
                results_by_class[getattr(result, 'asset_class', f'asset_{i}')] = {
                    'total_assets': getattr(result, 'total_assets', 0),
                    'parent_clouds': processing_stats.get('parent_clouds_count', 0),
                    'files_created': processing_stats.get('files_created', 0),
                    'source_path': getattr(result, 'source_path', 'N/A'),
                    'result_path': getattr(result, 'result_path', 'N/A')
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
                'total_asset_classes_configured': len(self.config.get_asset_names())
            },
            'detailed_results': [
                {
                    'asset_class': result.get('asset_class', f'asset_{i}'),
                    'source_path': result.get('source_path', 'N/A'),
                    'result_path': result.get('result_path', 'N/A'),
                    'total_assets': result.get('total_assets', 0),
                    'parent_clouds': result.get('parent_clouds', []),
                    'processing_stats': result.get('processing_stats', {})
                }
                for i, result in enumerate(self.results)
            ]
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"📊 Analysis report saved: {report_path}")
        return report_path
    
    def run_analysis(self) -> List[Dict[str, Any]]:
        """
        Run complete analysis with summary and report generation.
        
        Returns:
            List of analysis result dictionaries for each asset class
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
