"""
Flattener - Handles data flattening operations using FlattenerHelper

This module provides a sequential data flattener that leverages FlattenerHelper
for all flattening operations, ensuring consistency and eliminating code duplication.
"""

import json
from pathlib import Path
from typing import Dict, List, Any
from .base import Transformer
from .utils.flattener_helper import FlattenerHelper


class Flattener(Transformer):
    """
    Sequential data flattener that uses FlattenerHelper for all operations.
    
    This class provides a clean interface for flattening JSON assets while
    delegating all actual flattening logic to FlattenerHelper. It handles
    file processing, directory operations, and key mapping analysis.
    """
    
    def __init__(self):
        """Initialize the flattener."""
        super().__init__()
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """
        Transform all JSON files in a source directory to a target directory.
        
        Args:
            source_folder: Path to source directory containing JSON files
            target_folder: Path to target directory for flattened files
            
        Returns:
            Dictionary containing transformation results and statistics
        """
        setup_result = self.setup_directories(source_folder, target_folder)
        if not setup_result['success']:
            return setup_result
        
        source_path = setup_result['source_path']
        target_path = setup_result['target_path']
        json_files = setup_result['json_files']
        total_files = setup_result['total_files']
        
        if total_files == 0:
            return {
                'success': False,
                'error': 'No JSON files found in source directory',
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'files': []
            }
        
        results = []
        successful = 0
        failed = 0
        
        for json_file in json_files:
            try:
                result = self._process_single_file(str(json_file), str(target_path))
                results.append(result)
                
                if result['success']:
                    successful += 1
                else:
                    failed += 1
                    
            except Exception as e:
                failed += 1
                error_result = self.create_file_result(
                    success=False,
                    file_path=str(json_file),
                    error=str(e),
                    input=str(json_file),
                    output=None,
                    source_assets=0,
                    normalised_assets=0,
                    missing_attribution=0,
                    missing_properties=0
                )
                results.append(error_result)
        
        return self.create_result_dict(
            success=True,
            total_files=total_files,
            successful=successful,
            failed=failed,
            files=results,
            processing_time=0
        )
    
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """
        Transform a single file.
        
        Args:
            source_file: Path to source file
            target_file: Path to target file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            Path(target_file).parent.mkdir(parents=True, exist_ok=True)
            result = self._process_single_file(source_file, str(Path(target_file).parent))
            return result['success']
        except Exception as e:
            print(f"Error transforming file {source_file}: {e}")
            return False
    
    def _process_single_file(self, file_path: str, target_dir: str) -> Dict[str, Any]:
        """
        Process a single JSON file and create flattened version using FlattenerHelper.
        
        Args:
            file_path: Path to source JSON file
            target_dir: Path to target directory
            
        Returns:
            Dictionary containing processing results
        """
        try:
            source_path = Path(file_path)
            target_path = Path(target_dir)
            
            flattened_filename = f"{source_path.stem}_flattened.json"
            output_path = target_path / flattened_filename
            
            # Load and normalize JSON data
            assets = self._load_and_normalize_json(file_path)
            
            # Use FlattenerHelper to flatten assets with analysis
            flattened_assets, missing_attribution, missing_properties, missing_name, missing_parent_cloud = self._flatten_assets_with_analysis(assets)
            
            # Save flattened data
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
            
            return self.create_file_result(
                success=True,
                file_path=str(source_path),
                input=str(source_path),
                output=str(output_path),
                source_assets=len(assets),
                normalised_assets=len(flattened_assets),
                missing_attribution=missing_attribution,
                missing_properties=missing_properties,
                missing_name=missing_name,
                missing_parent_cloud=missing_parent_cloud
            )
            
        except Exception as e:
            return self.create_file_result(
                success=False,
                file_path=file_path,
                error=str(e),
                input=file_path,
                output=None,
                source_assets=0,
                normalised_assets=0,
                missing_attribution=0,
                missing_properties=0
            )
    
    def get_asset_key_mappings(self, source_file_path: str, flattened_file_path: str, asset_index: int = 0) -> Dict[str, Any]:
        """
        Get key mappings for a specific asset using FlattenerHelper patterns.
        
        Args:
            source_file_path: Path to source JSON file
            flattened_file_path: Path to flattened JSON file
            asset_index: Index of asset to analyze
            
        Returns:
            Dictionary containing key mappings and analysis results
        """
        try:
            # Load source and flattened data
            with open(source_file_path, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            with open(flattened_file_path, 'r', encoding='utf-8') as f:
                flattened_data = json.load(f)
            
            # Ensure data is lists
            if not isinstance(source_data, list):
                source_data = [source_data]
            if not isinstance(flattened_data, list):
                flattened_data = [flattened_data]
            
            # Get specific asset
            if asset_index >= len(source_data) or asset_index >= len(flattened_data):
                return {'error': 'Asset index out of range'}
            
            source_asset = source_data[asset_index]
            flattened_asset = flattened_data[asset_index]
            
            
            # Get key mappings using FlattenerHelper patterns
            mappings = self._get_key_mappings_using_helper(source_asset, flattened_asset)
            
            # Calculate summary statistics
            summary = self._calculate_summary_statistics(mappings, source_asset)
            
            # Add flags to each mapping
            for mapping in mappings:
                mapping['is_missing_attribution'] = summary['missing_attribution']
                mapping['is_missing_properties'] = summary['missing_properties']
            
            asset_info = {
                'name': source_asset.get('name', 'Unknown'),
                'asset_class': source_asset.get('assetClass', 'Unknown'),
                'id': source_asset.get('id', 'Unknown')
            }
            
            return {
                'success': True,
                'source_file': source_file_path,
                'flattened_file': flattened_file_path,
                'asset_index': asset_index,
                'mappings': mappings,
                'summary': summary,
                'asset_info': asset_info,
                'total_mappings': len(mappings)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'source_file': source_file_path,
                'flattened_file': flattened_file_path,
                'asset_index': asset_index
            }
    
    def _get_key_mappings_using_helper(self, source_asset: Dict[str, Any], flattened_asset: Dict[str, Any]) -> List[Dict]:
        """
        Get key mappings using FlattenerHelper patterns.
        
        Args:
            source_asset: Original asset data
            flattened_asset: Flattened asset data
            
        Returns:
            List of key mapping dictionaries
        """
        mappings = []
        
        # Process each flattened key using FlattenerHelper patterns
        for flat_key, flat_value in flattened_asset.items():
            # Determine original key based on FlattenerHelper patterns
            original_key = self._get_original_key_from_flattened(flat_key)
            
            # Get value type
            value_type = self._get_value_type(flat_value)
            
            mappings.append({
                'original_key': original_key,
                'flattened_key': flat_key,
                'value': str(flat_value),
                'type': value_type
            })
        
        return mappings
    
    def _get_original_key_from_flattened(self, flattened_key: str) -> str:
        """
        Get original key path from flattened key using FlattenerHelper patterns.
        
        Args:
            flattened_key: Flattened key (e.g., 'properties_mbu')
            
        Returns:
            Original key path (e.g., 'properties.mbu')
        """
        if flattened_key.startswith('properties_'):
            return f"properties.{flattened_key.replace('properties_', '')}"
        elif flattened_key.startswith('tags_'):
            return f"tags.{flattened_key.replace('tags_', '')}"
        elif flattened_key in ['parent_cloud', 'parent_cloud_id', 'parent_cloud_owner_email', 
                              'cloud', 'cloud_id', 'cloud_owner_email',
                              'team', 'team_id', 'team_owner_email']:
            return f"assetAttributions[0].{flattened_key}"
        else:
            return flattened_key
    
    def _calculate_summary_statistics(self, mappings: List[Dict], source_asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate summary statistics for the mappings.
        
        Args:
            mappings: List of key mappings
            source_asset: Original asset data
            
        Returns:
            Dictionary containing summary statistics
        """
        # Count by type
        direct_values = sum(1 for m in mappings if m.get('type') in ['value', 'string', 'number', 'boolean', 'null'])
        objects = sum(1 for m in mappings if m.get('type') == 'object')
        arrays = sum(1 for m in mappings if m.get('type') == 'array')
        
        # Check for missing data
        is_missing_attribution = 'assetAttributions' not in source_asset or not source_asset.get('assetAttributions')
        is_missing_properties = 'properties' not in source_asset or not source_asset.get('properties')
        
        # Check for missing name and parent cloud
        name_value = source_asset.get('name')
        has_name = bool(name_value and str(name_value).strip() not in ['', 'None', 'null'])
        
        has_parent_cloud = any(
            mapping.get('flattened_key') == 'parent_cloud' and 
            mapping.get('value') and 
            str(mapping.get('value')).strip() not in ['None', 'null', '']
            for mapping in mappings
        )
        
        return {
            'total_mappings': len(mappings),
            'direct_values': direct_values,
            'objects': objects,
            'arrays': arrays,
            'missing_attribution': is_missing_attribution,
            'missing_properties': is_missing_properties,
            'has_name': has_name,
            'has_parent_cloud': has_parent_cloud
        }
    
    def _get_value_type(self, value: Any) -> str:
        """
        Get the type of a value for display purposes.
        
        Args:
            value: Value to analyze
            
        Returns:
            String representation of the value type
        """
        if isinstance(value, dict):
            return "object"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, (int, float)):
            return "number"
        elif isinstance(value, bool):
            return "boolean"
        elif value is None:
            return "null"
        else:
            return "value"
    
    def get_performance_info(self) -> Dict[str, Any]:
        """
        Get performance information about the flattener.
        
        Returns:
            Dictionary containing performance information
        """
        return {
            'flattener_type': 'Flattener (Sequential)',
            'uses_flattener_helper': True,
            'optimized': True
        }
