"""
SupersonicFlattener - High-performance multiprocessing data flattening operations
Uses multiprocessing to bypass Python's GIL for maximum performance with large file counts
"""

import json
import os
import pickle
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional
from multiprocessing import Pool, cpu_count, Manager
import time
import tempfile
from .base import Transformer


class SupersonicFlattener(Transformer):
    """Supersonic-high-performance multiprocessing data flattening operations"""
    
    def __init__(self, max_workers: int = None, chunk_size: int = 100):
        super().__init__()
        self.max_workers = max_workers or min(cpu_count(), 16)  # Limit to prevent memory issues
        self.chunk_size = chunk_size  # Process files in chunks to manage memory
        self._temp_dir = None
    
    def transform_directory(self, source_dir: str, target_dir: str) -> Dict[str, Any]:
        """
        Transform all JSON files in source directory using multiprocessing.
        
        Args:
            source_dir: Source directory containing JSON files
            target_dir: Target directory for transformed files
            
        Returns:
            Dictionary with transformation results
        """
        start_time = time.time()
        
        # Use base class setup logic
        setup_result = self.setup_directories(source_dir, target_dir)
        if not setup_result['success']:
            return setup_result
        
        # Extract setup results
        source_path = setup_result['source_path']
        target_path = setup_result['target_path']
        json_files = setup_result['json_files']
        total_files = setup_result['total_files']
        
        print(f"🚀 SupersonicFlattener: Processing {len(json_files)} files with {self.max_workers} processes")
        
        # Split files into chunks for processing
        file_chunks = [
            [str(f) for f in json_files[i:i + self.chunk_size]]
            for i in range(0, len(json_files), self.chunk_size)
        ]
        
        all_results = []
        successful = 0
        failed = 0
        
        # Process chunks in parallel using multiprocessing Pool
        with Pool(processes=self.max_workers) as pool:
            # Submit all chunks with target directory
            chunk_args = [(chunk, target_dir) for chunk in file_chunks]
            chunk_results = pool.starmap(_process_file_chunk, chunk_args)
            
            # Collect results
            for chunk_result in chunk_results:
                all_results.extend(chunk_result['results'])
                successful += chunk_result['successful']
                failed += chunk_result['failed']
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"🎯 SupersonicFlattener: Completed {total_files} files in {processing_time:.2f}s ({total_files / processing_time:.1f} files/sec)")
        
        return self.create_result_dict(
            success=True,
            total_files=total_files,
            successful=successful,
            failed=failed,
            files=all_results,
            processing_time=processing_time
        )
    
    def get_asset_key_mappings(self, source_file_path: str, flattened_file_path: str, asset_index: int = 0) -> Dict[str, Any]:
        """Get key mappings for a specific asset in a file."""
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
            
            # Get key mappings with values
            mappings = self._get_key_mappings_with_values([source_asset], [flattened_asset])
            
            # Calculate summary statistics
            direct_values = sum(1 for m in mappings if m.get('type') == 'value')
            objects = sum(1 for m in mappings if m.get('type') == 'object')
            arrays = sum(1 for m in mappings if m.get('type') == 'array')
            
            # Check for missing attribution and properties
            is_missing_attribution = 'assetAttributions' not in source_asset or not source_asset.get('assetAttributions')
            is_missing_properties = 'properties' not in source_asset or not source_asset.get('properties')
            
            summary = {
                'total_mappings': len(mappings),
                'direct_values': direct_values,
                'objects': objects,
                'arrays': arrays,
                'missing_attribution': is_missing_attribution,
                'missing_properties': is_missing_properties
            }
            
            # Add missing attribution/properties flags to each mapping
            for mapping in mappings:
                mapping['is_missing_attribution'] = is_missing_attribution
                mapping['is_missing_properties'] = is_missing_properties
            
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
    
    def _get_key_mappings_with_values(self, source_assets: List[Dict], flattened_assets: List[Dict]) -> List[Dict]:
        """Get all key mappings with their values and types"""
        mappings = []

        for source_asset, flattened_asset in zip(source_assets, flattened_assets):
            # Get all flattened keys with their values
            flattened_keys = self._get_all_keys_with_values(flattened_asset)

            # For each flattened key, find its corresponding original key
            for flat_key, (flat_value, flat_type) in flattened_keys.items():
                original_key = self._find_original_key_for_flattened(source_asset, flat_key)

                if original_key:
                    # Get the original value
                    orig_value = self._get_nested_value(source_asset, original_key)
                    orig_type = self._get_value_type(orig_value)

                    mappings.append({
                        'original_key': original_key,
                        'flattened_key': flat_key,
                        'value': str(orig_value)[:100] + ('...' if len(str(orig_value)) > 100 else ''),
                        'type': orig_type
                    })

        return mappings
    
    def _get_all_keys_with_values(self, data: Dict, prefix: str = "") -> Dict[str, tuple]:
        """Recursively get all keys with their values and types"""
        result = {}
        
        for key, value in data.items():
            current_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                result.update(self._get_all_keys_with_values(value, current_key))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        result.update(self._get_all_keys_with_values(item, f"{current_key}[{i}]"))
                    else:
                        result[f"{current_key}[{i}]"] = (item, self._get_value_type(item))
            else:
                result[current_key] = (value, self._get_value_type(value))
        
        return result
    
    def _find_original_key_for_flattened(self, source_data: Dict, flattened_key: str) -> str:
        """Find the original nested key path for a flattened key"""
        # Handle array indices in flattened key
        if '[' in flattened_key and ']' in flattened_key:
            # Split by array indices
            parts = []
            current = ""
            i = 0
            while i < len(flattened_key):
                if flattened_key[i] == '[':
                    if current:
                        parts.append(current)
                        current = ""
                    # Find the closing bracket
                    j = i + 1
                    while j < len(flattened_key) and flattened_key[j] != ']':
                        j += 1
                    if j < len(flattened_key):
                        index = flattened_key[i+1:j]
                        parts.append(f"[{index}]")
                        i = j + 1
                    else:
                        current += flattened_key[i]
                        i += 1
                else:
                    current += flattened_key[i]
                    i += 1
            if current:
                parts.append(current)
        else:
            parts = flattened_key.split('.')
        
        # Reconstruct the original key path
        result_parts = []
        for part in parts:
            if part.startswith('[') and part.endswith(']'):
                result_parts.append(part)
            else:
                result_parts.append(part)
        
        return '.'.join(result_parts) if '[' in flattened_key and ']' in flattened_key else flattened_key
    
    def _get_nested_value(self, data: Dict, key_path: str) -> Any:
        """Get a value from nested dictionary using dot notation key path"""
        keys = key_path.split('.')
        current = data
        
        for key in keys:
            if '[' in key and ']' in key:
                # Handle array access
                base_key = key.split('[')[0]
                if base_key in current:
                    current = current[base_key]
                    # Extract index
                    index_str = key.split('[')[1].split(']')[0]
                    try:
                        index = int(index_str)
                        if isinstance(current, list) and 0 <= index < len(current):
                            current = current[index]
                        else:
                            return None
                    except (ValueError, IndexError):
                        return None
                else:
                    return None
            else:
                if key in current:
                    current = current[key]
                else:
                    return None
        
        return current
    
    def _get_value_type(self, value: Any) -> str:
        """Get the type of a value for display purposes"""
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
    
    def transform_file(self, source_file: str, target_file: str) -> Dict[str, Any]:
        """
        Transform a single JSON file.
        
        Args:
            source_file: Path to source JSON file
            target_file: Path to target JSON file
            
        Returns:
            Dictionary with transformation results
        """
        try:
            # Load source data
            with open(source_file, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            # Ensure data is a list
            if not isinstance(source_data, list):
                source_data = [source_data]
            
            # Flatten each asset
            flattened_assets = []
            missing_attribution = 0
            missing_properties = 0
            
            for asset in source_data:
                if not isinstance(asset, dict):
                    continue
                
                # Check for missing attribution
                if 'assetAttributions' not in asset or not asset['assetAttributions']:
                    missing_attribution += 1
                
                # Check for missing properties
                if 'properties' not in asset or not asset['properties']:
                    missing_properties += 1
                
                # Flatten the asset
                flattened_asset = _flatten_asset(asset)
                flattened_assets.append(flattened_asset)
            
            # Save flattened data
            with open(target_file, 'w', encoding='utf-8') as f:
                json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
            
            return {
                'success': True,
                'input': source_file,
                'output': target_file,
                'source_assets': len(source_data),
                'normalised_assets': len(flattened_assets),
                'missing_attribution': missing_attribution,
                'missing_properties': missing_properties
            }
            
        except Exception as e:
            return {
                'success': False,
                'input': source_file,
                'output': target_file,
                'error': str(e),
                'source_assets': 0,
                'normalised_assets': 0,
                'missing_attribution': 0,
                'missing_properties': 0
            }
    
    def get_performance_info(self) -> Dict[str, Any]:
        """Get performance information about the flattener"""
        return {
            'max_workers': self.max_workers,
            'chunk_size': self.chunk_size,
            'cpu_count': cpu_count(),
            'flattener_type': 'SupersonicFlattener (Multiprocessing)'
        }


def _create_file_result(success: bool, file_path: str, error: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """Helper function to create file result (for multiprocessing compatibility)."""
    result = {
        'success': success,
        'file': file_path,
        'error': error
    }
    result.update(kwargs)
    return result

def _flatten_json_data(data: Any, parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Helper function to flatten JSON data (for multiprocessing compatibility)."""
    items = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(_flatten_json_data(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Handle lists by creating indexed keys
                for i, item in enumerate(value):
                    list_key = f"{new_key}[{i}]"
                    if isinstance(item, dict):
                        items.extend(_flatten_json_data(item, list_key, sep=sep).items())
                    else:
                        items.append((list_key, item))
            else:
                items.append((new_key, value))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            list_key = f"{parent_key}[{i}]" if parent_key else f"[{i}]"
            if isinstance(item, dict):
                items.extend(_flatten_json_data(item, list_key, sep=sep).items())
            else:
                items.append((list_key, item))
    else:
        items.append((parent_key, data))
    
    return dict(items)

def _process_single_file(file_path: str, target_dir: str) -> Dict[str, Any]:
    """
    Process a single JSON file and create flattened version.
    This function runs in a separate process.
    
    Args:
        file_path: Path to source JSON file
        target_dir: Target directory for output
        
    Returns:
        Dictionary with processing results
    """
    try:
        source_path = Path(file_path)
        target_path = Path(target_dir)
        
        # Create flattened filename
        flattened_filename = f"{source_path.stem}_flattened.json"
        output_path = target_path / flattened_filename
        
        # Load source data
        with open(source_path, 'r', encoding='utf-8') as f:
            source_data = json.load(f)
        
        # Ensure data is a list
        if not isinstance(source_data, list):
            source_data = [source_data]
        
        # Flatten each asset
        flattened_assets = []
        missing_attribution = 0
        missing_properties = 0
        
        for asset in source_data:
            if not isinstance(asset, dict):
                continue
            
            # Check for missing attribution
            if 'assetAttributions' not in asset or not asset['assetAttributions']:
                missing_attribution += 1
            
            # Check for missing properties
            if 'properties' not in asset or not asset['properties']:
                missing_properties += 1
            
            # Flatten the asset using helper function
            flattened_asset = _flatten_json_data(asset)
            flattened_assets.append(flattened_asset)
        
        # Save flattened data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
        
        return _create_file_result(
            success=True,
            file_path=str(source_path),
            input=str(source_path),
            output=str(output_path),
            source_assets=len(source_data),
            normalised_assets=len(flattened_assets),
            missing_attribution=missing_attribution,
            missing_properties=missing_properties
        )
        
    except Exception as e:
        return _create_file_result(
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




def _process_file_chunk(file_paths: List[str], target_dir: str) -> Dict[str, Any]:
    """
    Process a chunk of files in a single process.
    
    Args:
        file_paths: List of file paths to process
        target_dir: Target directory for output files
        
    Returns:
        Dictionary with chunk processing results
    """
    chunk_results = []
    successful = 0
    failed = 0
    
    for file_path in file_paths:
        result = _process_single_file(file_path, target_dir)
        chunk_results.append(result)
        
        if result['success']:
            successful += 1
        else:
            failed += 1
    
    return {
        'results': chunk_results,
        'successful': successful,
        'failed': failed
    }
