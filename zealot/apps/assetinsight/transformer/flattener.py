"""
Flattener - Handles data flattening operations
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from .base import Transformer
from .utils.flattener_helper import FlattenerHelper


class Flattener(Transformer):
    """Handles data flattening operations using sequential processing"""
    
    def __init__(self):
        super().__init__()
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """Transform all JSON files in a source directory to a target directory."""
        # Use base class setup logic
        setup_result = self.setup_directories(source_folder, target_folder)
        if not setup_result['success']:
            return setup_result
        
        # Extract setup results
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
            processing_time=0  # Sequential processing doesn't track time
        )
    
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """Transform a single file."""
        try:
            Path(target_file).parent.mkdir(parents=True, exist_ok=True)
            result = self._process_single_file(source_file, str(Path(target_file).parent))
            return result['success']
        except Exception as e:
            print(f"Error transforming file {source_file}: {e}")
            return False
    
    def _process_single_file(self, file_path: str, target_dir: str) -> Dict[str, Any]:
        """Process a single JSON file and create flattened version."""
        try:
            source_path = Path(file_path)
            target_path = Path(target_dir)
            
            # Create flattened filename
            flattened_filename = f"{source_path.stem}_flattened.json"
            output_path = target_path / flattened_filename
            
            # Load and normalize JSON data
            assets = self._load_and_normalize_json(file_path)
            
            # Flatten assets with analysis
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
            
            # Check for missing name and parent cloud
            name_value = source_asset.get('name')
            has_name = bool(name_value and str(name_value).strip() not in ['', 'None', 'null'])
            
            has_parent_cloud = any(
                mapping.get('flattened_key') == 'parent_cloud' and 
                mapping.get('value') and 
                str(mapping.get('value')).strip() not in ['None', 'null', '']
                for mapping in mappings
            )
            
            summary = {
                'total_mappings': len(mappings),
                'direct_values': direct_values,
                'objects': objects,
                'arrays': arrays,
                'missing_attribution': is_missing_attribution,
                'missing_properties': is_missing_properties,
                'has_name': has_name,
                'has_parent_cloud': has_parent_cloud
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
                else:
                    # For flattened-only fields (like ownership fields), use the flattened value
                    orig_value = None
                    orig_type = flat_type

                mappings.append({
                    'original_key': original_key or 'N/A',
                    'flattened_key': flat_key,
                    'value': str(flat_value)[:100] + ('...' if len(str(flat_value)) > 100 else ''),
                    'type': flat_type
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
    
    def get_performance_info(self) -> Dict[str, Any]:
        """Get performance information about the flattener"""
        return {
            'flattener_type': 'Flattener (Sequential)'
        }
    
