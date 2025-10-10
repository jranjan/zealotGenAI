"""
Flattener - Handles data flattening operations
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional
from .base import Transformer


class Flattener(Transformer):
    """Handles data flattening operations"""
    
    def __init__(self):
        super().__init__()
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """Transform all JSON files in a source directory to a target directory."""
        Path(target_folder).mkdir(parents=True, exist_ok=True)
        
        source_path = Path(source_folder)
        json_files = list(source_path.glob("*.json"))
        
        if not json_files:
            return {
                'success': False,
                'error': 'No JSON files found in source directory',
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'files': []
            }
        
        results = {
            'success': True,
            'total_files': len(json_files),
            'successful': 0,
            'failed': 0,
            'files': []
        }
        
        for json_file in json_files:
            try:
                output_file = Path(target_folder) / f"{json_file.stem}_flattened.json"
                source_analysis = self._analyze_source_file(str(json_file))
                success = self._transform_file(str(json_file), str(output_file))
                
                if success:
                    results['successful'] += 1
                    status = "Success"
                else:
                    results['failed'] += 1
                    status = "Failed"
                
                results['files'].append({
                    'input': str(json_file),
                    'output': str(output_file),
                    'status': status,
                    'source_assets': source_analysis['total_assets'],
                    'normalised_assets': source_analysis['total_assets'],
                    'missing_attribution': source_analysis['missing_attribution'],
                    'missing_properties': source_analysis['missing_properties']
                })
                
            except Exception as e:
                results['failed'] += 1
                results['files'].append({
                    'input': str(json_file),
                    'output': '',
                    'status': f"Error: {str(e)}",
                    'source_assets': 0,
                    'normalised_assets': 0,
                    'missing_attribution': 0,
                    'missing_properties': 0
                })
        
        return results
    
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """Transform a single file."""
        try:
            Path(target_file).parent.mkdir(parents=True, exist_ok=True)
            return self._transform_file(source_file, target_file)
        except Exception as e:
            print(f"Error transforming file {source_file}: {e}")
            return False
    
    def _transform_file(self, input_path: str, output_path: str) -> bool:
        """Transform assets from input file to output file by flattening keys."""
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            assets = data if isinstance(data, list) else [data]
            flattened_assets = [self._flatten_dict(asset, prefix="") for asset in assets]
            
            with open(output_path, 'w', encoding='utf-8') as f:
                if len(flattened_assets) == 1:
                    json.dump(flattened_assets[0], f, indent=2, ensure_ascii=False)
                else:
                    json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            print(f"Error transforming file {input_path}: {e}")
            return False
    
    def _flatten_dict(self, data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Recursively flatten a nested dictionary using dot notation."""
        flattened = {}
        
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                flattened.update(self._flatten_dict(value, new_key))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened.update(self._flatten_dict(item, f"{new_key}[{i}]"))
                    else:
                        flattened[f"{new_key}[{i}]"] = item
            else:
                flattened[new_key] = value
        
        return flattened
    
    def _analyze_source_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a source file to count assets and missing fields"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            assets = data if isinstance(data, list) else [data]
            total_assets = len(assets)
            missing_attribution = sum(1 for asset in assets if not asset.get('assetAttributions'))
            missing_properties = sum(1 for asset in assets if not asset.get('properties'))
            
            return {
                'total_assets': total_assets,
                'missing_attribution': missing_attribution,
                'missing_properties': missing_properties
            }
        except Exception as e:
            return {
                'total_assets': 0,
                'missing_attribution': 0,
                'missing_properties': 0
            }
    
    def get_asset_key_mappings(self, source_file_path: str, flattened_file_path: str, asset_index: int = 0) -> Dict[str, Any]:
        """Get key mappings for a specific asset in a file."""
        try:
            with open(source_file_path, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            with open(flattened_file_path, 'r', encoding='utf-8') as f:
                flattened_data = json.load(f)
            
            if isinstance(source_data, list):
                source_asset = source_data[asset_index] if asset_index < len(source_data) else source_data[0]
            else:
                source_asset = source_data
            
            if isinstance(flattened_data, list):
                flattened_asset = flattened_data[asset_index] if asset_index < len(flattened_data) else flattened_data[0]
            else:
                flattened_asset = flattened_data
            
            mappings = self._get_key_mappings_with_values([source_asset], [flattened_asset])
            
            has_attribution = bool(source_asset.get('assetAttributions'))
            has_properties = bool(source_asset.get('properties'))
            
            for mapping in mappings:
                mapping['is_missing_attribution'] = not has_attribution
                mapping['is_missing_properties'] = not has_properties
            
            value_count = len([m for m in mappings if m.get('type') == 'value'])
            object_count = len([m for m in mappings if m.get('type') == 'object'])
            array_mappings = [m for m in mappings if m.get('type') in ['array', 'array_item', 'array_value']]
            array_count = len(array_mappings)
            
            array_names = []
            for mapping in array_mappings:
                if mapping.get('type') == 'array':
                    key_name = mapping['original_key'].split('.')[-1]
                    array_names.append(key_name)
                elif mapping.get('type') in ['array_item', 'array_value']:
                    key_parts = mapping['original_key'].split('[')[0].split('.')
                    if key_parts:
                        array_names.append(key_parts[-1])
            
            unique_array_names = list(set(array_names))
            array_display = f"{array_count} ({', '.join(unique_array_names)})" if unique_array_names else str(array_count)
            
            return {
                'mappings': mappings,
                'summary': {
                    'total_mappings': len(mappings),
                    'direct_values': value_count,
                    'objects': object_count,
                    'arrays': array_display
                },
                'asset_info': {
                    'name': source_asset.get('name', 'Unknown'),
                    'id': source_asset.get('id', 'Unknown'),
                    'asset_class': source_asset.get('assetClass', 'Unknown')
                }
            }
        except Exception as e:
            return {
                'error': str(e),
                'mappings': [],
                'summary': {'total_mappings': 0, 'direct_values': 0, 'objects': 0, 'arrays': '0'},
                'asset_info': {'name': 'Unknown', 'id': 'Unknown', 'asset_class': 'Unknown'}
            }
    
    def _get_key_mappings_with_values(self, source_assets: List[Dict], flattened_assets: List[Dict]) -> List[Dict]:
        """Get all key mappings with their values and types"""
        mappings = []
        
        for source_asset, flattened_asset in zip(source_assets, flattened_assets):
            original_keys = self._get_all_keys_with_values(source_asset)
            flattened_keys = self._get_all_keys_with_values(flattened_asset)
            
            for orig_key, (orig_value, orig_type) in original_keys.items():
                if orig_key in flattened_keys:
                    flat_value = flattened_keys[orig_key]
                    if orig_value == flat_value or (orig_type in ["object", "array"] and str(orig_value) == str(flat_value)):
                        mappings.append({
                            'original_key': orig_key,
                            'flattened_key': orig_key,
                            'value': str(orig_value)[:100] + ('...' if len(str(orig_value)) > 100 else ''),
                            'type': orig_type
                        })
                        continue
                
                for flat_key, flat_value in flattened_keys.items():
                    if self._is_key_equivalent(orig_key, flat_key, orig_value, flat_value):
                        mappings.append({
                            'original_key': orig_key,
                            'flattened_key': flat_key,
                            'value': str(orig_value)[:100] + ('...' if len(str(orig_value)) > 100 else ''),
                            'type': orig_type
                        })
                        break
        
        return mappings
    
    def _get_all_keys_with_values(self, data: Dict, prefix: str = "") -> Dict[str, tuple]:
        """Recursively get all keys with their values and types"""
        keys = {}
        
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                keys[full_key] = (value, "object")
                nested_keys = self._get_all_keys_with_values(value, full_key)
                keys.update(nested_keys)
            elif isinstance(value, list):
                keys[full_key] = (value, "array")
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        array_key = f"{full_key}[{i}]"
                        keys[array_key] = (item, "array_item")
                        nested_keys = self._get_all_keys_with_values(item, array_key)
                        keys.update(nested_keys)
                    else:
                        array_value_key = f"{full_key}[{i}]"
                        keys[array_value_key] = (item, "array_value")
            else:
                keys[full_key] = (value, "value")
        
        return keys
    
    def _is_key_equivalent(self, orig_key, flat_key, orig_value, flat_value):
        """Check if a flattened key is equivalent to an original key"""
        if orig_key == flat_key:
            return orig_value == flat_value
        
        if orig_key in flat_key:
            if isinstance(orig_value, dict) and isinstance(flat_value, str):
                return str(orig_value) == flat_value
            elif isinstance(orig_value, list) and isinstance(flat_value, str):
                return str(orig_value) == flat_value
            else:
                return orig_value == flat_value
        
        if "[" in flat_key and orig_key.replace("[", "").replace("]", "") in flat_key.replace("[", "").replace("]", ""):
            return orig_value == flat_value
        
        return False
