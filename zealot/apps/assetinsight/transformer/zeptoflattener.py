"""
ZeptoFlattener - Multithreaded data flattening operations
"""

import json
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
from .base import Transformer


class ZeptoFlattener(Transformer):
    """Multithreaded data flattening operations for high-performance processing"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self._lock = threading.Lock()
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """Transform all JSON files using multithreading."""
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
                'files': [],
                'processing_time_seconds': 0
            }
        
        results = {
            'success': True,
            'total_files': len(json_files),
            'successful': 0,
            'failed': 0,
            'files': [],
            'processing_time_seconds': 0
        }
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._process_single_file, str(json_file), target_folder): json_file
                for json_file in json_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_result = future.result()
                    with self._lock:
                        if file_result['status'] == "Success":
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        results['files'].append(file_result)
                except Exception as e:
                    with self._lock:
                        results['failed'] += 1
                        results['files'].append({
                            'input': str(file_path),
                            'output': '',
                            'status': f"Error: {str(e)}",
                            'source_assets': 0,
                            'normalised_assets': 0,
                            'missing_attribution': 0,
                            'missing_properties': 0
                        })
        
        end_time = time.time()
        results['processing_time_seconds'] = end_time - start_time
        
        if results['failed'] > 0:
            results['success'] = False
        
        return results
    
    def _process_single_file(self, json_file_path: str, target_folder: str) -> Dict[str, Any]:
        """Helper to process a single file for multithreading."""
        output_file = Path(target_folder) / f"{Path(json_file_path).stem}_flattened.json"
        
        source_analysis = self._analyze_source_file(json_file_path)
        success = self._transform_file(json_file_path, str(output_file))
        
        status = "Success" if success else "Failed"
        
        return {
            'input': json_file_path,
            'output': str(output_file) if success else '',
            'status': status,
            'source_assets': source_analysis['total_assets'],
            'normalised_assets': source_analysis['total_assets'],
            'missing_attribution': source_analysis['missing_attribution'],
            'missing_properties': source_analysis['missing_properties']
        }
    
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
