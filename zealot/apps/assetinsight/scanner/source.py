"""
Source Scanner - Concrete implementation for source data inspection
"""

import json
import os
from pathlib import Path
from typing import Dict, Any
from .base import Scanner


class SourceScanner(Scanner):
    """Handles source data inspection and validation"""
    
    def __init__(self):
        super().__init__()
    
    def scan_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Scan a directory and return inspection results.
        
        Args:
            directory_path: Path to the directory to scan
            
        Returns:
            Dictionary with scanning results
        """
        return self.scan_source_directory(directory_path)
    
    def get_directory_info(self, directory_path: str) -> Dict[str, Any]:
        """
        Get basic directory information without full scanning.
        
        Args:
            directory_path: Path to the directory
            
        Returns:
            Dictionary with basic directory information
        """
        validation = self.validate_directory(directory_path)
        if not validation['valid']:
            return validation
        
        file_count = self.count_files(directory_path, "*.json")
        json_files = list(Path(directory_path).glob("*.json"))
        return {
            'valid': True,
            'total_files': file_count,
            'json_files': [f.name for f in json_files[:5]]
        }
    
    def scan_source_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Scan source directory and analyze JSON files.
        
        Args:
            directory_path: Path to source directory
            
        Returns:
            Dictionary with analysis results
        """
        validation = self.validate_directory(directory_path)
        if not validation['valid']:
            return {
                'success': False,
                'error': validation['error'],
                'total_files': 0,
                'estimated_assets': 0,
                'source_folder': directory_path,
                'file_details': []
            }
        
        try:
            source_path = Path(directory_path)
            json_files = list(source_path.glob("*.json"))
            
            if not json_files:
                return {
                    'success': True,
                    'total_files': 0,
                    'estimated_assets': 0,
                    'source_folder': directory_path,
                    'file_details': []
                }
            
            total_assets = 0
            file_details = []
            
            for json_file in json_files:
                try:
                    file_size = json_file.stat().st_size
                    file_size_mb = file_size / (1024 * 1024)
                    
                    # Count assets in file
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if isinstance(data, list):
                        asset_count = len(data)
                    else:
                        asset_count = 1
                    
                    total_assets += asset_count
                    
                    file_details.append({
                        'File': json_file.name,
                        'Size (MB)': file_size_mb,
                        'Assets': asset_count
                    })
                    
                except Exception as e:
                    print(f"Warning: Could not process {json_file.name}: {e}")
                    continue
            
            return {
                'success': True,
                'total_files': len(json_files),
                'estimated_assets': total_assets,
                'source_folder': directory_path,
                'file_details': file_details
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'total_files': 0,
                'estimated_assets': 0,
                'source_folder': directory_path,
                'file_details': []
            }
