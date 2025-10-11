"""
Transformer Base Class - Abstract base class for data transformation operations
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pathlib import Path
import json
from .utils.flattener_helper import FlattenerHelper


class Transformer(ABC):
    """
    Abstract base class for data transformation operations.
    
    This class defines the interface for different types of data transformation
    operations. Subclasses must implement the specific transformation logic.
    """
    
    def __init__(self):
        """Initialize the transformer."""
        pass
    
    @abstractmethod
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """
        Transform all files in a source directory to a target directory.
        
        Args:
            source_folder: Path to source directory containing files
            target_folder: Path to target directory for transformed files
            
        Returns:
            Dictionary with transformation results
        """
        pass
    
    @abstractmethod
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """
        Transform a single file.
        
        Args:
            source_file: Path to source file
            target_file: Path to target file
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def _process_single_file(self, file_path: str, target_dir: str) -> Dict[str, Any]:
        """
        Process a single JSON file and create flattened version.
        This method must be implemented by derived classes.
        
        Args:
            file_path: Path to source JSON file
            target_dir: Target directory for output
            
        Returns:
            Dictionary with processing results
        """
        pass
    
    def validate_directories(self, source_dir: str, target_dir: str) -> Optional[str]:
        """
        Validate source and target directories with safety checks.
        
        Args:
            source_dir: Path to source directory
            target_dir: Path to target directory
            
        Returns:
            Error message if validation fails, None if validation passes
        """
        try:
            source_path = Path(source_dir).resolve()
            target_path = Path(target_dir).resolve()
            
            # Check if source directory exists
            if not source_path.exists():
                return f"Source directory does not exist: {source_dir}"
            
            # Safety check: prevent writing to source directory
            if target_path == source_path or target_path.is_relative_to(source_path):
                return f"Cannot write to source directory or its subdirectories: {target_dir}"
            
            return None  # Validation passed
            
        except Exception as e:
            return f"Directory validation error: {str(e)}"
    
    def setup_directories(self, source_dir: str, target_dir: str) -> Dict[str, Any]:
        """
        Common setup logic for directory transformation.
        
        Args:
            source_dir: Path to source directory
            target_dir: Path to target directory
            
        Returns:
            Dictionary with setup results or error
        """
        # Validate directories using base class validation
        validation_error = self.validate_directories(source_dir, target_dir)
        if validation_error:
            return {'success': False, 'error': validation_error}
        
        # Create target directory if it doesn't exist
        target_path = Path(target_dir)
        target_path.mkdir(parents=True, exist_ok=True)
        
        # Get all JSON files
        source_path = Path(source_dir)
        json_files = list(source_path.glob("*.json"))
        
        if not json_files:
            return {
                'success': True,
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'files': [],
                'processing_time': 0,
                'files_per_second': 0,
                'error': 'No JSON files found in source directory'
            }
        
        return {
            'success': True,
            'source_path': source_path,
            'target_path': target_path,
            'json_files': json_files,
            'total_files': len(json_files)
        }
    
    def create_result_dict(self, success: bool, total_files: int, successful: int, 
                          failed: int, files: List[Dict], processing_time: float, 
                          error: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a standardized result dictionary.
        
        Args:
            success: Whether the operation was successful
            total_files: Total number of files processed
            successful: Number of successfully processed files
            failed: Number of failed files
            files: List of file processing results
            processing_time: Time taken for processing
            error: Error message if any
            
        Returns:
            Standardized result dictionary
        """
        files_per_second = total_files / processing_time if processing_time > 0 else 0
        
        result = {
            'success': success,
            'total_files': total_files,
            'successful': successful,
            'failed': failed,
            'files': files,
            'processing_time': processing_time,
            'files_per_second': files_per_second
        }
        
        if error:
            result['error'] = error
            
        return result
    
    def create_file_result(self, success: bool, file_path: str, error: Optional[str] = None, 
                          **kwargs) -> Dict[str, Any]:
        """
        Create a standardized file processing result dictionary.
        
        Args:
            success: Whether the file processing was successful
            file_path: Path to the processed file
            error: Error message if processing failed
            **kwargs: Additional fields to include in the result
            
        Returns:
            Standardized file result dictionary
        """
        result = {
            'success': success,
            'file': file_path,
            'error': error
        }
        
        # Add any additional fields
        result.update(kwargs)
        
        return result
    
    
    def get_directory_info(self, directory_path: str) -> Dict[str, Any]:
        """
        Get basic directory information.
        
        Args:
            directory_path: Path to directory
            
        Returns:
            Dictionary with directory information
        """
        try:
            if not Path(directory_path).exists():
                return {
                    'exists': False,
                    'file_count': 0,
                    'error': f"Directory {directory_path} does not exist"
                }
            
            path = Path(directory_path)
            json_files = list(path.glob("*.json"))
            
            return {
                'exists': True,
                'file_count': len(json_files),
                'files': [f.name for f in json_files]
            }
        except Exception as e:
            return {
                'exists': False,
                'file_count': 0,
                'error': str(e)
            }
    
    
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
    
    def _load_and_normalize_json(self, file_path: str) -> List[Dict[str, Any]]:
        """Load JSON file and normalize to list format"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data if isinstance(data, list) else [data]
    
    def _flatten_assets_with_analysis(self, assets: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int, int, int, int]:
        """Flatten assets and count missing attribution/properties"""
        flattened_assets = []
        missing_attribution = 0
        missing_properties = 0
        missing_name = 0
        missing_parent_cloud = 0
        
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            
            # Check for missing attribution
            if 'assetAttributions' not in asset or not asset['assetAttributions']:
                missing_attribution += 1
            
            # Check for missing properties
            if 'properties' not in asset or not asset['properties']:
                missing_properties += 1
            
            # Check for missing name
            name_value = asset.get('name')
            if not name_value or str(name_value).strip() in ['', 'None', 'null']:
                missing_name += 1
            
            # Flatten the asset using FlattenerHelper
            flattened_asset = FlattenerHelper.flatten_asset(asset)
            
            # Check for missing parent cloud in flattened data
            parent_cloud_value = flattened_asset.get('parent_cloud')
            if parent_cloud_value is None or (isinstance(parent_cloud_value, str) and parent_cloud_value.strip() in ['', 'None', 'null']):
                missing_parent_cloud += 1
            
            flattened_assets.append(flattened_asset)
        
        return flattened_assets, missing_attribution, missing_properties, missing_name, missing_parent_cloud
