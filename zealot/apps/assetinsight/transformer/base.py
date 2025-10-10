"""
Transformer Base Class - Abstract base class for data transformation operations
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pathlib import Path


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
