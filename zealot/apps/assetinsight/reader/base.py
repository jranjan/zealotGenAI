"""
Reader Base Class - Abstract base class for data reading operations
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List


class Reader(ABC):
    """
    Abstract base class for data reading and inspection operations.
    
    This class defines the interface for different types of data reading
    and validation operations. Subclasses must implement the specific
    reading logic.
    """
    
    def __init__(self):
        """Initialize the scanner."""
        pass
    
    @abstractmethod
    def scan_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Scan a directory and return inspection results.
        
        Args:
            directory_path: Path to the directory to scan
            
        Returns:
            Dictionary with scanning results
        """
        pass
    
    @abstractmethod
    def get_directory_info(self, directory_path: str) -> Dict[str, Any]:
        """
        Get basic directory information without full scanning.
        
        Args:
            directory_path: Path to the directory
            
        Returns:
            Dictionary with basic directory information
        """
        pass
    
    def validate_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Validate if the given path is a valid and existing directory.
        
        Args:
            directory_path: Path to the directory to validate
            
        Returns:
            Dictionary with 'valid' status and 'error' message if invalid
        """
        if not directory_path:
            return {'valid': False, 'error': 'Please provide a directory path'}
        
        path = Path(directory_path)
        if not path.exists():
            return {'valid': False, 'error': f"Directory not found: {directory_path}"}
        
        if not path.is_dir():
            return {'valid': False, 'error': f"Path is not a directory: {directory_path}"}
        
        return {'valid': True}
    
    def count_files(self, directory_path: str, pattern: str = "*") -> int:
        """
        Count files matching a pattern in a directory.
        
        Args:
            directory_path: Path to the directory
            pattern: File pattern to match (e.g., "*.json")
            
        Returns:
            Number of matching files
        """
        path = Path(directory_path)
        if not path.is_dir():
            return 0
        return len(list(path.glob(pattern)))
