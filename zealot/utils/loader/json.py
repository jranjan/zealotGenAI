"""
Simple JSON Loader - Load and query JSON files from a directory.

This module provides a lightweight JSON file loader that can load all JSON files
from a specified directory and provide simple query functionality using dot notation
for accessing nested attributes.

Example:
    loader = JSONLoader("path/to/json/files")
    data = loader.get_file_data("config")
    value = loader.get_attribute("config", "database.host")
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class JSONLoader:
    """
    Simple JSON file loader that reads files on-demand without caching.
    
    This class provides on-demand loading of JSON files from disk without
    keeping them in memory. Files are read fresh from disk on each access.
    
    Attributes:
        json_folder (Path): Path to the directory containing JSON files
        file_list (List[str]): List of available JSON filenames
    """
    
    def __init__(self, json_folder: str):
        """
        Initialize the JSON loader with a directory path.
        
        Args:
            json_folder (str): Path to the directory containing JSON files
            
        Raises:
            FileNotFoundError: If the specified directory does not exist
        """
        self.json_folder = Path(json_folder)
        self.file_list = []
        
        if not self.json_folder.exists():
            raise FileNotFoundError(f"Folder not found: {self.json_folder}")
        
        self._scan_files()
    
    def _scan_files(self) -> None:
        """
        Scan directory for JSON files without loading them into memory.
        
        This method only discovers available JSON files without loading their content.
        Files are read from disk on each access.
        """
        self.file_list = [f.stem for f in self.json_folder.glob("*.json")]
    
    def _load_file(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Load a specific JSON file from disk.
        
        Args:
            filename (str): Name of the file without extension
            
        Returns:
            Optional[Dict[str, Any]]: The JSON data or None if file not found/error
        """
        file_path = self.json_folder / f"{filename}.json"
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return None
    
    def get_file_data(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Get the complete JSON data from a specific file (read from disk).
        
        Args:
            filename (str): Name of the file without extension (e.g., 'config' for 'config.json')
            
        Returns:
            Optional[Dict[str, Any]]: The JSON data as a dictionary, or None if file not found
        """
        return self._load_file(filename)
    
    def get_attribute(self, filename: str, path: str, default: Any = None) -> Any:
        """
        Get a nested attribute from a JSON file using dot notation.
        
        This method allows you to access deeply nested values in JSON data using
        a dot-separated path. It safely handles missing keys by returning the default value.
        
        Args:
            filename (str): Name of the file without extension
            path (str): Dot-separated path to the attribute (e.g., 'user.profile.name')
            default (Any, optional): Default value to return if path not found. Defaults to None.
            
        Returns:
            Any: The value at the specified path, or default if not found
        """
        data = self.get_file_data(filename)
        if not data:
            return default
        
        current = data
        for key in path.split('.'):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current
    
    def list_files(self) -> List[str]:
        """
        Get a list of all available JSON filenames.
        
        Returns:
            List[str]: List of filenames (without .json extension) available in the directory
        """
        return self.file_list.copy()
