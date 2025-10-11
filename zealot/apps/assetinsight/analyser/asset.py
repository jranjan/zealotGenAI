"""
Asset Analyser - Abstract base class for asset analysis.

This module provides a clean abstract base class that defines the interface
for different types of asset analysers. Business logic is kept in concrete
implementations, not in the base class.
"""

from typing import Any, Dict, List
from abc import ABC, abstractmethod


class AssetAnalyser(ABC):
    """
    Abstract base class for asset analysers.
    
    This class defines the minimal interface for asset analysers.
    All business logic should be implemented in concrete subclasses.
    """
    
    def __init__(self, analyser_type: str):
        """
        Initialize the asset analyser.
        
        Args:
            analyser_type: Type of analyser (e.g., 'owner', 'security', 'network')
        """
        self.analyser_type = analyser_type
        self.reader = None
    
    @abstractmethod
    def analyse_with_config(self, config, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze assets using configuration.
        
        Args:
            config: Configuration object
            source_directory: Path to source directory
            result_directory: Path to result directory
            
        Returns:
            Analysis results dictionary
        """
        pass
    
    @abstractmethod
    def get_asset_fields(self) -> List[str]:
        """
        Get the list of asset fields specific to this analyser type.
        
        Returns:
            List of asset field names to extract
        """
        pass
    
    @abstractmethod
    def get_cloud_fields(self) -> List[str]:
        """
        Get the list of cloud fields specific to this analyser type.
        
        Returns:
            List of cloud field names to extract
        """
        pass
    
    @abstractmethod
    def process_asset_specific_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset-specific data based on analyser type.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Processed asset data with analyser-specific fields
        """
        pass
    
    def create_reader(self, source_directory: str) -> None:
        """
        Create a reader for the specified source directory.
        
        Args:
            source_directory: Path to source directory containing asset files
        """
        from database.reader.duckdb import DuckDBReader
        self.reader = DuckDBReader.get_instance(source_directory)
    
    def close_reader(self) -> None:
        """Close the reader and clean up resources."""
        if self.reader:
            self.reader.close()
            self.reader = None