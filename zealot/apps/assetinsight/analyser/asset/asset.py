"""
Asset Analyser - Abstract base class for asset-specific analysis.

This module provides an abstract base class that extends the base Analyser
class with asset-specific functionality. It focuses on asset data processing
and analysis patterns.
"""

from typing import Any, Dict, List
from abc import ABC, abstractmethod
from ..base import Analyser


class AssetAnalyser(Analyser):
    """
    Abstract base class for asset-specific analysers.
    
    This class extends the base Analyser class with asset-specific functionality.
    It provides common patterns for asset data processing and analysis.
    """
    
    def __init__(self, analyser_type: str):
        """
        Initialize the asset analyser.
        
        Args:
            analyser_type: Type of analyser (e.g., 'owner', 'security', 'network')
        """
        super().__init__(analyser_type)
    
    @abstractmethod
    def get_cloud_fields(self) -> List[str]:
        """
        Get the list of cloud fields specific to this analyser type.
        
        Returns:
            List of cloud field names to extract
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
    def process_asset_specific_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset-specific data based on analyser type.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Processed asset data with analyser-specific fields
        """
        pass