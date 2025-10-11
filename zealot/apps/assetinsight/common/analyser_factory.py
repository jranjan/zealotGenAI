"""
Analyser Factory - Common factory for creating analysers
"""

from typing import Dict, Type, Any
import sys
from pathlib import Path

# Add the parent directory to the path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


class AnalyserFactory:
    """Factory class for creating analysers based on type."""
    
    _analyser_types = ["owner", "security", "network"]
    
    @classmethod
    def create_analyser(cls, analyser_type: str) -> Any:
        """
        Create an analyser instance based on type.
        
        Args:
            analyser_type: Type of analyser to create
            
        Returns:
            Analyser instance
            
        Raises:
            ValueError: If analyser_type is not supported
        """
        if analyser_type not in cls._analyser_types:
            raise ValueError(f"Unknown analyser type: {analyser_type}. Must be one of: {cls._analyser_types}")
        
        # Dynamic import to avoid circular import issues
        if analyser_type == "owner":
            from analyser.owner import OwnerAnalyser
            return OwnerAnalyser()
        elif analyser_type == "security":
            from analyser.security import SecurityAnalyser
            return SecurityAnalyser()
        elif analyser_type == "network":
            from analyser.network import NetworkAnalyser
            return NetworkAnalyser()
        else:
            raise ValueError(f"Unknown analyser type: {analyser_type}")
    
    @classmethod
    def get_available_types(cls) -> list[str]:
        """Get list of available analyser types."""
        return cls._analyser_types.copy()
    
    @classmethod
    def is_valid_type(cls, analyser_type: str) -> bool:
        """Check if analyser type is valid."""
        return analyser_type in cls._analyser_types
