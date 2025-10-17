"""
Simple Analyser Factory
"""

from enum import Enum
from .asset.owner import OwnerAnalyser
from .schema.schema import SchemaAnalyser


class AnalyserType(Enum):
    """Enumeration of available analyser types"""
    OWNER = "owner"
    SCHEMA = "schema"


class AnalyserFactory:
    """Simple factory for creating analysers"""
    
    @staticmethod
    def create_analyser(analyser_type: AnalyserType):
        """Create analyser by type"""
        if analyser_type == AnalyserType.OWNER:
            return OwnerAnalyser()
        elif analyser_type == AnalyserType.SCHEMA:
            return SchemaAnalyser()
        else:
            raise ValueError(f"Unsupported analyser type: {analyser_type}")
