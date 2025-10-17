"""
Simple Transformer Factory
"""

from enum import Enum
from .flattener.basic import BasicFlattener
from .flattener.sonic import SonicFlattener


class TransformerType(Enum):
    """Enumeration of available transformer types"""
    BASIC_NORMALISER = "basic_normaliser"
    SONIC_NORMALISER = "sonic_normaliser"


class TransformerFactory:
    """Simple factory for creating transformers"""
    
    @staticmethod
    def create_transformer(transformer_type: TransformerType, max_workers: int = None):
        """Create transformer by type"""
        if transformer_type == TransformerType.BASIC_NORMALISER:
            return BasicFlattener()
        elif transformer_type == TransformerType.SONIC_NORMALISER:
            return SonicFlattener(max_workers=max_workers)
        else:
            raise ValueError(f"Unsupported transformer type: {transformer_type}")
