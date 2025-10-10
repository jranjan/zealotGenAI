"""
Transformer Factory - Factory pattern for creating transformer instances
"""

from enum import Enum
from typing import Dict, Any, Optional
from .base import Transformer
from .flattener import Flattener
from .sonicflattener import SonicFlattener
from .supersonicflattener import SupersonicFlattener


class TransformerType(Enum):
    """Enumeration of available transformer types"""
    FLATTENER = "flattener"
    SONIC_FLATTENER = "sonic_flattener"
    SUPERSONIC_FLATTENER = "supersonic_flattener"


class TransformerFactory:
    """Factory class for creating transformer instances"""
    
    @staticmethod
    def create_transformer(transformer_type: TransformerType, **kwargs) -> Transformer:
        """
        Create a transformer instance based on the specified type.
        
        Args:
            transformer_type: Type of transformer to create
            **kwargs: Additional arguments for transformer initialization
            
        Returns:
            Transformer instance
            
        Raises:
            ValueError: If transformer_type is not supported
        """
        if transformer_type == TransformerType.FLATTENER:
            return Flattener()
        elif transformer_type == TransformerType.SONIC_FLATTENER:
            max_workers = kwargs.get('max_workers', None)
            return SonicFlattener(max_workers=max_workers)
        elif transformer_type == TransformerType.SUPERSONIC_FLATTENER:
            max_workers = kwargs.get('max_workers', None)
            chunk_size = kwargs.get('chunk_size', 100)
            return SupersonicFlattener(max_workers=max_workers, chunk_size=chunk_size)
        else:
            raise ValueError(f"Unsupported transformer type: {transformer_type}")
    
    @staticmethod
    def get_available_types() -> Dict[str, str]:
        """
        Get available transformer types and their descriptions.
        
        Returns:
            Dictionary mapping type names to descriptions
        """
        return {
            TransformerType.FLATTENER.value: "Standard single-threaded flattener",
            TransformerType.SONIC_FLATTENER.value: "High-performance multithreaded flattener",
            TransformerType.SUPERSONIC_FLATTENER.value: "Supersonic-high-performance multiprocessing flattener"
        }
