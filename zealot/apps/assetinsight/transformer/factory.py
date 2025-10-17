"""
Simple Transformer Factory
"""

from .flattener.basic import BasicFlattener
from .flattener.sonic import SonicFlattener


class TransformerFactory:
    """Simple factory for creating transformers"""
    
    @staticmethod
    def create_basic_flattener() -> BasicFlattener:
        """Create BasicFlattener"""
        return BasicFlattener()
    
    @staticmethod
    def create_sonic_flattener(max_workers: int = None) -> SonicFlattener:
        """Create SonicFlattener with multiprocessing support"""
        return SonicFlattener(max_workers=max_workers)
