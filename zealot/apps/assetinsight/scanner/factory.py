"""
Simple Scanner Factory
"""

from .basic import BasicScanner
from .sonic import SonicScanner


class ScannerFactory:
    """Simple factory for creating scanners"""
    
    @staticmethod
    def create_basic_scanner() -> BasicScanner:
        """Create BasicScanner"""
        return BasicScanner()
    
    @staticmethod
    def create_sonic_scanner(max_workers: int = None, chunk_size: int = 50) -> SonicScanner:
        """Create SonicScanner with multiprocessing support"""
        return SonicScanner(max_workers=max_workers, chunk_size=chunk_size)
