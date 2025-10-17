"""
Factory for creating singleton readers
"""

from .memory.basic import BasicMemoryDuckdb
from .memory.sonic import SonicMemoryDuckdb
from typing import Dict, Any, Optional
from pathlib import Path


class DatabaseFactory:
    """Simple factory for creating database readers"""
    
    @staticmethod
    def create_basic_reader(folder_path: str) -> BasicMemoryDuckdb:
        """Create BasicMemoryDuckdb reader"""
        return BasicMemoryDuckdb.get_instance(folder_path)
    
    @staticmethod
    def create_sonic_reader(folder_path: str, max_workers: Optional[int] = None, 
                           batch_size: int = 1000, memory_limit_gb: float = 2.0) -> SonicMemoryDuckdb:
        """Create SonicMemoryDuckdb reader with multiprocessing support"""
        return SonicMemoryDuckdb(folder_path, max_workers, batch_size, memory_limit_gb)
