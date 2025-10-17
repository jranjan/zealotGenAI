"""
Factory for creating database readers
"""

from enum import Enum
from .memory.basic import BasicMemoryDuckdb
from .memory.sonic import SonicMemoryDuckdb
from typing import Optional


class DatabaseType(Enum):
    """Enumeration of available database types"""
    BASIC = "basic"
    SONIC = "sonic"


class DatabaseFactory:
    """Simple factory for creating database readers"""
    
    @staticmethod
    def create_reader(db_type: DatabaseType, folder_path: str, max_workers: Optional[int] = None, 
                     batch_size: int = 1000, memory_limit_gb: float = 2.0):
        """Create database reader by type"""
        if db_type == DatabaseType.BASIC:
            return BasicMemoryDuckdb.get_instance(folder_path)
        elif db_type == DatabaseType.SONIC:
            return SonicMemoryDuckdb(folder_path, max_workers, batch_size, memory_limit_gb)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
