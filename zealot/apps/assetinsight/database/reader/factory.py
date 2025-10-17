"""
Factory for creating singleton readers
"""

from .duckdb import DuckDBMemoryReader
from .duckdb_sonic import DuckDBSonicMemoryReader
from typing import Union
from enum import Enum


class ReaderType(Enum):
    """Reader types"""
    MEMORY = "memory"
    MEMORY_SONIC = "memory_sonic"


class ReaderFactory:
    """Factory for creating singleton readers"""
    
    @staticmethod
    def create_reader(folder_path: str, reader_type: ReaderType) -> Union[DuckDBMemoryReader, DuckDBSonicMemoryReader]:
        """Create reader instance based on type"""
        print(f"🏭 Factory: Creating reader type {reader_type.value} for folder {folder_path}")
        try:
            if reader_type == ReaderType.MEMORY:
                return DuckDBMemoryReader(folder_path=folder_path)
            else:  # Default to MEMORY_SONIC
                return DuckDBSonicMemoryReader(folder_path=folder_path)
        except Exception as e:
            # Fallback to basic memory reader if sonic fails
            print(f"⚠️ Factory: Sonic reader failed, falling back to basic reader: {e}")
            return DuckDBMemoryReader(folder_path=folder_path)