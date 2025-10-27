"""
Simple Reader Factory
"""

from enum import Enum
from .inventory.basic import BasicReader
from .inventory.sonic import SonicReader


class ReaderType(Enum):
    """Enumeration of available reader types"""
    BASIC_READER = "basic_reader"
    SONIC_READER = "sonic_reader"


class ReaderFactory:
    """Simple factory for creating readers"""
    
    @staticmethod
    def create_reader(reader_type: ReaderType, max_workers: int = None, chunk_size: int = 50):
        """Create reader by type"""
        if reader_type == ReaderType.BASIC_READER:
            return BasicReader()
        elif reader_type == ReaderType.SONIC_READER:
            return SonicReader(max_workers=max_workers, chunk_size=chunk_size)
        else:
            raise ValueError(f"Unsupported reader type: {reader_type}")
