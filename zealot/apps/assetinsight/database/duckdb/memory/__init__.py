"""
Memory package for database readers
"""

from .basic import BasicMemoryDuckdb
from .sonic import SonicMemoryDuckdb

__all__ = [
    'BasicMemoryDuckdb',
    'SonicMemoryDuckdb'
]
