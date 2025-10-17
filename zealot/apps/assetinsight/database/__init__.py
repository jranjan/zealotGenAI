"""
Database package for efficient data access and processing
"""

from .duckdb import DatabaseFactory, DatabaseType

__all__ = [
    'DatabaseFactory',
    'DatabaseType'
]