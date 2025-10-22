"""
Analyser Base Class - Minimal database connectivity and query execution.

This module provides the bare minimum functionality for database connectivity
and query execution that all analysers need.
"""

from typing import Any, Dict, List
from abc import ABC, abstractmethod


class Analyser(ABC):
    """
    Abstract base class for all analysers.
    
    Provides minimal database connectivity and query execution functionality.
    """
    
    def __init__(self, analyser_type: str):
        """Initialize the analyser."""
        self.analyser_type = analyser_type
        self.reader = None
    
    def create_reader(self, source_directory: str) -> None:
        """Create a database reader for the specified source directory."""
        from database.duckdb import DatabaseFactory, DatabaseType
        
        # Use factory to create reader
        self.reader = DatabaseFactory.create_reader(DatabaseType.SONIC, source_directory)
    
    def close_reader(self) -> None:
        """Close the reader and clean up resources."""
        if self.reader:
            self.reader.close()
            self.reader = None
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query on the database."""
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        return self.reader.execute_query(query)
    
    @abstractmethod
    def analyse(self, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """Analyze assets."""
        pass
    
