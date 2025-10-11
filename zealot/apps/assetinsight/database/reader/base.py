"""
Minimal Reader interface for SQL operations
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pathlib import Path


class Reader(ABC):
    """Minimal SQL reader interface"""
    
    def __init__(self, folder_path: str):
        self.folder_path = Path(folder_path)
        if not self.folder_path.exists() or not self.folder_path.is_dir():
            raise ValueError(f"Folder does not exist: {folder_path}")
    
    @abstractmethod
    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection"""
        pass
    
    @abstractmethod
    def check_data_readiness(self) -> Dict[str, Any]:
        """
        Check data readiness by querying database health and getting object count.
        
        Returns:
            Dictionary containing readiness status, health info, and object count
        """
        pass
