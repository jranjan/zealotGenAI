"""
Factory for creating singleton readers
"""

from .duckdb import DuckDBReader
from .duckdb_sonic import DuckDBSonicReader
from typing import Dict, Any, Optional
from pathlib import Path


class ReaderFactory:
    """Factory for creating singleton readers"""
    
    @staticmethod
    def create_reader(folder_path: str, config_path: str = None, asset_name: str = None) -> Dict[str, Any]:
        """
        Create reader and return result-based response.
        Always returns a dictionary with success/error information.
        """
        try:
            reader = DuckDBReader.get_instance(folder_path)
            return reader.check_data_readiness()
        except Exception as e:
            # Return error result instead of raising exception
            source_path = Path(folder_path)
            return {
                'ready': False,
                'error': f"Failed to create reader: {str(e)}",
                'health_status': 'ERROR',
                'object_count': 0,
                'table_count': 0,
                'database_connected': False,
                'health_queries': [],
                'source_directory': str(source_path.resolve()) if source_path.exists() else folder_path,
                'json_files_found': len(list(source_path.glob("*.json"))) if source_path.exists() else 0
            }
    
    @staticmethod
    def create_sonic_reader(folder_path: str, max_workers: Optional[int] = None, 
                           batch_size: int = 1000, memory_limit_gb: float = 2.0) -> Dict[str, Any]:
        """
        Create DuckDBSonicReader with multiprocessing support.
        
        Args:
            folder_path: Path to folder containing JSON files
            max_workers: Maximum number of worker processes
            batch_size: Number of assets to process in each batch
            memory_limit_gb: Memory limit in GB before switching to streaming mode
            
        Returns:
            Dictionary with success/error information and performance stats
        """
        try:
            # DuckDBSonicReader extends DuckDBReader and inherits singleton pattern
            reader = DuckDBSonicReader(folder_path, max_workers, batch_size, memory_limit_gb)
            readiness_result = reader.check_data_readiness()
            
            # Add performance stats to the result
            if readiness_result.get('ready', False):
                performance_stats = reader.get_performance_stats()
                readiness_result.update(performance_stats)
            
            return readiness_result
            
        except Exception as e:
            # Return error result instead of raising exception
            source_path = Path(folder_path)
            return {
                'ready': False,
                'error': f"Failed to create sonic reader: {str(e)}",
                'health_status': 'ERROR',
                'object_count': 0,
                'table_count': 0,
                'database_connected': False,
                'health_queries': [],
                'source_directory': str(source_path.resolve()) if source_path.exists() else folder_path,
                'json_files_found': len(list(source_path.glob("*.json"))) if source_path.exists() else 0,
                'max_workers': max_workers or 0,
                'file_chunks': 0,
                'files_per_chunk': 0,
                'total_files': 0
            }
