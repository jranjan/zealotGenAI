"""
Schema Analyser - Database schema analysis and metadata exploration.
"""

from typing import Any, Dict, List, Optional
from ..base import Analyser


class SchemaAnalyser(Analyser):
    """
    Schema analyser for database metadata exploration.
    """
    
    def __init__(self):
        super().__init__("schema")
    
    def analyse(self, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze database schema and metadata using singleton pattern.
        
        Args:
            source_directory: Path to source directory containing database
            result_directory: Path to result directory (not used for schema analysis)
            
        Returns:
            Schema analysis results dictionary
        """
        try:
            # Use the base class method to create reader
            self.create_reader(source_directory)
            
            # Get list of tables
            tables_result = self.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            tables = [row['table_name'] for row in tables_result] if tables_result else []
            
            # Get table metadata for each table
            table_metadata = {}
            for table_name in tables:
                metadata_query = f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        ordinal_position
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'main'
                    ORDER BY ordinal_position
                """
                
                metadata_result = self.execute_query(metadata_query)
                table_metadata[table_name] = metadata_result if metadata_result else []
            
            return {
                'success': True,
                'tables': tables,
                'table_metadata': table_metadata,
                'total_tables': len(tables),
                'total_columns': sum(len(columns) for columns in table_metadata.values())
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'tables': [],
                'table_metadata': {},
                'total_tables': 0,
                'total_columns': 0
            }
        finally:
            self.close_reader()
    
    
    def get_table_list(self, source_directory: str) -> List[str]:
        """
        Get list of tables in the database using singleton pattern.
        
        Args:
            source_directory: Path to source directory containing database
            
        Returns:
            List of table names
        """
        try:
            # Use factory to create reader
            from database.duckdb import DatabaseFactory
            reader = DatabaseFactory.create_basic_reader(source_directory)
            tables_result = reader.execute_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            tables = [row['table_name'] for row in tables_result] if tables_result else []
            
            return tables
            
        except Exception as e:
            print(f"Error getting table list: {e}")
            return []
    
    def get_table_metadata(self, source_directory: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Get metadata for a specific table using singleton pattern.
        
        Args:
            source_directory: Path to source directory containing database
            table_name: Name of the table to get metadata for
            
        Returns:
            List of column metadata dictionaries
        """
        try:
            # Use factory to create reader
            from database.duckdb import DatabaseFactory
            reader = DatabaseFactory.create_basic_reader(source_directory)
            
            metadata_query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' 
                AND table_schema = 'main'
                ORDER BY ordinal_position
            """
            
            metadata_result = reader.execute_query(metadata_query)
            
            return metadata_result if metadata_result else []
            
        except Exception as e:
            print(f"Error getting table metadata for {table_name}: {e}")
            return []
    
    def get_sample_data(self, source_directory: str, table_name: str, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Get sample data from a specific table.
        
        Args:
            source_directory: Path to source directory containing database
            table_name: Name of the table to get sample data from
            limit: Number of sample records to return (default: 1)
            
        Returns:
            List of sample records
        """
        try:
            # Use factory to create reader
            from database.duckdb import DatabaseFactory
            reader = DatabaseFactory.create_basic_reader(source_directory)
            
            sample_query = f"SELECT * FROM \"{table_name}\" LIMIT {limit}"
            sample_data = reader.execute_query(sample_query)
            
            return sample_data if sample_data else []
            
        except Exception as e:
            print(f"Error getting sample data for {table_name}: {e}")
            return []
