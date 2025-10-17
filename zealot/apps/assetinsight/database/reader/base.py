"""
Minimal Reader interface for SQL operations
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pathlib import Path
import json
from configreader import SchemaGuide


class Reader(ABC):
    """Minimal SQL reader interface with common schema operations"""
    
    def __init__(self, folder_path: str):
        self.folder_path = Path(folder_path)
        if not self.folder_path.exists() or not self.folder_path.is_dir():
            raise ValueError(f"Folder does not exist: {folder_path}")
    
    def _get_schema(self):
        """Get schema from assets.yaml - common implementation"""
        try:
            schema_guide = SchemaGuide()
            table_schema = schema_guide.get_assets_table_schema()  # Uses default path
            
            if not table_schema or 'columns' not in table_schema:
                raise Exception("Schema loading failed")
            
            return table_schema
        except Exception as e:
            print(f"⚠️ Error loading schema: {e}")
            raise e
    
    def _create_assets_table(self, conn):
        """Create assets table using SchemaGuide - common implementation"""
        try:
            # Check if table already exists first using a more reliable method
            try:
                # Try to query the table - if it exists, this will succeed
                conn.execute("SELECT 1 FROM assets LIMIT 1").fetchone()
                # If we get here, table exists, no need to create it
                return
            except:
                # If query fails, table doesn't exist, proceed with creation
                pass
            
            # Get schema from assets.yaml
            table_schema = self._get_schema()
            
            # Build CREATE TABLE statement from schema
            columns = []
            for col in table_schema['columns']:
                col_name = col['column_name']
                col_type = col['data_type']
                columns.append(f"{col_name} {col_type}")
            
            create_sql = f"CREATE TABLE assets ({', '.join(columns)})"
            print(f"📋 Creating table with schema from assets.yaml...")
            conn.execute(create_sql)
            print(f"✅ Table created with {len(columns)} columns")
            
        except Exception as e:
            print(f"⚠️ Error creating table from schema: {e}")
            raise e
    
    def _reconstruct_nested_json(self, asset: dict, prefix: str) -> str:
        """
        Reconstruct nested JSON object from flattened fields with given prefix.
        
        Args:
            asset: Asset dictionary with flattened fields
            prefix: Prefix to look for (e.g., 'properties_', 'tags_')
            
        Returns:
            JSON string of the reconstructed nested object
        """
        nested_obj = {}
        
        # Find all fields that start with the prefix
        for key, value in asset.items():
            if key.startswith(prefix):
                # Remove the prefix to get the original key
                original_key = key[len(prefix):]
                # Only add non-null values
                if value is not None and value != '':
                    nested_obj[original_key] = value
        
        return json.dumps(nested_obj)
    
    def _create_data_tuples(self, assets: List[Dict[str, Any]]) -> List[tuple]:
        """Create data tuples dynamically based on schema - common implementation"""
        try:
            table_schema = self._get_schema()
            columns = table_schema['columns']
            
            data_tuples = []
            for asset in assets:
                values = []
                for col in columns:
                    col_name = col['column_name']
                    col_type = col['data_type']
                    
                    if col_type == 'JSON':
                        if col_name == 'properties':
                            value = self._reconstruct_nested_json(asset, 'properties_')
                        elif col_name == 'tags':
                            value = self._reconstruct_nested_json(asset, 'tags_')
                        elif col_name == 'raw_data':
                            value = json.dumps(asset)
                        else:
                            value = json.dumps(asset.get(col_name, {}))
                    else:
                        # VARCHAR fields - use None for missing values to get NULL in database
                        value = asset.get(col_name, None)
                    
                    values.append(value)
                
                data_tuples.append(tuple(values))
            
            return data_tuples
        except Exception as e:
            print(f"⚠️ Error creating data tuples: {e}")
            return []
    
    def _get_insert_sql(self) -> str:
        """Get dynamic INSERT SQL statement based on schema"""
        try:
            table_schema = self._get_schema()
            column_names = [col['column_name'] for col in table_schema['columns']]
            placeholders = ', '.join(['?' for _ in column_names])
            return f"INSERT INTO assets ({', '.join(column_names)}) VALUES ({placeholders})"
        except Exception as e:
            print(f"⚠️ Error getting insert SQL: {e}")
            raise e
    
    def _get_database_connection(self):
        """Get a new database connection for queries"""
        # This should be implemented by subclasses
        raise NotImplementedError("Subclasses must implement _get_database_connection")
    
    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results - common implementation"""
        conn = self._get_database_connection()
        try:
            result = conn.execute(sql_query).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in result]
        finally:
            # Only close connection if it's not the main in-memory connection
            if hasattr(self, 'db_path') and self.db_path != ":memory:":
                conn.close()
    
    def get_total_objects(self) -> int:
        """Get total number of assets in the database - common implementation"""
        conn = self._get_database_connection()
        try:
            result = conn.execute("SELECT COUNT(*) as count FROM assets").fetchone()
            return result[0] if result else 0
        finally:
            # Only close connection if it's not the main in-memory connection
            if hasattr(self, 'db_path') and self.db_path != ":memory:":
                conn.close()
    
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
    
    @abstractmethod
    def load_data_from_folder(self, data_folder: Path) -> None:
        """
        Load data from a folder into the database
        
        Args:
            data_folder: Path to folder containing data files
        """
        pass
    
    def create_tables(self) -> Dict[str, Any]:
        """
        Create database tables (schema only) - common implementation for all readers
        
        Returns:
            Dictionary with success status and message
        """
        try:
            if self.conn is None:
                return {
                    'success': False,
                    'message': "Database connection not established",
                    'database_ready': False
                }
            
            # Create tables using schema from config
            self._create_assets_table(self.conn)
            
            return {
                'success': True,
                'message': 'Database tables created successfully!',
                'database_ready': True
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f"Table creation failed: {str(e)}",
                'database_ready': False
            }
    
    def load_data_into_tables(self, target_folder: str = None) -> Dict[str, Any]:
        """
        Load data into existing tables - common implementation for all readers
        
        Args:
            target_folder: Path to folder containing data files
            
        Returns:
            Dictionary with success status, message, and stats
        """
        try:
            data_folder = Path(target_folder) if target_folder else self.folder_path
            if not data_folder.exists():
                return {
                    'success': False,
                    'message': f"Target folder does not exist: {data_folder}",
                    'stats': {},
                    'database_ready': False
                }
            
            # Load data from folder
            self.load_data_from_folder(data_folder)
            
            # Get performance stats
            performance_stats = self.get_performance_stats()
            
            # Return standardized result
            result = {
                'success': True,
                'message': 'Data loaded successfully!',
                'stats': {
                    'total_assets': performance_stats.get('total_assets', 0),
                    'total_files': performance_stats.get('total_files', 0),
                    'health_status': performance_stats.get('health_status', 'UNKNOWN'),
                    'table_count': performance_stats.get('table_count', 0),
                    'max_workers': performance_stats.get('max_workers', 0),
                    'file_chunks': performance_stats.get('file_chunks', 0),
                    'files_per_chunk': performance_stats.get('files_per_chunk', 0),
                    'processing_time': performance_stats.get('processing_time', 0)
                },
                'database_ready': True
            }
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'message': f"Data loading failed: {str(e)}",
                'stats': {},
                'database_ready': False
            }
    
    def update_session_state(self, target_folder: str = None) -> None:
        """
        Update Streamlit session state with database information
        
        Args:
            target_folder: Optional target folder path to store in session state
        """
        try:
            import streamlit as st
            
            # Get performance stats
            performance_stats = self.get_performance_stats()
            
            # Update session state
            st.session_state['database_ready'] = True
            st.session_state['database_stats'] = {
                'total_assets': performance_stats.get('total_assets', 0),
                'total_files': performance_stats.get('total_files', 0),
                'health_status': performance_stats.get('health_status', 'UNKNOWN'),
                'table_count': performance_stats.get('table_count', 0),
                'max_workers': performance_stats.get('max_workers', 0),
                'file_chunks': performance_stats.get('file_chunks', 0),
                'files_per_chunk': performance_stats.get('files_per_chunk', 0),
                'processing_time': performance_stats.get('processing_time', 0)
            }
            
            if target_folder:
                st.session_state['database_path'] = target_folder
            else:
                st.session_state['database_path'] = str(self.folder_path)
                
        except ImportError:
            # Streamlit not available, skip session state update
            pass
        except Exception as e:
            # Log error but don't fail the operation
            print(f"Warning: Failed to update session state: {e}")
    
    def setup_database_complete(self, target_folder: str = None) -> Dict[str, Any]:
        """
        Complete database setup with data loading and session state update
        
        Args:
            target_folder: Optional target folder to load data from. If None, uses self.folder_path
            
        Returns:
            Dictionary with success status, message, and performance stats
        """
        # Load data into existing tables
        result = self.load_data_into_tables(target_folder)
        
        # If successful, update session state
        if result['success']:
            self.update_session_state(target_folder)
        
        return result
