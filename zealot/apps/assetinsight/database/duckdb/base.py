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
            # Get schema from assets.yaml
            table_schema = self._get_schema()
            
            # Build CREATE TABLE statement from schema
            columns = []
            for col in table_schema['columns']:
                col_name = col['column_name']
                col_type = col['data_type']
                columns.append(f"{col_name} {col_type}")
            
            create_sql = f"CREATE TABLE IF NOT EXISTS assets ({', '.join(columns)})"
            print(f"📋 Creating table with schema from assets.yaml...")
            conn.execute(create_sql)
            print(f"✅ Table ensured with {len(columns)} columns")
            
        except Exception as e:
            print(f"⚠️ Error creating table from schema: {e}")
            raise e
    
    def _create_all_tables(self, conn):
        """Create all tables defined in the schema configuration"""
        try:
            # Get all table schemas from assets.yaml
            all_schemas = self._get_all_schemas()
            
            if not all_schemas:
                # Fallback to single assets table if no multi-table schema
                self._create_assets_table(conn)
                return
            
            # Create each table defined in the schema
            for table_name, table_schema in all_schemas.items():
                columns = []
                for col in table_schema['columns']:
                    col_name = col['column_name']
                    col_type = col['data_type']
                    columns.append(f"{col_name} {col_type}")
                
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
                print(f"📋 Creating table '{table_name}' with {len(columns)} columns...")
                conn.execute(create_sql)
                print(f"✅ Table '{table_name}' ensured")
            
        except Exception as e:
            print(f"⚠️ Error creating tables from schema: {e}")
            raise e
    
    def _get_all_schemas(self):
        """Get all table schemas from SchemaGuide"""
        try:
            schema_guide = SchemaGuide()
            return schema_guide.get_all_table_schemas()
        except Exception as e:
            print(f"⚠️ Error loading all schemas: {e}")
            return {}
    
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
