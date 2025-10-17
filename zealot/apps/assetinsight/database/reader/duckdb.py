"""
Minimal DuckDB Reader - Singleton Pattern
"""

import json
import duckdb
from pathlib import Path
from typing import Dict, Any, List
import os
from .base import Reader
from configreader import SchemaGuide


class DuckDBMemoryReader(Reader):
    """Singleton DuckDB reader"""
    
    _instance = None  # Class variable to store the singleton instance
    _initialized = False  # Track if singleton has been initialized
    
    def __new__(cls, folder_path: str = None):
        # Return existing singleton instance if it exists
        if cls._instance is not None:
            return cls._instance
        
        # Create new singleton instance
        instance = super(DuckDBMemoryReader, cls).__new__(cls)
        cls._instance = instance
        return instance
    
    def __init__(self, folder_path: str = None):
        # Only initialize once for singleton
        if not self._initialized:
            if folder_path is None:
                folder_path = "."
            super().__init__(folder_path)
            self.folder_path = Path(folder_path)
            self.db_path = None
            self.conn = None
            self._setup_database()
            self._initialized = True
    
    @classmethod
    def reset_singleton(cls):
        """Reset the singleton instance (useful for testing)"""
        cls._instance = None
        cls._initialized = False
    
    def _setup_database(self):
        """Setup DuckDB database connection only (no tables created initially)"""
        # Use in-memory database for maximum performance
        self.db_path = ":memory:"
        print(f"🗄️ Creating in-memory database connection")
        self.conn = duckdb.connect(self.db_path)
        print(f"✅ In-memory database connection established")
        
        # Database is created but no tables are initialized yet
        # Tables will be created when data is loaded via load_data() method
        print(f"✅ Database ready for table creation")
    
    # Schema methods now inherited from base Reader class
    
    def load_data_from_folder(self, data_folder: Path) -> None:
        """Load data from a folder into the database"""
        if self.conn is None:
            raise Exception("Database connection not established")
        
        print(f"🗄️ Loading data from folder: {data_folder}")
        self._load_json_files_from_folder(data_folder)
        print(f"✅ Data loaded successfully")
    
    def _load_json_files_from_folder(self, data_folder: Path) -> None:
        """Load JSON files into DuckDB from specified folder"""
        json_files = list(data_folder.glob("*.json"))
        if not json_files:
            print("⚠️ No JSON files found in directory")
            return
        
        print(f"🚀 Loading {len(json_files)} JSON files into DuckDB...")
        
        # Drop existing table to ensure clean schema
        self.conn.execute("DROP TABLE IF EXISTS assets")
        
        # Create table using SchemaGuide
        self._create_assets_table(self.conn)
        
        # Load files with progress tracking
        total_assets = 0
        for i, file_path in enumerate(json_files, 1):
            file_assets = self._load_single_file(file_path)
            total_assets += file_assets
            
            # Progress update
            progress = (i / len(json_files)) * 100
            print(f"📊 Progress: {progress:.1f}% ({i}/{len(json_files)}) - Loaded {file_assets} assets from {file_path.name}")
        
        print(f"✅ Load Complete: {total_assets:,} total assets loaded from {len(json_files)} files")
    
    def _load_single_file(self, file_path: Path) -> int:
        """Load a single JSON file into DuckDB and return asset count"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            return 0
        
        # Get schema for dynamic field extraction
        try:
            table_schema = self._get_schema()
        except Exception as e:
            print(f"⚠️ Could not load schema for data insertion: {e}")
            return 0
        
        # Get column names and types
        columns = table_schema['columns']
        column_names = [col['column_name'] for col in columns]
        
        asset_count = 0
        for asset in data:
            if not isinstance(asset, dict):
                continue
            
            # Extract values for all columns dynamically
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
            
            # Build dynamic INSERT statement
            placeholders = ', '.join(['?' for _ in column_names])
            insert_sql = f"INSERT INTO assets ({', '.join(column_names)}) VALUES ({placeholders})"
            
            # Insert into database
            self.conn.execute(insert_sql, values)
            
            asset_count += 1
        
        return asset_count
    
    def _get_database_connection(self):
        """Get a new database connection for queries"""
        # Always create a new connection for each query to ensure data consistency
        if not self.db_path:
            raise Exception("Database path not set")
        
        # For in-memory databases, we need to use the existing connection
        # as each new connection creates a separate in-memory database
        if self.db_path == ":memory:":
            if not self.conn:
                raise Exception("In-memory database connection not established")
            return self.conn
        
        # For file-based databases, create a new connection
        try:
            return duckdb.connect(str(self.db_path))
        except Exception as e:
            raise Exception(f"Failed to connect to database at {self.db_path}: {e}")
    
    def check_data_readiness(self) -> Dict[str, Any]:
        """
        Check data readiness by querying DuckDB database health and getting object count.
        This method never raises exceptions - all errors are returned in the result.
        
        Returns:
            Dictionary containing readiness status, health info, and object count
        """
        # Initialize result with default values
        result = {
            'ready': False,
            'health_status': 'ERROR',
            'object_count': 0,
            'table_count': 0,
            'database_connected': False,
            'health_queries': [],
            'source_directory': str(self.folder_path.resolve()),
            'json_files_found': len(list(self.folder_path.glob("*.json"))),
            'error': None
        }
        
        # Check if database connection exists
        if not hasattr(self, 'conn') or not self.conn:
            # For in-memory databases, we need the connection
            if self.db_path == ":memory:":
                result['error'] = "In-memory database connection not initialized"
                return result
        
        # Query database health
        health_status = 'HEALTHY'
        health_queries = []
        table_count = 0
        
        # Test basic connectivity
        health_queries.append("SELECT 1 as test_connection")
        connectivity_result = self._safe_execute_query("SELECT 1 as test_connection")
        print(f"🔍 DEBUG: Connectivity result: {connectivity_result}")
        if not connectivity_result['success']:
            health_status = 'ERROR'
            health_queries.append(f"ERROR: Connection test failed - {connectivity_result['error']}")
            result.update({
                'health_status': health_status,
                'health_queries': health_queries,
                'error': f"Database connection failed: {connectivity_result['error']}"
            })
            return result
        
        # Test table existence and count
        health_queries.append("SHOW TABLES")
        tables_result = self._safe_execute_query("SHOW TABLES")
        if not tables_result['success']:
            health_status = 'ERROR'
            health_queries.append(f"ERROR: Failed to list tables - {tables_result['error']}")
        else:
            tables = tables_result['data']
            table_count = len(tables) if tables else 0
            
            if table_count == 0:
                health_status = 'ERROR'
                health_queries.append("ERROR: No tables found in database")
            elif 'assets' not in [table.get('name', '') for table in tables]:
                health_status = 'WARNING'
                health_queries.append("WARNING: assets table not found")
        
        # Test table structure (only if we have tables)
        if health_status != 'ERROR' and table_count > 0:
            health_queries.append("DESCRIBE assets")
            schema_result = self._safe_execute_query("DESCRIBE assets")
            if not schema_result['success']:
                health_status = 'WARNING'
                health_queries.append(f"WARNING: Failed to describe assets table - {schema_result['error']}")
            else:
                schema = schema_result['data']
                # Get expected columns from schema
                try:
                    table_schema = self._get_schema()
                    expected_columns = [col['column_name'] for col in table_schema['columns']]
                except Exception:
                    # Fallback to basic columns if schema loading fails
                    expected_columns = ['id', 'name', 'assetClass']
                
                schema_columns = [col.get('column_name', '') for col in schema]
                missing_columns = [col for col in expected_columns if col not in schema_columns]
                if missing_columns:
                    health_status = 'WARNING'
                    health_queries.append(f"WARNING: Missing expected columns: {missing_columns}")
        
        # Query number of objects
        object_count = 0
        if health_status != 'ERROR':
            count_result = self._safe_get_total_objects()
            if not count_result['success']:
                health_status = 'ERROR'
                health_queries.append(f"ERROR: Failed to get object count - {count_result['error']}")
            else:
                object_count = count_result['data']
        
        # Query asset classes count
        asset_classes = 0
        if health_status != 'ERROR' and table_count > 0:
            asset_classes_result = self._safe_execute_query("SELECT COUNT(DISTINCT assetClass) as asset_classes FROM assets")
            if asset_classes_result['success'] and asset_classes_result['data']:
                asset_classes = asset_classes_result['data'][0].get('asset_classes', 0)
        
        # Determine overall readiness
        ready = health_status == 'HEALTHY' and object_count > 0
        
        # Update result with final values
        result.update({
            'ready': ready,
            'health_status': health_status,
            'object_count': object_count,
            'table_count': table_count,
            'asset_classes': asset_classes,
            'database_connected': True,
            'health_queries': health_queries,
            'error': None if ready else f"Data not ready: {health_status}, {object_count} objects"
        })
        
        return result
    
    def _safe_execute_query(self, sql_query: str) -> Dict[str, Any]:
        """Safely execute a SQL query without raising exceptions."""
        try:
            data = self.execute_query(sql_query)
            return {'success': True, 'data': data, 'error': None}
        except Exception as e:
            return {'success': False, 'data': None, 'error': str(e)}
    
    def _safe_get_total_objects(self) -> Dict[str, Any]:
        """Safely get total objects count without raising exceptions."""
        try:
            count = self.get_total_objects()
            return {'success': True, 'data': count, 'error': None}
        except Exception as e:
            return {'success': False, 'data': 0, 'error': str(e)}
    
    # _reconstruct_nested_json now inherited from base Reader class
    
    def close(self):
        """Close the database and clean up resources"""
        if self.conn:
            self.conn.close()
        # Don't try to delete in-memory database files
        if self.db_path and self.db_path != ":memory:" and os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    @classmethod
    def get_instance(cls, folder_path: str):
        """Get singleton instance for a specific folder path"""
        folder_path_str = str(Path(folder_path).resolve())
        if folder_path_str not in cls._instances:
            cls._instances[folder_path_str] = cls(folder_path)
        return cls._instances[folder_path_str]
    
    @classmethod
    def close_instance(cls, folder_path: str):
        """Close and remove instance for a specific folder path"""
        folder_path_str = str(Path(folder_path).resolve())
        if folder_path_str in cls._instances:
            instance = cls._instances[folder_path_str]
            instance.close()
            del cls._instances[folder_path_str]
            cls._initialized.discard(folder_path_str)
    
    @classmethod
    def close_all_instances(cls):
        """Close and remove all instances"""
        for instance in list(cls._instances.values()):
            instance.close()
        cls._instances.clear()
        cls._initialized.clear()
    
    @classmethod
    def get_all_instances(cls):
        """Get all active instances"""
        return dict(cls._instances)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics for the database reader.
        
        Returns:
            Dictionary containing performance statistics
        """
        # Create a new connection for this query to avoid connection issues
        if not self.db_path or not os.path.exists(self.db_path):
            print(f"⚠️ Database file not found: {self.db_path}")
            return {
                'status': 'not_connected',
                'total_files': 0,
                'total_assets': 0,
                'asset_classes': 0,
                'health_status': 'Unknown'
            }
        
        try:
            # Create new connection for this query
            print(f"🔌 Connecting to database: {self.db_path}")
            conn = duckdb.connect(str(self.db_path))
            
            try:
                # Get basic stats
                print("📊 Querying total assets...")
                result = conn.execute("SELECT COUNT(*) as total_assets FROM assets").fetchone()
                total_assets = result[0] if result else 0
                print(f"📊 Found {total_assets} total assets")
                
                # Get file count - DuckDBMemoryReader doesn't track source_file, so estimate from folder
                json_files = list(self.folder_path.glob("*.json"))
                total_files = len(json_files)
                print(f"📁 Found {total_files} JSON files in folder")
                
                # Get asset classes
                print("📊 Querying asset classes...")
                result = conn.execute("SELECT COUNT(DISTINCT assetClass) as asset_classes FROM assets").fetchone()
                asset_classes = result[0] if result else 0
                print(f"📊 Found {asset_classes} asset classes")
                
                return {
                    'status': 'connected',
                    'total_files': total_files,
                    'total_assets': total_assets,
                    'asset_classes': asset_classes,
                    'health_status': 'Healthy' if total_assets > 0 else 'Empty'
                }
            finally:
                # Always close the connection
                conn.close()
                print("🔌 Connection closed")
                
        except Exception as e:
            print(f"❌ Error in get_performance_stats: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_files': 0,
                'total_assets': 0,
                'asset_classes': 0,
                'health_status': 'Error'
            }
